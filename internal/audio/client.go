package audio

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/book-expert/logger"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	SubjectSpeechRequest = "audio.speech.request"
	SubjectMusicRequest  = "audio.music.request"
	RequestTimeout       = 120 * time.Second // Long timeout for audio generation
)

type Client struct {
	nc     *nats.Conn
	store  jetstream.ObjectStore
	logger *logger.Logger
}

func NewClient(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, log *logger.Logger) (*Client, error) {
	store, err := js.ObjectStore(ctx, "AUDIO_INTERMEDIATE")
	if err != nil {
		return nil, fmt.Errorf("failed to bind to AUDIO_INTERMEDIATE: %w", err)
	}

	return &Client{
		nc:     nc,
		store:  store,
		logger: log,
	}, nil
}

type SpeechRequest struct {
	RequestID  string `json:"request_id"`
	Text       string `json:"text"`
	VoiceID    string `json:"voice_id"`
	PromptText string `json:"prompt_text,omitempty"`
	OutputPath string `json:"output_path"`
}

type SpeechResponse struct {
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
	ObjectKey string `json:"object_key"`
	Error     string `json:"error,omitempty"`
}

type MusicRequest struct {
	RequestID  string `json:"request_id"`
	Prompt     string `json:"prompt"`
	Variations int    `json:"variations"`
	Duration   int    `json:"duration"`
}

type MusicResponse struct {
	RequestID  string   `json:"request_id"`
	Status     string   `json:"status"`
	ObjectKeys []string `json:"object_keys"`
	Error      string   `json:"error,omitempty"`
}

func (c *Client) GenerateSpeech(ctx context.Context, text, voiceID, promptText string) (string, error) {
	reqID := uuid.New().String()

	payload := SpeechRequest{
		RequestID:  reqID,
		Text:       text,
		VoiceID:    voiceID,
		PromptText: promptText,
		OutputPath: "",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	msg, err := c.nc.RequestWithContext(ctx, SubjectSpeechRequest, data)
	if err != nil {
		return "", fmt.Errorf("nats request failed: %w", err)
	}

	var resp SpeechResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}

	if resp.Status != "success" {
		return "", fmt.Errorf("audio server error: %s", resp.Error)
	}

	return c.downloadTempFile(ctx, resp.ObjectKey, "tts_speech_")
}

func (c *Client) GenerateMusicSuite(ctx context.Context, prompt string) ([]string, error) {
	reqID := uuid.New().String()
	payload := MusicRequest{
		RequestID:  reqID,
		Prompt:     prompt,
		Variations: 3,
		Duration:   180, // 3 minutes for better loopability
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	msg, err := c.nc.RequestWithContext(ctx, SubjectMusicRequest, data)
	if err != nil {
		return nil, fmt.Errorf("nats request failed: %w", err)
	}

	var resp MusicResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if resp.Status != "success" {
		return nil, fmt.Errorf("audio server error: %s", resp.Error)
	}

	var localFiles []string
	for _, key := range resp.ObjectKeys {
		path, err := c.downloadTempFile(ctx, key, "tts_music_")
		if err != nil {
			c.logger.Warnf("Failed to download music file %s: %v", key, err)
			continue
		}
		localFiles = append(localFiles, path)
	}

	return localFiles, nil
}

func (c *Client) downloadTempFile(ctx context.Context, key, prefix string) (string, error) {
	objResult, err := c.store.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("obj get failed for %s: %w", key, err)
	}

	// Create temp file
	// Use a simpler prefix to avoid overly long/messy filenames
	tmpFile, err := os.CreateTemp("", prefix+"*.wav")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		if err := tmpFile.Close(); err != nil {
			c.logger.Warnf("Failed to close temp file %s: %v", tmpFile.Name(), err)
		}
	}()

	// Copy
	if _, err := io.Copy(tmpFile, objResult); err != nil {
		return "", fmt.Errorf("copy content: %w", err)
	}

	// Delete from Object Store to free space/maintain hygiene
	if err := c.store.Delete(ctx, key); err != nil {
		c.logger.Warnf("Failed to delete intermediate audio object %s: %v", key, err)
	}

	return tmpFile.Name(), nil
}
