/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS
--------------------------------------------------------------------------------
"Work is love made visible."

1.  PARTNERSHIP & CRAFT: We do not just "finish tasks"; we build with pride.
    We question decisions that compromise safety, security, or quality.
2.  HONESTY: We verify before we claim success. We write what we mean.
    If we don't know, we ask.
3.  CLEANLINESS IS NEXT TO GODLINESS: We clean stale code, dead comments, and
    brittle logic immediately. A sanitary codebase protects focus.
4.  SIMPLICITY IS KINDNESS: We choose the simple, explicit path. Complexity
    is a burden we refuse to place on others.
5.  PROTECTION: We handle errors, secure data, and ensure non-blocking flow.
    We do not leave traps for the future.

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
--------------------------------------------------------------------------------
*/

package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/book-expert/logger"
)

const (
	RequestTimeout = 15 * time.Minute
	EndpointSpeech = "/speech"
	EndpointMusic  = "/music"
)

type SpeechClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *logger.Logger
}

func NewSpeechClient(baseURL string, log *logger.Logger) *SpeechClient {
	return &SpeechClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: RequestTimeout,
		},
		logger: log,
	}
}

type SpeechRequest struct {
	Chunks     []string `json:"chunks"`
	VoiceID    string   `json:"voice_id"`
	PromptText string   `json:"prompt_text,omitempty"`
}

type MusicRequest struct {
	Prompt      string `json:"prompt"`
	DurationSec int    `json:"duration_sec"`
	// Config can be added if needed
}

// GenerateSpeech calls the audio-server to generate speech.
// It returns a stream of the WAV audio. The caller is responsible for closing the stream.
func (c *SpeechClient) GenerateSpeech(ctx context.Context, chunks []string, voiceID, promptText string) (io.ReadCloser, error) {
	payload := SpeechRequest{
		Chunks:     chunks,
		VoiceID:    voiceID,
		PromptText: promptText,
	}

	return c.postRequest(ctx, EndpointSpeech, payload)
}


func (c *SpeechClient) postRequest(ctx context.Context, endpoint string, payload interface{}) (io.ReadCloser, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("audio server error (%d): %s", resp.StatusCode, string(body))
	}

	return resp.Body, nil
}
