/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
"Work is love made visible. And if you cannot work with love but only with
distaste, it is better that you should leave your work and sit at the gate of
the temple and take alms of those who work with joy." — Kahlil Gibran

1.  LOVE AND CARE (Primary Driver)
    - This is a craft. Build with pride, honesty, and kindness.
    - If you put love in your work, you build something deserving of love.
    - Be helpful: Code is read more than written; optimize for the reader.

2.  WRITE WHAT YOU MEAN (Explicit > Implicit)
    - Use WHOLE WORDS: `RequestIdentifier` not `ReqID`.
    - No magic numbers: Move application settings to `project.toml`.
    - Secure by design: Keep API keys and secrets strictly in `.env`.
    - No ambiguity: If you assume something, document it.

3.  SIMPLE IS EFFICIENT (Minimal Viable Elegance)
    - Avoid over-engineering. Small interfaces, clear structs.
    - If a design requires a hack, stop. Redesign it with elegance.
    - Lean, Clean, Mean: Delete dead code immediately.

4.  NO BASELESS ASSUMPTIONS (Scientific Rigor)
    - Do not guess. Base decisions on documentation and proven patterns.
    - If you do not know, ask or verify.

5.  NON-BLOCKING & ROBUST
    - Never block the main goroutine. Use Context for cancellation.
    - Handle errors explicitly: Don't just return them, wrap them with context.

--------------------------------------------------------------------------------
EXAMPLES OF "LOVE AND CARE" IN THIS CONTEXT:
--------------------------------------------------------------------------------
(A) NAMING
    Indifferent:  func Gen(t string, v string)
    With Love:    func GenerateSoundscape(ctx context.Context, textPrompt string, voiceID string)
    *Why: The Agent reading this next year will know exactly what it does and that it is cancellable.*

(B) CONFIGURATION
    Indifferent:  const Timeout = 30 // Hardcoded
    With Love:    config.App.TimeoutSeconds // Loaded from project.toml
    *Why: Allows behavior tuning without recompiling or touching the codebase.*

(C) ERROR HANDLING
    Indifferent:  if err != nil { return err }
    With Love:    if err != nil { return fmt.Errorf("failed to initialize vox engine: %w", err) }
    *Why: Wrapping the error gives the user the 'trace of breadcrumbs' they need to fix it. That is kindness.*
--------------------------------------------------------------------------------
*/

package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/book-expert/logger"
)

const (
	EndpointSpeech = "/speech"
	EndpointMusic  = "/music"
	RequestTimeout = 120 * time.Second // Long timeout for audio generation
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *logger.Logger
}

func NewClient(baseURL string, log *logger.Logger) *Client {
	return &Client{
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
func (c *Client) GenerateSpeech(ctx context.Context, chunks []string, voiceID, promptText string) (io.ReadCloser, error) {
	payload := SpeechRequest{
		Chunks:     chunks,
		VoiceID:    voiceID,
		PromptText: promptText,
	}

	return c.postRequest(ctx, EndpointSpeech, payload)
}

// GenerateMusic calls the audio-server to generate a single music track.
// It returns a stream of the WAV audio. The caller is responsible for closing the stream.
func (c *Client) GenerateMusic(ctx context.Context, prompt string, duration int) (io.ReadCloser, error) {
	payload := MusicRequest{
		Prompt:      prompt,
		DurationSec: duration,
	}

	return c.postRequest(ctx, EndpointMusic, payload)
}

// MixAudio calls the audio-server to mix speech and music.
func (c *Client) MixAudio(ctx context.Context, speechData, musicData []byte) (io.ReadCloser, error) {
	url := c.baseURL + "/mix"

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add Speech
	part, err := writer.CreateFormFile("speech", "speech.wav")
	if err != nil {
		return nil, fmt.Errorf("create speech part: %w", err)
	}
	if _, err := part.Write(speechData); err != nil {
		return nil, fmt.Errorf("write speech data: %w", err)
	}

	// Add Music
	part, err = writer.CreateFormFile("music", "music.wav")
	if err != nil {
		return nil, fmt.Errorf("create music part: %w", err)
	}
	if _, err := part.Write(musicData); err != nil {
		return nil, fmt.Errorf("write music data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

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

// FinalizeAudio calls the audio-server to generate music for the given speech and mix them.
func (c *Client) FinalizeAudio(ctx context.Context, speechData []byte, musicPrompt string) (io.ReadCloser, error) {
	url := c.baseURL + "/finalize"

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add Speech
	part, err := writer.CreateFormFile("speech", "speech.wav")
	if err != nil {
		return nil, fmt.Errorf("create speech part: %w", err)
	}
	if _, err := part.Write(speechData); err != nil {
		return nil, fmt.Errorf("write speech data: %w", err)
	}

	// Add Music Prompt
	if err := writer.WriteField("music_prompt", musicPrompt); err != nil {
		return nil, fmt.Errorf("write music prompt: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

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

func (c *Client) postRequest(ctx context.Context, endpoint string, payload interface{}) (io.ReadCloser, error) {
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
