/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */
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

func NewSpeechClient(baseURL string, serviceLogger *logger.Logger) *SpeechClient {
	return &SpeechClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: RequestTimeout,
		},
		logger: serviceLogger,
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
func (speechClient *SpeechClient) GenerateSpeech(requestContext context.Context, chunks []string, voiceID, promptText string) (io.ReadCloser, error) {
	payload := SpeechRequest{
		Chunks:     chunks,
		VoiceID:    voiceID,
		PromptText: promptText,
	}

	return speechClient.postRequest(requestContext, EndpointSpeech, payload)
}

func (speechClient *SpeechClient) postRequest(requestContext context.Context, endpoint string, payload interface{}) (io.ReadCloser, error) {
	data, marshalError := json.Marshal(payload)
	if marshalError != nil {
		return nil, fmt.Errorf("marshal request: %w", marshalError)
	}

	targetURL := speechClient.baseURL + endpoint
	httpRequest, creationError := http.NewRequestWithContext(requestContext, "POST", targetURL, bytes.NewBuffer(data))
	if creationError != nil {
		return nil, fmt.Errorf("create request: %w", creationError)
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, executionError := speechClient.httpClient.Do(httpRequest)
	if executionError != nil {
		return nil, fmt.Errorf("http request failed: %w", executionError)
	}

	if httpResponse.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(httpResponse.Body)
		_ = httpResponse.Body.Close()
		return nil, fmt.Errorf("audio server error (%d): %s", httpResponse.StatusCode, string(responseBody))
	}

	return httpResponse.Body, nil
}
