// Package tts implements the Text-To-Speech processing logic using Google's GenAI REST API.
// It handles the interaction with the Gemini API to generate audio from text.
package tts

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/core"
)

const (
	// ModalityAudio specifies that we want the model to generate audio output.
	ModalityAudio = "AUDIO"
)

var (
	// ErrEmptyText is returned when the input text is empty.
	ErrEmptyText = errors.New("input text is empty")
	// ErrVoiceUnspecified is returned when no voice is provided in config or defaults.
	ErrVoiceUnspecified = errors.New("voice name must be specified either in config or as default")
	// ErrEmptyResponse is returned when the API returns a success status but no content.
	ErrEmptyResponse = errors.New("received empty content response from Gemini API")
	// ErrNoAudioData is returned when the response content is missing the expected inline audio bytes.
	ErrNoAudioData = errors.New("no audio inline data found in response part")
)

// GeminiProcessor implements the core.TTSProcessor interface using the Gemini REST API.
type GeminiProcessor struct {
	baseURL      string
	apiKey       string
	model        string
	defaultVoice string
	client       *http.Client
	systemLogger *logger.Logger
}

// Structs for JSON Payload

type part struct {
	Text string `json:"text"`
}

type content struct {
	Parts []part `json:"parts"`
}

type prebuiltVoiceConfig struct {
	VoiceName string `json:"voiceName"`
}

type voiceConfig struct {
	PrebuiltVoiceConfig prebuiltVoiceConfig `json:"prebuiltVoiceConfig"`
}

type speechConfig struct {
	VoiceConfig voiceConfig `json:"voiceConfig"`
}

type generationConfig struct {
	ResponseModalities []string     `json:"responseModalities"`
	SpeechConfig       speechConfig `json:"speechConfig"`
}

type generateRequest struct {
	Contents         []content        `json:"contents"`
	GenerationConfig generationConfig `json:"generationConfig"`
	Model            string           `json:"model,omitempty"`
}

// Structs for JSON Response

type inlineData struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"` // Base64 encoded
}

type responsePart struct {
	InlineData *inlineData `json:"inlineData,omitempty"`
}

type candidateContent struct {
	Parts []responsePart `json:"parts"`
}

type candidate struct {
	Content candidateContent `json:"content"`
}

type generateResponse struct {
	Candidates []candidate `json:"candidates"`
}

// New creates a new GeminiProcessor instance.
// Why: Encapsulates the client creation and ensures all required fields are initialized.
func New(ctx context.Context, baseURL string, apiKey string, model string, defaultVoice string, systemLogger *logger.Logger) (*GeminiProcessor, error) {
	if apiKey == "" {
		return nil, errors.New("API key cannot be empty")
	}
	if baseURL == "" {
		return nil, errors.New("base URL cannot be empty")
	}

	return &GeminiProcessor{
		baseURL:      strings.TrimRight(baseURL, "/"),
		apiKey:       apiKey,
		model:        model,
		defaultVoice: defaultVoice,
		client:       &http.Client{Timeout: 600 * time.Second}, // Generous timeout for audio generation
		systemLogger: systemLogger,
	}, nil
}

// Process generates audio from the provided text using the configured model and voice.
// Flow: Validate Input -> Resolve Voice -> Build Payload -> Call REST API -> Extract Audio
func (processor *GeminiProcessor) Process(ctx context.Context, textBytes []byte, config core.TTSConfig) ([]byte, error) {
	textInput := string(textBytes)
	if strings.TrimSpace(textInput) == "" {
		return nil, ErrEmptyText
	}

	// Prepend the style prompt to the text content if provided.
	// This "steers" the model's delivery.
	if config.StylePrompt != "" {
		textInput = fmt.Sprintf("%s: %s", config.StylePrompt, textInput)
	}

	voiceName, err := processor.resolveVoice(config.Voice)
	if err != nil {
		return nil, err
	}

	// 1. Construct Request Payload
	payload := generateRequest{
		Contents: []content{
			{
				Parts: []part{
					{Text: textInput},
				},
			},
		},
		GenerationConfig: generationConfig{
			ResponseModalities: []string{ModalityAudio},
			SpeechConfig: speechConfig{
				VoiceConfig: voiceConfig{
					PrebuiltVoiceConfig: prebuiltVoiceConfig{
						VoiceName: voiceName,
					},
				},
			},
		},
		Model: processor.model, // Including model in body as per user example
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	// 2. Create HTTP Request
	// URL Pattern: https://<base>/v1beta/models/<model>:generateContent
	url := fmt.Sprintf("%s/v1beta/models/%s:generateContent", processor.baseURL, processor.model)
	
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", processor.apiKey)

	// 3. Execute Request
	processor.systemLogger.Infof("Sending TTS request to Gemini (%s) for voice %s...", processor.model, voiceName)
	resp, err := processor.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("api request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("api returned error status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// 4. Parse Response
	var genResp generateResponse
	if err := json.NewDecoder(resp.Body).Decode(&genResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// 5. Extract Audio
	if len(genResp.Candidates) == 0 ||
		len(genResp.Candidates[0].Content.Parts) == 0 ||
		genResp.Candidates[0].Content.Parts[0].InlineData == nil {
		return nil, ErrNoAudioData
	}

	base64Data := genResp.Candidates[0].Content.Parts[0].InlineData.Data
	pcmData, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, fmt.Errorf("base64 decode failed: %w", err)
	}

	return pcmData, nil
}

// resolveVoice determines which voice to use, falling back to default if necessary.
func (processor *GeminiProcessor) resolveVoice(requestedVoice string) (string, error) {
	if requestedVoice != "" {
		return requestedVoice, nil
	}
	if processor.defaultVoice != "" {
		return processor.defaultVoice, nil
	}
	return "", ErrVoiceUnspecified
}