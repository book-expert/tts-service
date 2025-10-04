package tts

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "time"

    "github.com/book-expert/logger"
)

// API endpoints and paths.
const (
	apiGenerateSpeech = "/v1/generate/speech"
	apiHealth         = "/health"
)

// HTTP headers.
const (
	headerContentType = "Content-Type"
	headerAccept      = "Accept"
	contentTypeJSON   = "application/json"
	contentTypeWAV    = "audio/wav"
)

// Default values.
const (
	defaultTemperature = 0.75
	defaultLanguage    = "en"
)

// Static errors.
var (
	ErrTextCannotBeEmpty     = errors.New("text cannot be empty")
	ErrUnexpectedContentType = errors.New("unexpected content type")
	ErrReceivedEmptyAudio    = errors.New("received empty audio data")
	ErrHealthCheckFailed     = errors.New("health check failed")
	ErrServiceError          = errors.New("TTS service error")
	ErrServiceNonOKStatus    = errors.New("TTS service returned non-OK status")
)

// Helper functions for dynamic error messages.
func newUnexpectedContentTypeError(contentType string) error {
	return fmt.Errorf(
		"%w: expected audio/wav, got %s",
		ErrUnexpectedContentType,
		contentType,
	)
}

func newHealthCheckFailedError(status string) error {
	return fmt.Errorf("%w with status: %s", ErrHealthCheckFailed, status)
}

func newServiceErrorWithCodeError(status, detail, errorCode string) error {
	return fmt.Errorf(
		"%w (%s): %s (code: %s)",
		ErrServiceError,
		status,
		detail,
		errorCode,
	)
}

func newServiceNonOKStatusError(status, body string) error {
	return fmt.Errorf("%w: %s, body: %s", ErrServiceNonOKStatus, status, body)
}

// HTTPClient represents a client for the standalone TTS HTTP service.
// It encapsulates the HTTP configuration and provides methods for
// speech generation and health monitoring.
type HTTPClient struct {
    httpClient *http.Client
    baseURL    string
    log        *logger.Logger
}

// Request defines the JSON payload structure for TTS generation requests.
// All fields follow the explicit API contract defined in the service blueprint.
type Request struct {
	// Text contains the input text to convert to speech.
	// Must be non-empty and within reasonable length limits.
	Text string `json:"text"`

	// SpeakerRefPath optionally specifies a server-side path to a speaker
	// reference file for voice cloning. If empty, default speaker is used.
	SpeakerRefPath string `json:"speakerRefPath,omitempty"`

	// Language specifies the target language code (e.g., "en", "es").
	// Defaults to "en" if not specified.
	Language string `json:"language"`

	// Temperature controls randomness in speech generation.
	// Valid range: 0.0 (deterministic) to 2.0 (highly random).
	Temperature float64 `json:"temperature"`
}

// ErrorResponse represents a structured error response from the TTS service.
// This provides actionable diagnostics when requests fail.
type ErrorResponse struct {
	// Detail contains a human-readable error description.
	Detail string `json:"detail"`

	// ErrorCode provides a machine-readable error classification.
	ErrorCode string `json:"errorCode,omitempty"`
}

// NewHTTPClient creates and configures an HTTP client for the TTS service.
// The baseURL should include the protocol and port (e.g., "http://localhost:8000").
// The timeout applies to all HTTP requests made by this client.
func NewHTTPClient(baseURL string, timeout time.Duration, log *logger.Logger) *HTTPClient {
    return &HTTPClient{
        baseURL: baseURL,
        httpClient: &http.Client{
            Transport:     nil,
            CheckRedirect: nil,
            Jar:           nil,
            Timeout:       timeout,
        },
        log: log,
    }
}

// GenerateSpeech sends a TTS generation request and returns the raw audio data.
// This method validates input parameters, constructs the HTTP request according
// to the API contract, and handles both successful responses and error conditions.
//
// The returned audio data is in WAV format as specified by the service contract.
// Callers are responsible for writing this data to files or streaming it as needed.
func (c *HTTPClient) GenerateSpeech(ctx context.Context, req Request) ([]byte, error) {
	err := c.validateRequest(&req)
	if err != nil {
		return nil, err
	}

	httpReq, err := c.buildHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	resp, err := c.sendRequest(httpReq)
	if err != nil {
		return nil, err
	}

    defer func() {
        closeErr := resp.Body.Close()
        if closeErr != nil && c.log != nil {
            c.log.Warn("failed to close response body: %v", closeErr)
        }
    }()

	return c.processResponse(resp)
}

// HealthCheck verifies that the TTS service is running and operational.
// This method performs a lightweight check against the service health endpoint
// and returns an error if the service is unavailable or reports unhealthy status.
//
// Health checks should be performed before processing large workloads to fail fast
// and provide clear diagnostics when the service is unavailable.
func (c *HTTPClient) HealthCheck(ctx context.Context) error {
	url := c.baseURL + apiHealth

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf(
			"health check failed for service at %s: %w",
			c.baseURL,
			err,
		)
	}

    defer func() {
        closeErr := resp.Body.Close()
        if closeErr != nil && c.log != nil {
            c.log.Warn("failed to close response body: %v", closeErr)
        }
    }()

	if resp.StatusCode != http.StatusOK {
		return newHealthCheckFailedError(resp.Status)
	}

	return nil
}

// validateRequest validates and normalizes the TTS request parameters.
func (c *HTTPClient) validateRequest(req *Request) error {
	if req.Text == "" {
		return ErrTextCannotBeEmpty
	}

	if req.Temperature == 0 {
		req.Temperature = defaultTemperature
	}

	if req.Language == "" {
		req.Language = defaultLanguage
	}

	return nil
}

// buildHTTPRequest constructs the HTTP request with proper headers and body.
func (c *HTTPClient) buildHTTPRequest(
	ctx context.Context,
	req Request,
) (*http.Request, error) {
	requestBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.baseURL + apiGenerateSpeech

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewBuffer(requestBody),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set(headerContentType, contentTypeJSON)
	httpReq.Header.Set(headerAccept, contentTypeWAV)

	return httpReq, nil
}

// sendRequest executes the HTTP request and returns the response.
func (c *HTTPClient) sendRequest(httpReq *http.Request) (*http.Response, error) {
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to send request to TTS service at %s: %w",
			c.baseURL,
			err,
		)
	}

	return resp, nil
}

// processResponse handles the HTTP response and extracts audio data.
func (c *HTTPClient) processResponse(resp *http.Response) ([]byte, error) {
	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	err := c.validateResponseContentType(resp)
	if err != nil {
		return nil, err
	}

	return c.readAudioData(resp)
}

// validateResponseContentType ensures the response has the expected content type.
func (c *HTTPClient) validateResponseContentType(resp *http.Response) error {
	contentType := resp.Header.Get("Content-Type")
	if contentType != contentTypeWAV {
		return newUnexpectedContentTypeError(contentType)
	}

	return nil
}

// readAudioData reads and validates the audio response data.
func (c *HTTPClient) readAudioData(resp *http.Response) ([]byte, error) {
	audioData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read audio data: %w", err)
	}

	if len(audioData) == 0 {
		return nil, ErrReceivedEmptyAudio
	}

	return audioData, nil
}

// parseErrorResponse attempts to decode a structured JSON error from the service.
// If structured parsing fails, it falls back to returning the raw response body
// to ensure diagnostic information is preserved.
func (c *HTTPClient) parseErrorResponse(resp *http.Response) error {
	var errorResp ErrorResponse

	err := json.NewDecoder(resp.Body).Decode(&errorResp)
	if err == nil {
		return newServiceErrorWithCodeError(
			resp.Status,
			errorResp.Detail,
			errorResp.ErrorCode,
		)
	}

	// Fallback to raw response for non-JSON errors
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("failed to read error response body: %w", readErr)
	}

	return newServiceNonOKStatusError(resp.Status, string(body))
}
