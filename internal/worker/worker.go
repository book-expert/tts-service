// Package worker provides a NATS worker that processes TTS jobs.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/book-expert/events"
	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const handleMessageTimeout = 30 * time.Second

var (
	// ErrModelPathEmpty indicates that the model path is empty.
	ErrModelPathEmpty = errors.New("model path cannot be empty")
	// ErrSnacModelPathEmpty indicates that the SNAC model path is empty.
	ErrSnacModelPathEmpty = errors.New("snac model path cannot be empty")
	// ErrVoiceEmpty indicates that the voice is empty.
	ErrVoiceEmpty = errors.New("voice cannot be empty")
	// ErrUnsupportedVoice indicates that the provided voice is not supported.
	ErrUnsupportedVoice = errors.New("unsupported voice")
	// ErrTopPRange indicates that the TopP parameter is out of the valid range [0.0, 1.0].
	ErrTopPRange = errors.New("top_p must be between 0.0 and 1.0")
	// ErrRepetitionPenaltyRange indicates that the RepetitionPenalty parameter is out of the valid range [1.0, ...).
	ErrRepetitionPenaltyRange = errors.New("repetition penalty must be >= 1.0")
	// ErrTemperatureRange indicates that the Temperature parameter is out of the valid range [0.0, ...).
	ErrTemperatureRange = errors.New("temperature must be >= 0.0")
	// ErrNGLNegative indicates that the NGL (number of GPU layers) parameter is negative.
	ErrNGLNegative = errors.New("n_gpu_layers must be non-negative")
)

// NatsWorker listens for TTS jobs on a NATS subject and processes them.
type NatsWorker struct {
	natsConnection   *nats.Conn
	jetstreamContext nats.JetStreamContext
	subject          string
	store            core.ObjectStore
	processor        core.TTSProcessor
	log              *logger.Logger
}

// NewNatsWorker creates a new instance of a NATS worker.
func NewNatsWorker(
	natsConnection *nats.Conn,
	jetstreamContext nats.JetStreamContext,
	subject string,
	store core.ObjectStore,
	processor core.TTSProcessor,
	log *logger.Logger,
) (*NatsWorker, error) {
	return &NatsWorker{
		natsConnection:   natsConnection,
		jetstreamContext: jetstreamContext,
		subject:          subject,
		store:            store,
		processor:        processor,
		log:              log,
	}, nil
}

// Run starts the worker and begins listening for messages.
func (w *NatsWorker) Run(ctx context.Context) error {
	sub, err := w.natsConnection.Subscribe(w.subject, w.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to subject %s: %w", w.subject, err)
	}

	<-ctx.Done()

	drainErr := sub.Drain()
	if drainErr != nil {
		return fmt.Errorf("failed to drain subscription: %w", drainErr)
	}

	return nil
}

func (w *NatsWorker) handleMessage(msg *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), handleMessageTimeout)
	defer cancel()

	event, err := w.parseAndValidateEvent(msg)
	if err != nil {
		w.log.Error("Failed to parse and validate event: %v", err)

		return
	}

	audioKey, processErr := w.processTTSJob(ctx, event)
	if processErr != nil {
		w.log.Error("Failed to process TTS job for event %s: %v", event.Header.WorkflowID, processErr)

		return
	}

	replyEvent := &events.AudioChunkCreatedEvent{
		Header:     event.Header,
		AudioKey:   audioKey,
		PageNumber: event.PageNumber,
		TotalPages: event.TotalPages,
	}

	err = w.publishReplyEvent(msg, replyEvent)
	if err != nil {
		w.log.Error("Failed to publish reply event for workflow %s: %v", event.Header.WorkflowID, err)
	}
}

// processTTSJob handles the core logic of downloading text, processing it, and uploading audio.
func (w *NatsWorker) processTTSJob(ctx context.Context, event *events.TextProcessedEvent) (string, error) {
	textData, err := w.store.Download(ctx, event.TextKey)
	if err != nil {
		return "", fmt.Errorf("failed to download text data for key '%s': %w", event.TextKey, err)
	}

	ttsCfg := core.TTSConfig{
		ModelPath:         w.processor.GetConfig().ModelPath,
		SnacModelPath:     w.processor.GetConfig().SnacModelPath,
		Voice:             event.Voice,
		Seed:              event.Seed,
		NGL:               event.NGL,
		TopP:              event.TopP,
		RepetitionPenalty: event.RepetitionPenalty,
		Temperature:       event.Temperature,
	}

	validationErr := w.validateTTSConfig(ttsCfg)
	if validationErr != nil {
		w.log.Error("Invalid TTS configuration for workflow %s: %v", event.Header.WorkflowID, validationErr)

		return "", validationErr
	}

	audioData, err := w.processor.Process(ctx, textData, ttsCfg)
	if err != nil {
		return "", fmt.Errorf("failed to process text to speech: %w", err)
	}

	audioKey := uuid.NewString() + ".pcm"

	err = w.store.Upload(ctx, audioKey, audioData)
	if err != nil {
		return "", fmt.Errorf("failed to upload audio data for key '%s': %w", audioKey, err)
	}

	return audioKey, nil
}

// publishReplyEvent marshals and responds with the AudioChunkCreatedEvent.
func (w *NatsWorker) publishReplyEvent(msg *nats.Msg, replyEvent *events.AudioChunkCreatedEvent) error {
	replyData, err := json.Marshal(replyEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal reply event: %w", err)
	}

	err = msg.Respond(replyData)
	if err != nil {
		return fmt.Errorf("failed to publish reply event: %w", err)
	}

	return nil
}

func (w *NatsWorker) parseAndValidateEvent(msg *nats.Msg) (*events.TextProcessedEvent, error) {
	var event events.TextProcessedEvent

	err := json.Unmarshal(msg.Data, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	return &event, nil
}

// validateTTSConfig ensures that the TTSConfig contains valid and safe values.
func (w *NatsWorker) validateTTSConfig(cfg core.TTSConfig) error {
	// Validate ModelPath
	if cfg.ModelPath == "" {
		return ErrModelPathEmpty
	}
	// For simplicity, assuming ModelPath is always absolute and trusted for now.
	// In a real-world scenario, more robust path validation (e.g., against whitelists,
	// checking for directory traversal) would be needed.

	// Validate SnacModelPath
	if cfg.SnacModelPath == "" {
		return ErrSnacModelPathEmpty
	}
	// Similar to ModelPath, assuming trusted for now.

	// Validate Voice (example with a simple whitelist)
	allowedVoices := map[string]struct{}{
		"default": {},
		"tara":    {},
		"leah":    {},
		"jess":    {},
		"leo":     {},
		"dan":     {},
		"mia":     {},
		"zac":     {},
		"zoe":     {},
	}

	if cfg.Voice == "" {
		return ErrVoiceEmpty
	}

	if _, ok := allowedVoices[cfg.Voice]; !ok {
		return fmt.Errorf("%w: '%s'", ErrUnsupportedVoice, cfg.Voice)
	}

	// Validate numeric parameters
	if cfg.TopP < 0.0 || cfg.TopP > 1.0 {
		return fmt.Errorf("%w: got %f", ErrTopPRange, cfg.TopP)
	}
	// chatllm --help says 1.0=no penalty, so >= 1.0 is valid
	if cfg.RepetitionPenalty < 1.0 {
		return fmt.Errorf("%w: got %f", ErrRepetitionPenaltyRange, cfg.RepetitionPenalty)
	}
	// chatllm --help says T for --temp, typically >= 0.0
	if cfg.Temperature < 0.0 {
		return fmt.Errorf("%w: got %f", ErrTemperatureRange, cfg.Temperature)
	}
	// NGL (number of GPU layers) must be non-negative
	if cfg.NGL < 0 {
		return fmt.Errorf("%w: got %d", ErrNGLNegative, cfg.NGL)
	}
	// Seed is typically just an int, no specific range usually enforced beyond non-negative if desired.

	return nil
}
