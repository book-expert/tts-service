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
	"github.com/nats-io/nats.go/jetstream"
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
	natsConnection           *nats.Conn
	jetstreamContext         JetStreamPub
	subject                  string
	streamName               string
	consumerName             string
	audioChunkCreatedSubject string
	store                    core.ObjectStore
	processor                core.TTSProcessor
	log                      *logger.Logger
}

// JetStreamPub is the minimal subset of jetstream.JetStream used by the worker.
type JetStreamPub interface {
	Stream(ctx context.Context, stream string) (jetstream.Stream, error)
	Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// NewNatsWorker creates a new instance of a NATS worker.
func NewNatsWorker(
	natsConnection *nats.Conn,
	jetstreamContext JetStreamPub,
	streamName string,
	subject string,
	consumerName string,
	audioChunkCreatedSubject string,
	store core.ObjectStore,
	processor core.TTSProcessor,
	log *logger.Logger,
) (*NatsWorker, error) {
	return &NatsWorker{
		natsConnection:           natsConnection,
		jetstreamContext:         jetstreamContext,
		streamName:               streamName,
		subject:                  subject,
		consumerName:             consumerName,
		audioChunkCreatedSubject: audioChunkCreatedSubject,
		store:                    store,
		processor:                processor,
		log:                      log,
	}, nil
}

// Run starts the worker and begins listening for messages.
func (w *NatsWorker) Run(ctx context.Context) error {
	// Create or update consumer for the stream
	stream, err := w.jetstreamContext.Stream(ctx, w.streamName)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", w.streamName, err)
	}

	consumerConfig := jetstream.ConsumerConfig{
		Durable:       w.consumerName,
		FilterSubject: w.subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer %s for stream %s: %w", w.consumerName, w.streamName, err)
	}

	// Consume messages from the consumer
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := consumer.Next()
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}

				w.log.Error("Failed to get next message: %v", err)

				continue
			}

			w.HandleMessage(msg)
		}
	}
}

// HandleMessage processes incoming NATS messages.
func (w *NatsWorker) HandleMessage(msg jetstream.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), handleMessageTimeout)
	defer cancel()

	event, err := w.parseAndValidateEvent(msg)
	if err != nil {
		w.log.Error("Failed to parse and validate event: %v", err)
		// Negative acknowledge the message so it can be redelivered
		nackErr := msg.Nak()
		if nackErr != nil {
			w.log.Error("Failed to NAK message: %v", nackErr)
		}

		return
	}

	audioKey, processErr := w.processTTSJob(ctx, event)
	if processErr != nil {
		w.log.Error("Failed to process TTS job for event %s: %v", event.Header.WorkflowID, processErr)
		// Negative acknowledge the message so it can be redelivered
		nackErr := msg.Nak()
		if nackErr != nil {
			w.log.Error("Failed to NAK message: %v", nackErr)
		}

		return
	}

	replyEvent := &events.AudioChunkCreatedEvent{
		Header:     event.Header,
		AudioKey:   audioKey,
		PageNumber: event.PageNumber,
		TotalPages: event.TotalPages,
	}

	err = w.publishEvent(ctx, replyEvent)
	if err != nil {
		w.log.Error("Failed to publish reply event for workflow %s: %v", event.Header.WorkflowID, err)
		// Negative acknowledge the message so it can be redelivered
		nackErr := msg.Nak()
		if nackErr != nil {
			w.log.Error("Failed to NAK message: %v", nackErr)
		}

		return
	}

	// Acknowledge the message as processed successfully
	ackErr := msg.Ack()
	if ackErr != nil {
		w.log.Error("Failed to ACK message: %v", ackErr)
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
		AllowedVoices:     w.processor.GetConfig().AllowedVoices,
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

// publishEvent marshals and publishes the AudioChunkCreatedEvent.
func (w *NatsWorker) publishEvent(ctx context.Context, replyEvent *events.AudioChunkCreatedEvent) error {
	replyData, err := json.Marshal(replyEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal reply event: %w", err)
	}

	_, err = w.jetstreamContext.Publish(ctx, w.audioChunkCreatedSubject, replyData)
	if err != nil {
		return fmt.Errorf("failed to publish reply event: %w", err)
	}

	return nil
}

func (w *NatsWorker) parseAndValidateEvent(msg jetstream.Msg) (*events.TextProcessedEvent, error) {
	var event events.TextProcessedEvent

	err := json.Unmarshal(msg.Data(), &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	return &event, nil
}

// validateTTSConfig ensures that the TTSConfig contains valid and safe values.
func (w *NatsWorker) validateTTSConfig(cfg core.TTSConfig) error {
	var err error

	err = validatePaths(cfg)
	if err != nil {
		return err
	}

	err = validateVoice(cfg)
	if err != nil {
		return err
	}

	err = validateNumericParams(cfg)
	if err != nil {
		return err
	}

	return nil
}

func validatePaths(cfg core.TTSConfig) error {
	if cfg.ModelPath == "" {
		return ErrModelPathEmpty
	}

	if cfg.SnacModelPath == "" {
		return ErrSnacModelPathEmpty
	}

	return nil
}

func validateVoice(cfg core.TTSConfig) error {
	allowedVoices := make(map[string]struct{}, len(cfg.AllowedVoices))
	for _, voice := range cfg.AllowedVoices {
		allowedVoices[voice] = struct{}{}
	}

	if cfg.Voice == "" {
		return ErrVoiceEmpty
	}

	if _, ok := allowedVoices[cfg.Voice]; !ok {
		return fmt.Errorf("%w: '%s'", ErrUnsupportedVoice, cfg.Voice)
	}

	return nil
}

func validateNumericParams(cfg core.TTSConfig) error {
	if cfg.TopP < 0.0 || cfg.TopP > 1.0 {
		return fmt.Errorf("%w: got %f", ErrTopPRange, cfg.TopP)
	}

	if cfg.RepetitionPenalty < 1.0 {
		return fmt.Errorf("%w: got %f", ErrRepetitionPenaltyRange, cfg.RepetitionPenalty)
	}

	if cfg.Temperature < 0.0 {
		return fmt.Errorf("%w: got %f", ErrTemperatureRange, cfg.Temperature)
	}

	if cfg.NGL < 0 {
		return fmt.Errorf("%w: got %d", ErrNGLNegative, cfg.NGL)
	}

	return nil
}
