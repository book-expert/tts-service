/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/book-expert/common-events"
	"github.com/book-expert/common-worker"
	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// MessageProcessingTimeout defines the maximum duration allowed for processing a single TTS job.
	MessageProcessingTimeout = 3600 * time.Second

	// DeadLetterQueuePublishMaxRetries defines the number of attempts to publish a failed message to the DLQ.
	DeadLetterQueuePublishMaxRetries = 3

	// DeadLetterQueueBackoffDuration defines the wait time between DLQ publish attempts.
	DeadLetterQueueBackoffDuration = 2 * time.Second

	// AudioSampleRateHz is the standard sample rate for the generated WAV audio.
	AudioSampleRateHz = 48000

	// ProgressCompletedValue is the value stored in the Key-Value store to mark a page as processed.
	ProgressCompletedValue = "done"

	// AudioChunkKeyFormat defines the naming convention for individual page audio chunks in the Object Store.
	AudioChunkKeyFormat = "%s_page_%d.wav"

	// NoSpeechMarker is the token used in text to indicate silence should be generated.
	NoSpeechMarker = "[NO_SPEECH]"
)

// JetStreamPublisher defines the interface for publishing messages to JetStream.
type JetStreamPublisher interface {
	Publish(requestContext context.Context, subject string, data []byte, options ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Worker manages the lifecycle of processing text-to-speech requests from NATS.
type Worker struct {
	baseWorker                *worker.Worker[*events.TextProcessedEvent]
	jetStreamPublisher        JetStreamPublisher
	jetStreamContext          jetstream.JetStream
	producerSubject           string
	textToSpeechStartedSubject string
	musicStartedSubject       string
	musicRequestSubject       string
	musicCreatedSubject       string
	aggregationStartedSubject string
	deadLetterQueueSubject    string
	textObjectStore           core.ObjectStore
	audioObjectStore          core.ObjectStore
	progressKeyValueStore     jetstream.KeyValue
	textToSpeechProcessor     core.TTSProcessor
	logger                    *logger.Logger
}

// New initializes a new Worker with all necessary dependencies.
func New(
	natsConnection *nats.Conn,
	jetStreamContext jetstream.JetStream,
	jetStreamPublisher JetStreamPublisher,
	subscriptionStream string,
	subscriptionSubject string,
	consumerDurableName string,
	producerSubject string,
	textToSpeechStartedSubject string,
	musicStartedSubject string,
	musicRequestSubject string,
	musicCreatedSubject string,
	aggregationStartedSubject string,
	deadLetterQueueSubject string,
	textObjectStore core.ObjectStore,
	audioObjectStore core.ObjectStore,
	progressKeyValueStore jetstream.KeyValue,
	textToSpeechProcessor core.TTSProcessor,
	serviceLogger *logger.Logger,
	workerCount int,
) (*Worker, error) {
	textToSpeechWorker := &Worker{
		jetStreamPublisher:         jetStreamPublisher,
		jetStreamContext:           jetStreamContext,
		producerSubject:            producerSubject,
		textToSpeechStartedSubject: textToSpeechStartedSubject,
		musicStartedSubject:        musicStartedSubject,
		musicRequestSubject:        musicRequestSubject,
		musicCreatedSubject:        musicCreatedSubject,
		aggregationStartedSubject:  aggregationStartedSubject,
		deadLetterQueueSubject:     deadLetterQueueSubject,
		textObjectStore:            textObjectStore,
		audioObjectStore:           audioObjectStore,
		progressKeyValueStore:      progressKeyValueStore,
		textToSpeechProcessor:      textToSpeechProcessor,
		logger:                     serviceLogger,
	}

	configuration := worker.Config{
		StreamName:    subscriptionStream,
		ConsumerName:  consumerDurableName,
		FilterSubject: subscriptionSubject,
		WorkerCount:   workerCount,
		MaxDeliver:    10,
	}

	textToSpeechWorker.baseWorker = worker.New(natsConnection, jetStreamContext, serviceLogger, configuration, textToSpeechWorker.handleMessage)
	return textToSpeechWorker, nil
}

// Run executes the main worker loop.
func (textToSpeechWorker *Worker) Run(systemContext context.Context) error {
	return textToSpeechWorker.baseWorker.Start(systemContext)
}

func (textToSpeechWorker *Worker) handleMessage(requestContext context.Context, event *events.TextProcessedEvent, message jetstream.Msg) error {
	processingContext, cancelProcessing := context.WithTimeout(requestContext, MessageProcessingTimeout)
	defer cancelProcessing()

	textToSpeechWorker.logger.Infof("Processing Page %d for Workflow %s", event.PageNumber, event.Header.WorkflowID)

	if executionError := textToSpeechWorker.executeJob(processingContext, event); executionError != nil {
		textToSpeechWorker.logger.Errorf("Job execution failed: %v", executionError)
		textToSpeechWorker.handleProcessingFailure(processingContext, message, message.Data())
		return executionError
	}

	return nil
}

func (textToSpeechWorker *Worker) handleProcessingFailure(requestContext context.Context, message jetstream.Msg, payload []byte) {
	metadata, metadataError := message.Metadata()
	if metadataError == nil {
		if metadata.NumDelivered < 10 {
			textToSpeechWorker.logger.Warnf("Processing failed (Attempt %d/10). Retrying in 20s...", metadata.NumDelivered)
			_ = message.NakWithDelay(20 * time.Second)
			return
		}
	}

	textToSpeechWorker.logger.Errorf("Processing failed after attempts. Moving to DLQ.")

	if textToSpeechWorker.deadLetterQueueSubject == "" {
		_ = message.Nak()
		return
	}

	for attempt := 1; attempt <= DeadLetterQueuePublishMaxRetries; attempt++ {
		if _, publishError := textToSpeechWorker.jetStreamPublisher.Publish(requestContext, textToSpeechWorker.deadLetterQueueSubject, payload); publishError == nil {
			_ = message.Ack()
			return
		}
		time.Sleep(DeadLetterQueueBackoffDuration)
	}

	_ = message.NakWithDelay(DeadLetterQueueBackoffDuration)
}

func (textToSpeechWorker *Worker) executeJob(requestContext context.Context, event *events.TextProcessedEvent) error {
	// 0a. Publish Text-To-Speech Started
	if publishStartedError := textToSpeechWorker.publishStarted(requestContext, event); publishStartedError != nil {
		textToSpeechWorker.logger.Warnf("Failed to publish text-to-speech started event: %v", publishStartedError)
	}

	// 0b. Request Background Music Generation (Page 1 Only)
	if event.PageNumber == 1 && event.Settings != nil && event.Settings.AudioSessionConfig != nil && event.Settings.AudioSessionConfig.MusicPrompt != "" {
		textToSpeechWorker.logger.Infof("Requesting background music generation for Workflow %s", event.Header.WorkflowID)
		if publishMusicRequestError := textToSpeechWorker.publishMusicRequest(requestContext, event.Header, event.Settings.AudioSessionConfig.MusicPrompt, 180, event.Settings.AudioSessionConfig.GenerationConfig); publishMusicRequestError != nil {
			textToSpeechWorker.logger.Errorf("Failed to publish music request: %v", publishMusicRequestError)
		}
	}

	// 1. Get Text
	cleanText, retrievalError := textToSpeechWorker.retrieveAndCleanText(requestContext, event.TextKey)
	if retrievalError != nil {
		return retrievalError
	}

	// 2. Create TTS Configuration
	var textToSpeechConfiguration core.TTSConfig
	if event.Settings != nil && event.Settings.AudioSessionConfig != nil {
		textToSpeechConfiguration = core.TTSConfig{
			SessionID:     event.Settings.AudioSessionConfig.SessionID,
			VoiceID:       event.Settings.AudioSessionConfig.VoiceID,
			VoiceStyle:    event.Settings.AudioSessionConfig.VoiceStyle,
			MusicPrompt:   event.Settings.AudioSessionConfig.MusicPrompt,
			TextDirective: event.Settings.AudioSessionConfig.TextDirective,
		}
	}

	// 3. Generate Audio
	var audioData []byte
	if strings.Contains(string(cleanText), NoSpeechMarker) {
		audioData = audio.GenerateSilentWav(1*time.Second, AudioSampleRateHz, 1, 32)
	} else {
		var generationError error
		audioData, generationError = textToSpeechWorker.textToSpeechProcessor.Process(requestContext, cleanText, textToSpeechConfiguration)
		if generationError != nil {
			return fmt.Errorf("text-to-speech generation failed: %w", generationError)
		}
	}

	// 4. Store Audio Chunk
	audioChunkKey := fmt.Sprintf(AudioChunkKeyFormat, event.Header.WorkflowID, event.PageNumber)
	if uploadError := textToSpeechWorker.audioObjectStore.Upload(requestContext, audioChunkKey, audioData); uploadError != nil {
		return fmt.Errorf("audio upload failed: %w", uploadError)
	}

	// 5. Publish Completion Event
	completionEvent := events.TTSCompletedEvent{
		Header:     event.Header,
		PageNumber: event.PageNumber,
		TotalPages: event.TotalPages,
		AudioKey:   audioChunkKey,
	}

	return textToSpeechWorker.publishCompleted(requestContext, &completionEvent)
}

func (textToSpeechWorker *Worker) retrieveAndCleanText(requestContext context.Context, textKey string) ([]byte, error) {
	textContent, downloadError := textToSpeechWorker.textObjectStore.Download(requestContext, textKey)
	if downloadError != nil {
		return nil, fmt.Errorf("download text failed: %w", downloadError)
	}

	var textSegments []string
	if jsonError := json.Unmarshal(textContent, &textSegments); jsonError != nil {
		return nil, fmt.Errorf("failed to unmarshal text segments (contract violation): %w", jsonError)
	}

	joinedText := ""
	for index, segment := range textSegments {
		if index > 0 {
			joinedText += "\n\n"
		}
		joinedText += segment
	}
	return []byte(joinedText), nil
}

func (textToSpeechWorker *Worker) publishStarted(requestContext context.Context, source *events.TextProcessedEvent) error {
	if textToSpeechWorker.textToSpeechStartedSubject == "" {
		return nil
	}

	event := events.TTSStartedEvent{
		Header:     source.Header,
		PageNumber: source.PageNumber,
		TotalPages: source.TotalPages,
	}

	data, marshalError := json.Marshal(event)
	if marshalError != nil {
		return marshalError
	}

	_, publishError := textToSpeechWorker.jetStreamPublisher.Publish(requestContext, textToSpeechWorker.textToSpeechStartedSubject, data)
	return publishError
}

func (textToSpeechWorker *Worker) publishMusicRequest(requestContext context.Context, header events.EventHeader, prompt string, duration int, config *events.LyriaGenerationConfig) error {
	if textToSpeechWorker.musicRequestSubject == "" {
		return nil
	}

	event := events.MusicRequestEvent{
		Header:           header,
		Prompt:           prompt,
		DurationSeconds:  duration,
		GenerationConfig: config,
	}

	data, marshalError := json.Marshal(event)
	if marshalError != nil {
		return marshalError
	}

	_, publishError := textToSpeechWorker.jetStreamPublisher.Publish(requestContext, textToSpeechWorker.musicRequestSubject, data)
	return publishError
}

func (textToSpeechWorker *Worker) publishCompleted(requestContext context.Context, event *events.TTSCompletedEvent) error {
	data, marshalError := json.Marshal(event)
	if marshalError != nil {
		return fmt.Errorf("marshal event failed: %w", marshalError)
	}
	if _, publishError := textToSpeechWorker.jetStreamPublisher.Publish(requestContext, textToSpeechWorker.producerSubject, data); publishError != nil {
		return fmt.Errorf("publish event failed: %w", publishError)
	}
	return nil
}