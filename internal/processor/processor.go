/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/book-expert/common-events"
	worker "github.com/book-expert/common-worker"
	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	MessageProcessingTimeout         = 3600 * time.Second
	DeadLetterQueuePublishMaxRetries = 3
	DeadLetterQueueBackoffDuration   = 2 * time.Second
	AudioChunkKeyFormat              = "%s_page_%d.wav"
	NoSpeechMarker                   = "[NO_SPEECH]"
)

func getAudioSampleRateHz() int {
	rateStr := os.Getenv("AUDIO_SAMPLE_RATE_TTS")
	if rateStr == "" {
		return 44100
	}
	rate, err := strconv.Atoi(rateStr)
	if err != nil {
		return 44100
	}
	return rate
}

type JetStreamPublisher interface {
	Publish(requestContext context.Context, subject string, data []byte, options ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Processor implements the business logic for converting text to speech.
type Processor struct {
	engine                     *worker.Worker[*events.TextCreatedEvent]
	jetStreamPublisher         JetStreamPublisher
	producerSubject            string
	textToSpeechStartedSubject string
	musicRequestSubject        string
	deadLetterQueueSubject     string
	textObjectStore            core.ObjectStore
	audioObjectStore           core.ObjectStore
	textToSpeechProcessor      core.TTSProcessor
	serviceLogger              *logger.Logger
}

// NewProcessor initializes a new Processor with its required dependencies.
func NewProcessor(
	natsConnection *nats.Conn,
	jetStreamContext jetstream.JetStream,
	jetStreamPublisher JetStreamPublisher,
	subscriptionStream string,
	subscriptionSubject string,
	consumerDurableName string,
	producerSubject string,
	textToSpeechStartedSubject string,
	musicRequestSubject string,
	deadLetterQueueSubject string,
	textObjectStore core.ObjectStore,
	audioObjectStore core.ObjectStore,
	textToSpeechProcessor core.TTSProcessor,
	serviceLogger *logger.Logger,
	workerCount int,
) (*Processor, error) {
	ttsProcessor := &Processor{
		jetStreamPublisher:         jetStreamPublisher,
		producerSubject:            producerSubject,
		textToSpeechStartedSubject: textToSpeechStartedSubject,
		musicRequestSubject:        musicRequestSubject,
		deadLetterQueueSubject:     deadLetterQueueSubject,
		textObjectStore:            textObjectStore,
		audioObjectStore:           audioObjectStore,
		textToSpeechProcessor:      textToSpeechProcessor,
		serviceLogger:              serviceLogger,
	}

	workerConfiguration := worker.Config{
		StreamName:    subscriptionStream,
		ConsumerName:  consumerDurableName,
		FilterSubject: subscriptionSubject,
		WorkerCount:   workerCount,
		MaxDeliver:    10,
	}

	ttsProcessor.engine = worker.New(natsConnection, jetStreamContext, serviceLogger, workerConfiguration, ttsProcessor.handleMessage)
	return ttsProcessor, nil
}

// Start executes the underlying processor engine.
func (processor *Processor) Start(systemContext context.Context) error {
	return processor.engine.Start(systemContext)
}

func (processor *Processor) handleMessage(requestContext context.Context, event *events.TextCreatedEvent, message jetstream.Msg) error {
	processingContext, cancelProcessing := context.WithTimeout(requestContext, MessageProcessingTimeout)
	defer cancelProcessing()

	if event.TextKey == "" {
		processor.serviceLogger.Errorf("Received event with empty TextKey for Workflow %s", event.Header.WorkflowIdentifier)
		_ = message.Term()
		return fmt.Errorf("empty TextKey")
	}

	processor.serviceLogger.Infof("Processing Page %d for Workflow %s", event.PageNumber, event.Header.WorkflowIdentifier)

	// Lifecycle: Initialized
	processor.publishLifecycleEvent(processingContext, event, "", events.SubjectTextToSpeechInitialized)

	audioKey, executionError := processor.executeJob(processingContext, event)
	if executionError != nil {
		processor.serviceLogger.Errorf("Job execution failed: %v", executionError)
		processor.handleProcessingFailure(processingContext, message, message.Data())
		return executionError
	}

	// Lifecycle: Completed
	processor.publishLifecycleEvent(processingContext, event, audioKey, events.SubjectTextToSpeechCompleted)

	return nil
}

func (processor *Processor) handleProcessingFailure(requestContext context.Context, message jetstream.Msg, payload []byte) {
	metadata, metadataError := message.Metadata()
	if metadataError == nil {
		if metadata.NumDelivered < 10 {
			processor.serviceLogger.Warnf("Processing failed (Attempt %d/10). Retrying in 20s...", metadata.NumDelivered)
			_ = message.NakWithDelay(20 * time.Second)
			return
		}
	}

	processor.serviceLogger.Errorf("Processing failed after attempts. Moving to DLQ.")

	if processor.deadLetterQueueSubject == "" {
		_ = message.Nak()
		return
	}

	for attempt := 1; attempt <= DeadLetterQueuePublishMaxRetries; attempt++ {
		if _, publishError := processor.jetStreamPublisher.Publish(requestContext, processor.deadLetterQueueSubject, payload); publishError == nil {
			_ = message.Ack()
			return
		}
		time.Sleep(DeadLetterQueueBackoffDuration)
	}

	_ = message.NakWithDelay(DeadLetterQueueBackoffDuration)
}

func (processor *Processor) executeJob(requestContext context.Context, event *events.TextCreatedEvent) (string, error) {
	// Lifecycle: Ready
	processor.publishLifecycleEvent(requestContext, event, "", events.SubjectTextToSpeechReady)

	// Lifecycle: Started (Publishing 'Started' signal)
	if publishStartedError := processor.publishStarted(requestContext, event); publishStartedError != nil {
		processor.serviceLogger.Warnf("Failed to publish text-to-speech started event: %v", publishStartedError)
	}

	if event.PageNumber == 1 && event.Settings != nil && event.Settings.AudioSessionConfig != nil && event.Settings.AudioSessionConfig.MusicPrompt != "" {
		processor.serviceLogger.Infof("Requesting background music generation for Workflow %s", event.Header.WorkflowIdentifier)
		if publishMusicRequestError := processor.publishMusicCreated(requestContext, event.Header, event.Settings.AudioSessionConfig.MusicPrompt, 180, event.Settings.AudioSessionConfig.GenerationConfig); publishMusicRequestError != nil {
			processor.serviceLogger.Errorf("Failed to publish music created: %v", publishMusicRequestError)
		}
	}

	cleanText, retrievalError := processor.retrieveAndCleanText(requestContext, event.TextKey)
	if retrievalError != nil {
		return "", retrievalError
	}

	var textToSpeechConfiguration core.TextToSpeechConfiguration
	if event.Settings != nil && event.Settings.AudioSessionConfig != nil {
		textToSpeechConfiguration = core.TextToSpeechConfiguration{
			SessionIdentifier: event.Settings.AudioSessionConfig.SessionIdentifier,
			VoiceIdentifier:   event.Settings.AudioSessionConfig.VoiceIdentifier,
			VoiceStyle:        event.Settings.AudioSessionConfig.VoiceStyle,
			MusicPrompt:       event.Settings.AudioSessionConfig.MusicPrompt,
			TextDirective:     event.Settings.AudioSessionConfig.TextDirective,
		}
	}

	var audioData []byte
	if strings.Contains(string(cleanText), NoSpeechMarker) {
		audioData = audio.GenerateSilentWav(1*time.Second, getAudioSampleRateHz(), 1, 32)
	} else {
		var generationError error
		audioData, generationError = processor.textToSpeechProcessor.Process(requestContext, cleanText, textToSpeechConfiguration)
		if generationError != nil {
			return "", fmt.Errorf("text-to-speech generation failed: %w", generationError)
		}
	}

	audioChunkKey := fmt.Sprintf(AudioChunkKeyFormat, event.Header.WorkflowIdentifier, event.PageNumber)
	if uploadError := processor.audioObjectStore.Upload(requestContext, audioChunkKey, audioData); uploadError != nil {
		return "", fmt.Errorf("audio upload failed: %w", uploadError)
	}

	// Lifecycle: Created (triggers next step - audio-mixer-service)
	processor.publishLifecycleEvent(requestContext, event, audioChunkKey, events.SubjectTextToSpeechCreated)

	completionEvent := events.TextToSpeechCompletedEvent{
		Header:     event.Header,
		PageNumber: event.PageNumber,
		TotalPages: event.TotalPages,
		AudioKey:   audioChunkKey,
	}

	return audioChunkKey, processor.publishResult(requestContext, &completionEvent)
}

func (processor *Processor) retrieveAndCleanText(requestContext context.Context, textKey string) ([]byte, error) {
	textContent, downloadError := processor.textObjectStore.Download(requestContext, textKey)
	if downloadError != nil {
		return nil, fmt.Errorf("download text failed: %w", downloadError)
	}

	var textSegments []string
	if jsonError := json.Unmarshal(textContent, &textSegments); jsonError != nil {
		return nil, fmt.Errorf("failed to unmarshal text segments (contract violation): %w", jsonError)
	}

	joinedText := strings.Join(textSegments, "\n\n")
	return []byte(joinedText), nil
}

func (processor *Processor) publishLifecycleEvent(ctx context.Context, source *events.TextCreatedEvent, audioKey, subject string) {
	lifecycleEvent := events.TextToSpeechCompletedEvent{
		Header:     source.Header,
		PageNumber: source.PageNumber,
		TotalPages: source.TotalPages,
		AudioKey:   audioKey,
	}
	data, _ := json.Marshal(lifecycleEvent)
	_, _ = processor.jetStreamPublisher.Publish(ctx, subject, data)
}

func (processor *Processor) publishStarted(requestContext context.Context, source *events.TextCreatedEvent) error {
	if processor.textToSpeechStartedSubject == "" {
		return nil
	}

	event := events.TextToSpeechStartedEvent{
		Header:     source.Header,
		PageNumber: source.PageNumber,
		TotalPages: source.TotalPages,
	}

	data, marshalError := json.Marshal(event)
	if marshalError != nil {
		return marshalError
	}

	_, publishError := processor.jetStreamPublisher.Publish(requestContext, processor.textToSpeechStartedSubject, data)
	return publishError
}

func (processor *Processor) publishMusicCreated(requestContext context.Context, header events.EventHeader, prompt string, duration int, configuration *events.LyriaGenerationConfig) error {
	if processor.musicRequestSubject == "" {
		return nil
	}

	event := events.MusicCreatedEvent{
		Header:           header,
		Prompt:           prompt,
		DurationSeconds:  duration,
		GenerationConfig: configuration,
	}

	data, marshalError := json.Marshal(event)
	if marshalError != nil {
		return marshalError
	}

	_, publishError := processor.jetStreamPublisher.Publish(requestContext, processor.musicRequestSubject, data)
	return publishError
}

func (processor *Processor) publishResult(requestContext context.Context, event *events.TextToSpeechCompletedEvent) error {
	data, marshalError := json.Marshal(event)
	if marshalError != nil {
		return fmt.Errorf("marshal event failed: %w", marshalError)
	}
	if _, publishError := processor.jetStreamPublisher.Publish(requestContext, processor.producerSubject, data); publishError != nil {
		return fmt.Errorf("publish event failed: %w", publishError)
	}
	return nil
}
