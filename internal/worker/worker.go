/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/book-expert/common-events"
	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// MessageProcessingTimeout defines the maximum duration allowed for processing a single TTS job.
	MessageProcessingTimeout = 3600 * time.Second

	// NatsFetchMaxWaitDuration defines how long the worker waits for a batch of messages from NATS.
	NatsFetchMaxWaitDuration = 10 * time.Second

	// DeadLetterQueuePublishMaxRetries defines the number of attempts to publish a failed message to the DLQ.
	DeadLetterQueuePublishMaxRetries = 3

	// DeadLetterQueueBackoffDuration defines the wait time between DLQ publish attempts.
	DeadLetterQueueBackoffDuration = 2 * time.Second

	// AudioSampleRateHz is the standard sample rate for the generated WAV audio.
	AudioSampleRateHz = 48000

	// ProgressCompletedValue is the value stored in the Key-Value store to mark a page as processed.
	ProgressCompletedValue = "done"

	// KeyValueFilterPatternFormat defines the key pattern for listing progress keys in the KV store.
	KeyValueFilterPatternFormat = "%s.page.*"

	// KeyValueKeyFormat defines the key format for storing progress in the KV store.
	KeyValueKeyFormat = "%s.page.%d"

	// TotalPagesKeyFormat defines the key format for storing total pages.
	TotalPagesKeyFormat = "%s.total_pages"

	// MusicRequestedKeyFormat defines the key format for marking that music was requested.
	MusicRequestedKeyFormat = "%s.music.requested"

	// MusicReadyKeyFormat defines the key format for marking that music is ready.
	MusicReadyKeyFormat = "%s.music.ready"

	// AudioChunkKeyFormat defines the naming convention for individual page audio chunks in the Object Store.
	AudioChunkKeyFormat = "%s_page_%d.wav"

	// NoSpeechMarker is the token used in text to indicate silence should be generated.
	NoSpeechMarker = "[NO_SPEECH]"
)

// JetStreamPublisher defines the interface for publishing messages to JetStream.
type JetStreamPublisher interface {
	Publish(requestContext context.Context, subject string, data []byte, options ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Worker manages the lifecycle of processing TTS requests from NATS.
type Worker struct {
	natsConnection            *nats.Conn
	jetStreamPublisher        JetStreamPublisher
	jetStreamContext          jetstream.JetStream
	subscriptionStream        string
	subscriptionSubject       string
	consumerDurableName       string
	producerSubject           string
	ttsStartedSubject         string
	musicStartedSubject       string
	musicRequestSubject       string
	musicCreatedSubject       string
	aggregationStartedSubject string
	deadLetterQueueSubject    string
	textObjectStore           core.ObjectStore
	audioObjectStore          core.ObjectStore
	progressKeyValueStore     jetstream.KeyValue
	ttsProcessor              core.TTSProcessor
	logger                    *logger.Logger
	workerCount               int
	waitGroup                 sync.WaitGroup
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
	ttsStartedSubject string,
	musicStartedSubject string,
	musicRequestSubject string,
	musicCreatedSubject string,
	aggregationStartedSubject string,
	deadLetterQueueSubject string,
	textObjectStore core.ObjectStore,
	audioObjectStore core.ObjectStore,
	progressKeyValueStore jetstream.KeyValue,
	ttsProcessor core.TTSProcessor,
	serviceLogger *logger.Logger,
	workerCount int,
) (*Worker, error) {
	if workerCount < 1 {
		workerCount = 1
	}
	return &Worker{
		natsConnection:            natsConnection,
		jetStreamPublisher:        jetStreamPublisher,
		jetStreamContext:          jetStreamContext,
		subscriptionStream:        subscriptionStream,
		subscriptionSubject:       subscriptionSubject,
		consumerDurableName:       consumerDurableName,
		producerSubject:           producerSubject,
		ttsStartedSubject:         ttsStartedSubject,
		musicStartedSubject:       musicStartedSubject,
		musicRequestSubject:       musicRequestSubject,
		musicCreatedSubject:       musicCreatedSubject,
		aggregationStartedSubject: aggregationStartedSubject,
		deadLetterQueueSubject:    deadLetterQueueSubject,
		textObjectStore:           textObjectStore,
		audioObjectStore:          audioObjectStore,
		progressKeyValueStore:     progressKeyValueStore,
		ttsProcessor:              ttsProcessor,
		logger:                    serviceLogger,
		workerCount:               workerCount,
	}, nil
}

// Run executes the main worker loop.
func (worker *Worker) Run(systemContext context.Context) error {
	// 1. Subscribe to Text Processing Stream (N Workers)
	for index := 0; index < worker.workerCount; index++ {
		if executionError := worker.bindAndConsume(systemContext, worker.subscriptionStream, worker.subscriptionSubject, worker.consumerDurableName, worker.processMessage); executionError != nil {
			return executionError
		}
	}

	worker.logger.Infof("Worker online with %d text workers.", worker.workerCount)
	worker.waitGroup.Wait()
	return nil
}

func (worker *Worker) bindAndConsume(
	systemContext context.Context,
	streamName, subject, durableName string,
	handler func(requestContext context.Context, message jetstream.Msg),
) error {
	stream, streamError := worker.jetStreamContext.Stream(systemContext, streamName)
	if streamError != nil {
		return fmt.Errorf("failed to bind stream %s: %w", streamName, streamError)
	}

	consumer, consumerError := stream.CreateOrUpdateConsumer(systemContext, jetstream.ConsumerConfig{
		Durable:       durableName,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if consumerError != nil {
		return fmt.Errorf("failed to create consumer %s: %w", durableName, consumerError)
	}

	worker.waitGroup.Add(1)
	go func() {
		defer worker.waitGroup.Done()
		worker.consumeLoop(systemContext, consumer, handler)
	}()

	return nil
}

func (worker *Worker) consumeLoop(systemContext context.Context, consumer jetstream.Consumer, handler func(requestContext context.Context, message jetstream.Msg)) {
	for {
		if systemContext.Err() != nil {
			return
		}

		messageBatch, fetchError := consumer.Fetch(1, jetstream.FetchMaxWait(NatsFetchMaxWaitDuration))
		if fetchError != nil {
			if !errors.Is(fetchError, nats.ErrTimeout) {
				worker.logger.Errorf("Fetch error: %v", fetchError)
				time.Sleep(1 * time.Second)
			}
			continue
		}

		for message := range messageBatch.Messages() {
			if systemContext.Err() != nil {
				return
			}
			handler(systemContext, message)
		}
	}
}

func (worker *Worker) processMessage(requestContext context.Context, message jetstream.Msg) {
	processingContext, cancelProcessing := context.WithTimeout(requestContext, MessageProcessingTimeout)
	defer cancelProcessing()

	event, parseError := worker.parseAndValidateEvent(message)
	if parseError != nil {
		worker.logger.Errorf("Event validation failed: %v", parseError)
		worker.handleProcessingFailure(processingContext, message, message.Data())
		return
	}

	worker.logger.Infof("Processing Page %d for Workflow %s", event.PageNumber, event.Header.WorkflowID)

	if inProgressError := message.InProgress(); inProgressError != nil {
		worker.logger.Warnf("Failed to signal InProgress: %v", inProgressError)
	}

	stopKeepAlive := worker.keepAlive(processingContext, message)
	defer stopKeepAlive()

	if executionError := worker.executeTTSJob(processingContext, event); executionError != nil {
		worker.logger.Errorf("Job execution failed: %v", executionError)
		worker.handleProcessingFailure(processingContext, message, message.Data())
		return
	}

	if ackError := message.Ack(); ackError != nil {
		worker.logger.Errorf("Failed to ACK message: %v", ackError)
	}
}

func (worker *Worker) keepAlive(requestContext context.Context, message jetstream.Msg) func() {
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-requestContext.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if inProgressError := message.InProgress(); inProgressError != nil {
					worker.logger.Warnf("Failed to send keep-alive signal: %v", inProgressError)
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (worker *Worker) handleProcessingFailure(requestContext context.Context, message jetstream.Msg, payload []byte) {
	metadata, metadataError := message.Metadata()
	if metadataError == nil {
		if metadata.NumDelivered < 10 {
			worker.logger.Warnf("Processing failed (Attempt %d/10). Retrying in 20s...", metadata.NumDelivered)
			_ = message.NakWithDelay(20 * time.Second)
			return
		}
	}

	worker.logger.Errorf("Processing failed after attempts. Moving to DLQ.")

	if worker.deadLetterQueueSubject == "" {
		_ = message.Nak()
		return
	}

	for attempt := 1; attempt <= DeadLetterQueuePublishMaxRetries; attempt++ {
		if _, publishError := worker.jetStreamPublisher.Publish(requestContext, worker.deadLetterQueueSubject, payload); publishError == nil {
			_ = message.Ack()
			return
		}
		time.Sleep(DeadLetterQueueBackoffDuration)
	}

	_ = message.NakWithDelay(DeadLetterQueueBackoffDuration)
}

func (worker *Worker) executeTTSJob(requestContext context.Context, event *events.TextProcessedEvent) error {
	// 0a. Publish TTS Started
	if publishStartedError := worker.publishTTSStarted(requestContext, event); publishStartedError != nil {
		worker.logger.Warnf("Failed to publish TTS started event: %v", publishStartedError)
	}

	// 0b. Request Background Music Generation (Page 1 Only)
	if event.PageNumber == 1 && event.Settings != nil && event.Settings.AudioSessionConfig != nil && event.Settings.AudioSessionConfig.MusicPrompt != "" {
		worker.logger.Infof("Requesting background music generation for Workflow %s", event.Header.WorkflowID)
		if publishMusicRequestError := worker.publishMusicRequest(requestContext, event.Header, event.Settings.AudioSessionConfig.MusicPrompt, 180, event.Settings.AudioSessionConfig.GenerationConfig); publishMusicRequestError != nil {
			worker.logger.Errorf("Failed to publish music request: %v", publishMusicRequestError)
		}
	}

	// 1. Get Text
	cleanText, retrievalError := worker.retrieveAndCleanText(requestContext, event.TextKey)
	if retrievalError != nil {
		return retrievalError
	}

	// 2. Create TTS Configuration
	var ttsConfiguration core.TTSConfig
	if event.Settings != nil && event.Settings.AudioSessionConfig != nil {
		ttsConfiguration = core.TTSConfig{
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
		audioData, generationError = worker.ttsProcessor.Process(requestContext, cleanText, ttsConfiguration)
		if generationError != nil {
			return fmt.Errorf("TTS generation failed: %w", generationError)
		}
	}

	// 4. Store Audio Chunk
	audioChunkKey := fmt.Sprintf(AudioChunkKeyFormat, event.Header.WorkflowID, event.PageNumber)
	if uploadError := worker.audioObjectStore.Upload(requestContext, audioChunkKey, audioData); uploadError != nil {
		return fmt.Errorf("audio upload failed: %w", uploadError)
	}

	// 5. Publish Completion Event
	completionEvent := events.TTSCompletedEvent{
		Header:     event.Header,
		PageNumber: event.PageNumber,
		TotalPages: event.TotalPages,
		AudioKey:   audioChunkKey,
	}

	return worker.publishTTSCompleted(requestContext, &completionEvent)
}

func (worker *Worker) retrieveAndCleanText(requestContext context.Context, textKey string) ([]byte, error) {
	textContent, downloadError := worker.textObjectStore.Download(requestContext, textKey)
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

func (worker *Worker) publishTTSStarted(requestContext context.Context, source *events.TextProcessedEvent) error {
	if worker.ttsStartedSubject == "" {
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

	_, publishError := worker.jetStreamPublisher.Publish(requestContext, worker.ttsStartedSubject, data)
	return publishError
}

func (worker *Worker) publishMusicRequest(requestContext context.Context, header events.EventHeader, prompt string, duration int, config *events.LyriaGenerationConfig) error {
	if worker.musicRequestSubject == "" {
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

	_, publishError := worker.jetStreamPublisher.Publish(requestContext, worker.musicRequestSubject, data)
	return publishError
}

func (worker *Worker) publishTTSCompleted(requestContext context.Context, event *events.TTSCompletedEvent) error {
	data, marshalError := json.Marshal(event)
	if marshalError != nil {
		return fmt.Errorf("marshal event failed: %w", marshalError)
	}
	if _, publishError := worker.jetStreamPublisher.Publish(requestContext, worker.producerSubject, data); publishError != nil {
		return fmt.Errorf("publish event failed: %w", publishError)
	}
	return nil
}

func (worker *Worker) parseAndValidateEvent(message jetstream.Msg) (*events.TextProcessedEvent, error) {
	var event events.TextProcessedEvent
	if unmarshalError := json.Unmarshal(message.Data(), &event); unmarshalError != nil {
		return nil, unmarshalError
	}
	return &event, nil
}
