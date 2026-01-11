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
	Publish(context context.Context, subject string, data []byte, options ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Worker manages the lifecycle of processing TTS requests from NATS.
type Worker struct {
	natsConnection            *nats.Conn
	jetStreamPublisher        JetStreamPublisher
	jetStream                 jetstream.JetStream
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
	jetStream jetstream.JetStream,
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
		jetStream:                 jetStream,
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
func (worker *Worker) Run(context context.Context) error {
	// 1. Subscribe to Text Processing Stream (N Workers)
	for index := 0; index < worker.workerCount; index++ {
		if error := worker.bindAndConsume(context, worker.subscriptionStream, worker.subscriptionSubject, worker.consumerDurableName, worker.processMessage); error != nil {
			return error
		}
	}

	worker.logger.Infof("Worker online with %d text workers.", worker.workerCount)
	worker.waitGroup.Wait()
	return nil
}

func (worker *Worker) bindAndConsume(
	context context.Context,
	streamName, subject, durableName string,
	handler func(parentContext context.Context, message jetstream.Msg),
) error {
	stream, error := worker.jetStream.Stream(context, streamName)
	if error != nil {
		return fmt.Errorf("failed to bind stream %s: %w", streamName, error)
	}

	consumer, error := stream.CreateOrUpdateConsumer(context, jetstream.ConsumerConfig{
		Durable:       durableName,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if error != nil {
		return fmt.Errorf("failed to create consumer %s: %w", durableName, error)
	}

	worker.waitGroup.Add(1)
	go func() {
		defer worker.waitGroup.Done()
		worker.consumeLoop(context, consumer, handler)
	}()

	return nil
}

func (worker *Worker) consumeLoop(context context.Context, consumer jetstream.Consumer, handler func(parentContext context.Context, message jetstream.Msg)) {
	for {
		if context.Err() != nil {
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
			if context.Err() != nil {
				return
			}
			handler(context, message)
		}
	}
}

func (worker *Worker) processMessage(parentContext context.Context, message jetstream.Msg) {
	processingContext, cancelProcessing := context.WithTimeout(parentContext, MessageProcessingTimeout)
	defer cancelProcessing()

	event, parseError := worker.parseAndValidateEvent(message)
	if parseError != nil {
		worker.logger.Errorf("Event validation failed: %v", parseError)
		worker.handleProcessingFailure(processingContext, message, message.Data())
		return
	}

	worker.logger.Infof("Processing Page %d for Workflow %s", event.PageNumber, event.Header.WorkflowID)

	if error := message.InProgress(); error != nil {
		worker.logger.Warnf("Failed to signal InProgress: %v", error)
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

func (worker *Worker) keepAlive(context context.Context, message jetstream.Msg) func() {
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-context.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if error := message.InProgress(); error != nil {
					worker.logger.Warnf("Failed to send keep-alive signal: %v", error)
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (worker *Worker) handleProcessingFailure(context context.Context, message jetstream.Msg, payload []byte) {
	metadata, error := message.Metadata()
	if error == nil {
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
		if _, error := worker.jetStreamPublisher.Publish(context, worker.deadLetterQueueSubject, payload); error == nil {
			_ = message.Ack()
			return
		}
		time.Sleep(DeadLetterQueueBackoffDuration)
	}

	_ = message.NakWithDelay(DeadLetterQueueBackoffDuration)
}

func (worker *Worker) executeTTSJob(context context.Context, event *events.TextProcessedEvent) error {
	// 0a. Publish TTS Started
	if error := worker.publishTTSStarted(context, event); error != nil {
		worker.logger.Warnf("Failed to publish TTS started event: %v", error)
	}

	// 0b. Request Background Music Generation (Page 1 Only)
	if event.PageNumber == 1 && event.Settings != nil && event.Settings.AudioSessionConfig != nil && event.Settings.AudioSessionConfig.MusicPrompt != "" {
		worker.logger.Infof("Requesting background music generation for Workflow %s", event.Header.WorkflowID)
		if error := worker.publishMusicRequest(context, event.Header, event.Settings.AudioSessionConfig.MusicPrompt, 180); error != nil {
			worker.logger.Errorf("Failed to publish music request: %v", error)
		}
	}

	// 1. Get Text
	cleanText, error := worker.retrieveAndCleanText(context, event.TextKey)
	if error != nil {
		return error
	}

	// 2. Create TTS Configuration
	var ttsConfiguration core.TTSConfig
	if event.Settings != nil && event.Settings.AudioSessionConfig != nil {
		ttsConfiguration = core.TTSConfig{
			SessionID:   event.Settings.AudioSessionConfig.SessionID,
			VoiceID:     event.Settings.AudioSessionConfig.VoiceID,
			MusicPrompt: event.Settings.AudioSessionConfig.MusicPrompt,
		}
	}

	// 3. Generate Audio
	var audioData []byte
	if strings.Contains(string(cleanText), NoSpeechMarker) {
		audioData = audio.GenerateSilentWav(1*time.Second, AudioSampleRateHz, 1, 32)
	} else {
		audioData, error = worker.ttsProcessor.Process(context, cleanText, ttsConfiguration)
		if error != nil {
			return fmt.Errorf("TTS generation failed: %w", error)
		}
	}

	// 4. Store Audio Chunk
	audioChunkKey := fmt.Sprintf(AudioChunkKeyFormat, event.Header.WorkflowID, event.PageNumber)
	if error := worker.audioObjectStore.Upload(context, audioChunkKey, audioData); error != nil {
		return fmt.Errorf("audio upload failed: %w", error)
	}

	// 5. Publish Completion Event
	completionEvent := events.TTSCompletedEvent{
		Header:     event.Header,
		PageNumber: event.PageNumber,
		TotalPages: event.TotalPages,
		AudioKey:   audioChunkKey,
	}

	return worker.publishTTSCompleted(context, &completionEvent)
}

func (worker *Worker) retrieveAndCleanText(context context.Context, textKey string) ([]byte, error) {
	textContent, error := worker.textObjectStore.Download(context, textKey)
	if error != nil {
		return nil, fmt.Errorf("download text failed: %w", error)
	}

	var textSegments []string
	if jsonError := json.Unmarshal(textContent, &textSegments); jsonError == nil {
		joinedText := ""
		for index, segment := range textSegments {
			if index > 0 {
				joinedText += "\n\n"
			}
			joinedText += segment
		}
		return []byte(joinedText), nil
	}

	return textContent, nil
}

func (worker *Worker) publishTTSStarted(context context.Context, source *events.TextProcessedEvent) error {
	if worker.ttsStartedSubject == "" {
		return nil
	}

	event := events.TTSStartedEvent{
		Header:     source.Header,
		PageNumber: source.PageNumber,
		TotalPages: source.TotalPages,
	}

	data, error := json.Marshal(event)
	if error != nil {
		return error
	}

	_, error = worker.jetStreamPublisher.Publish(context, worker.ttsStartedSubject, data)
	return error
}

func (worker *Worker) publishMusicRequest(context context.Context, header events.EventHeader, prompt string, duration int) error {
	if worker.musicRequestSubject == "" {
		return nil
	}

	event := events.MusicRequestEvent{
		Header:          header,
		Prompt:          prompt,
		DurationSeconds: duration,
	}

	data, error := json.Marshal(event)
	if error != nil {
		return error
	}

	_, error = worker.jetStreamPublisher.Publish(context, worker.musicRequestSubject, data)
	return error
}

func (worker *Worker) publishTTSCompleted(context context.Context, event *events.TTSCompletedEvent) error {
	data, error := json.Marshal(event)
	if error != nil {
		return fmt.Errorf("marshal event failed: %w", error)
	}
	if _, error := worker.jetStreamPublisher.Publish(context, worker.producerSubject, data); error != nil {
		return fmt.Errorf("publish event failed: %w", error)
	}
	return nil
}

func (worker *Worker) parseAndValidateEvent(message jetstream.Msg) (*events.TextProcessedEvent, error) {
	var event events.TextProcessedEvent
	if error := json.Unmarshal(message.Data(), &event); error != nil {
		return nil, error
	}
	return &event, nil
}

