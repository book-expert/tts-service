// Package worker implements the NATS consumer logic for the TTS service.
// It handles message consumption, text-to-speech processing, and audio aggregation.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/events"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// MessageProcessingTimeout defines the maximum duration allowed for processing a single TTS job.
	MessageProcessingTimeout = 700 * time.Second

	// NatsFetchMaxWaitDuration defines how long the worker waits for a batch of messages from NATS.
	NatsFetchMaxWaitDuration = 10 * time.Second

	// DeadLetterQueuePublishMaxRetries defines the number of attempts to publish a failed message to the DLQ.
	DeadLetterQueuePublishMaxRetries = 3

	// DeadLetterQueueBackoffDuration defines the wait time between DLQ publish attempts.
	DeadLetterQueueBackoffDuration = 2 * time.Second

	// AudioSampleRateHz is the standard sample rate for the generated WAV audio.
	AudioSampleRateHz = 24000

	// AudioChannelsMono indicates single-channel audio.
	AudioChannelsMono = 1

	// AudioBitsPerSample defines the bit depth of the PCM audio.
	AudioBitsPerSample = 16

	// ProgressCompletedValue is the value stored in the Key-Value store to mark a page as processed.
	ProgressCompletedValue = "done"

	// KeyValueFilterPatternFormat defines the key pattern for listing progress keys in the KV store.
	KeyValueFilterPatternFormat = "%s.page.*"

	// KeyValueKeyFormat defines the key format for storing progress in the KV store.
	KeyValueKeyFormat = "%s.page.%d"

	// AudioChunkKeyFormat defines the naming convention for individual page audio chunks in the Object Store.
	AudioChunkKeyFormat = "%s_page_%d.pcm"

	// FinalAudioKeyFormat defines the naming convention for the aggregated WAV file.
	FinalAudioKeyFormat = "%s.wav"
)

// JetStreamPublisher defines the interface for publishing messages to JetStream.
// Why: Isolates the worker from the concrete JetStream implementation for easier testing.
type JetStreamPublisher interface {
	Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// NatsWorker manages the lifecycle of processing TTS requests from NATS.
type NatsWorker struct {
	natsConnection         *nats.Conn
	jetstreamPublisher     JetStreamPublisher
	jetstreamAdmin         jetstream.JetStream
	subscriptionStream     string
	subscriptionSubject    string
	consumerDurableName    string
	producerSubject        string
	deadLetterQueueSubject string
	textObjectStore        core.ObjectStore
	audioObjectStore       core.ObjectStore
	progressKeyValueStore  jetstream.KeyValue
	ttsProcessor           core.TTSProcessor
	systemLogger           *logger.Logger
	workerCount            int
	requestsPerMinute      int
}

// NewNatsWorker initializes a new NatsWorker with all necessary dependencies.
func NewNatsWorker(
	natsConnection *nats.Conn,
	jetstreamAdmin jetstream.JetStream,
	jetstreamPublisher JetStreamPublisher,
	subscriptionStream string,
	subscriptionSubject string,
	consumerDurableName string,
	producerSubject string,
	deadLetterQueueSubject string,
	textObjectStore core.ObjectStore,
	audioObjectStore core.ObjectStore,
	progressKeyValueStore jetstream.KeyValue,
	ttsProcessor core.TTSProcessor,
	systemLogger *logger.Logger,
	workerCount int,
	requestsPerMinute int,
) (*NatsWorker, error) {
	if workerCount < 1 {
		workerCount = 1
	}
	if requestsPerMinute < 1 {
		requestsPerMinute = 1 // Prevent division by zero
	}
	return &NatsWorker{
		natsConnection:         natsConnection,
		jetstreamPublisher:     jetstreamPublisher,
		jetstreamAdmin:         jetstreamAdmin,
		subscriptionStream:     subscriptionStream,
		subscriptionSubject:    subscriptionSubject,
		consumerDurableName:    consumerDurableName,
		producerSubject:        producerSubject,
		deadLetterQueueSubject: deadLetterQueueSubject,
		textObjectStore:        textObjectStore,
		audioObjectStore:       audioObjectStore,
		progressKeyValueStore:  progressKeyValueStore,
		ttsProcessor:           ttsProcessor,
		systemLogger:           systemLogger,
		workerCount:            workerCount,
		requestsPerMinute:      requestsPerMinute,
	}, nil
}

// Run executes the main worker loop.
// Flow: Ensure Consumer -> Fetch -> Process -> Ack/Nak
func (worker *NatsWorker) Run(ctx context.Context) error {
	stream, err := worker.jetstreamAdmin.Stream(ctx, worker.subscriptionStream)
	if err != nil {
		return fmt.Errorf("failed to retrieve stream %s: %w", worker.subscriptionStream, err)
	}

	consumerConfig := jetstream.ConsumerConfig{
		Durable:       worker.consumerDurableName,
		FilterSubject: worker.subscriptionSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create or update consumer %s: %w", worker.consumerDurableName, err)
	}

	worker.systemLogger.Infof("Worker running on subject %s with %d concurrent workers. Rate limit: %d RPM", worker.subscriptionSubject, worker.workerCount, worker.requestsPerMinute)

	// Calculate the interval between requests to satisfy the Rate Limit (RPM).
	// Example: 10 RPM = 1 request every 6 seconds.
	rateLimitInterval := time.Minute / time.Duration(worker.requestsPerMinute)
	rateLimiterTicker := time.NewTicker(rateLimitInterval)
	defer rateLimiterTicker.Stop()

	var wg sync.WaitGroup

	for i := 0; i < worker.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker.consumeLoop(ctx, consumer, workerID, rateLimiterTicker.C)
		}(i)
	}

	wg.Wait()
	return nil
}

func (worker *NatsWorker) consumeLoop(ctx context.Context, consumer jetstream.Consumer, workerID int, tick <-chan time.Time) {
	for {
		// Check context cancellation at start of loop
		if ctx.Err() != nil {
			return
		}

		messageBatch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(NatsFetchMaxWaitDuration))
		if fetchErr != nil {
			if !errors.Is(fetchErr, nats.ErrTimeout) {
				worker.systemLogger.Errorf("[Worker %d] Failed to fetch message batch: %v", workerID, fetchErr)
			}
			continue
		}

		for message := range messageBatch.Messages() {
			// Rate Limit: Wait for the next tick before processing.
			// All workers share the same ticker channel, creating a global queue.
			select {
			case <-tick:
				// Proceed
			case <-ctx.Done():
				return
			}
			worker.processMessage(ctx, message)
		}
	}
}

// processMessage handles the lifecycle of a single NATS message.
func (worker *NatsWorker) processMessage(ctx context.Context, message jetstream.Msg) {
	processingContext, cancelProcessing := context.WithTimeout(ctx, MessageProcessingTimeout)
	defer cancelProcessing()

	event, parseErr := worker.parseAndValidateEvent(message)
	if parseErr != nil {
		worker.systemLogger.Errorf("Event validation failed: %v", parseErr)
		// Invalid events are terminal; move to DLQ immediately.
		worker.handleProcessingFailure(processingContext, message, message.Data())
		return
	}

	worker.systemLogger.Infof("Processing Page %d for Workflow %s", event.PageNumber, event.Header.WorkflowID)

	if err := message.InProgress(); err != nil {
		worker.systemLogger.Warnf("Failed to signal InProgress: %v", err)
	}

	// Start Keep-Alive (Heartbeat) to prevent NATS redelivery during long processing
	stopKeepAlive := worker.keepAlive(processingContext, message)
	defer stopKeepAlive()

	if executionErr := worker.executeTTSJob(processingContext, event); executionErr != nil {
		worker.systemLogger.Errorf("Job execution failed: %v", executionErr)
		worker.handleProcessingFailure(processingContext, message, message.Data())
		return
	}

	if ackErr := message.Ack(); ackErr != nil {
		worker.systemLogger.Errorf("Failed to ACK message: %v", ackErr)
	}
}

// keepAlive starts a background ticker that periodically sends InProgress signals
// to NATS to prevent the message from being redelivered due to AckWait timeout.
// It returns a cancellation function that must be called when processing is done.
func (worker *NatsWorker) keepAlive(ctx context.Context, msg jetstream.Msg) func() {
	// Send InProgress every 10 seconds.
	// Ensure this is less than the Consumer's AckWait (default often 30s).
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					worker.systemLogger.Warnf("Failed to send keep-alive signal: %v", err)
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

// handleProcessingFailure manages failed messages by attempting to publish them to the DLQ.
func (worker *NatsWorker) handleProcessingFailure(ctx context.Context, message jetstream.Msg, payload []byte) {
	if worker.deadLetterQueueSubject == "" {
		_ = message.Nak()
		return
	}

	for attempt := 1; attempt <= DeadLetterQueuePublishMaxRetries; attempt++ {
		if _, err := worker.jetstreamPublisher.Publish(ctx, worker.deadLetterQueueSubject, payload); err == nil {
			// If DLQ publish succeeds, ACK the original message to remove it from the main queue.
			_ = message.Ack()
			return
		}
		time.Sleep(DeadLetterQueueBackoffDuration)
	}

	// Fallback to NAK if DLQ fails.
	_ = message.NakWithDelay(DeadLetterQueueBackoffDuration)
}

// executeTTSJob performs the core business logic.
func (worker *NatsWorker) executeTTSJob(ctx context.Context, event *events.TextProcessedEvent) error {
	// 1. Get Text
	cleanText, err := worker.retrieveAndCleanText(ctx, event.TextKey)
	if err != nil {
		return err
	}

	// 2. Generate Audio
	stylePrompt := buildStylePrompt(event.Settings)
	ttsConfiguration := core.TTSConfig{
		Voice:       event.Voice,
		StylePrompt: stylePrompt,
	}
	pcmAudioData, err := worker.ttsProcessor.Process(ctx, cleanText, ttsConfiguration)
	if err != nil {
		return fmt.Errorf("TTS generation failed: %w", err)
	}

	// 3. Store Audio Chunk
	audioChunkKey := fmt.Sprintf(AudioChunkKeyFormat, event.Header.WorkflowID, event.PageNumber)
	if err := worker.audioObjectStore.Upload(ctx, audioChunkKey, pcmAudioData); err != nil {
		return fmt.Errorf("audio upload failed: %w", err)
	}

	// 4. Update Progress
	if err := worker.updateProgress(ctx, event.Header.WorkflowID, event.PageNumber); err != nil {
		return err
	}

	// 5. Check Completeness & Aggregate
	isComplete, err := worker.checkCompleteness(ctx, event.Header.WorkflowID, event.TotalPages)
	if err != nil {
		return err
	}

	if isComplete {
		worker.systemLogger.Infof("All pages complete for %s. Aggregating audio.", event.Header.WorkflowID)
		return worker.aggregateAndFinalizeWorkflow(ctx, event.Header.WorkflowID, event.TotalPages, event.Header)
	}

	return nil
}

// retrieveAndCleanText downloads the raw text and normalizes it (JSON parsing if needed).
func (worker *NatsWorker) retrieveAndCleanText(ctx context.Context, textKey string) ([]byte, error) {
	textContent, err := worker.textObjectStore.Download(ctx, textKey)
	if err != nil {
		return nil, fmt.Errorf("download text failed: %w", err)
	}

	var textSegments []string
	if jsonErr := json.Unmarshal(textContent, &textSegments); jsonErr == nil {
		// Join segments with double newlines for clear pauses.
		joinedText := ""
		for i, segment := range textSegments {
			if i > 0 {
				joinedText += "\n\n"
			}
			joinedText += segment
		}
		return []byte(joinedText), nil
	}

	// Fallback to raw bytes if not JSON
	return textContent, nil
}

// updateProgress marks the current page as done in the KV store.
func (worker *NatsWorker) updateProgress(ctx context.Context, workflowID string, pageNumber int) error {
	progressKey := fmt.Sprintf(KeyValueKeyFormat, workflowID, pageNumber)
	if _, err := worker.progressKeyValueStore.Put(ctx, progressKey, []byte(ProgressCompletedValue)); err != nil {
		return fmt.Errorf("KV put failed for %s: %w", progressKey, err)
	}
	return nil
}

// checkCompleteness verifies if all pages for the workflow exist in the KV store.
func (worker *NatsWorker) checkCompleteness(ctx context.Context, workflowID string, totalPages int) (bool, error) {
	keyFilter := fmt.Sprintf(KeyValueFilterPatternFormat, workflowID)
	keyLister, err := worker.progressKeyValueStore.ListKeysFiltered(ctx, keyFilter)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("list keys failed: %w", err)
	}

	completedCount := 0
	for range keyLister.Keys() {
		completedCount++
	}

	return completedCount == totalPages, nil
}

// aggregateAndFinalizeWorkflow combines chunks and publishes the final event.
func (worker *NatsWorker) aggregateAndFinalizeWorkflow(
	ctx context.Context,
	workflowID string,
	totalPages int,
	header events.EventHeader,
) error {
	var aggregatedPCMData []byte

	// Sequential aggregation guarantees order.
	for pageIndex := 1; pageIndex <= totalPages; pageIndex++ {
		chunkKey := fmt.Sprintf(AudioChunkKeyFormat, workflowID, pageIndex)
		chunkData, err := worker.audioObjectStore.Download(ctx, chunkKey)
		if err != nil {
			return fmt.Errorf("download chunk %s failed: %w", chunkKey, err)
		}
		aggregatedPCMData = append(aggregatedPCMData, chunkData...)
	}

	// Calling the function located in wav.go
	finalWavData := withWAVHeader(aggregatedPCMData, AudioSampleRateHz, AudioChannelsMono, AudioBitsPerSample)
	finalKey := fmt.Sprintf(FinalAudioKeyFormat, workflowID)

	if err := worker.audioObjectStore.Upload(ctx, finalKey, finalWavData); err != nil {
		return fmt.Errorf("upload final WAV failed: %w", err)
	}

	completionEvent := &events.AudioChunkCreatedEvent{
		Header:     header,
		AudioKey:   finalKey,
		PageNumber: 0, // 0 indicates the merged file
		TotalPages: totalPages,
	}

	if err := worker.publishCompletionEvent(ctx, completionEvent); err != nil {
		return err
	}

	return nil
}

func (worker *NatsWorker) publishCompletionEvent(ctx context.Context, event *events.AudioChunkCreatedEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event failed: %w", err)
	}
	if _, err := worker.jetstreamPublisher.Publish(ctx, worker.producerSubject, data); err != nil {
		return fmt.Errorf("publish event failed: %w", err)
	}
	return nil
}

func (worker *NatsWorker) parseAndValidateEvent(message jetstream.Msg) (*events.TextProcessedEvent, error) {
	var event events.TextProcessedEvent
	if err := json.Unmarshal(message.Data(), &event); err != nil {
		return nil, err
	}
	return &event, nil
}
