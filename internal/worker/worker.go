/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS
--------------------------------------------------------------------------------
"Work is love made visible."

1.  PARTNERSHIP & CRAFT: We do not just "finish tasks"; we build with pride.
    We question decisions that compromise safety, security, or quality.
2.  HONESTY: We verify before we claim success. We write what we mean.
    If we don't know, we ask.
3.  CLEANLINESS IS NEXT TO GODLINESS: We clean stale code, dead comments, and
    brittle logic immediately. A sanitary codebase protects focus.
4.  SIMPLICITY IS KINDNESS: We choose the simple, explicit path. Complexity
    is a burden we refuse to place on others.
5.  PROTECTION: We handle errors, secure data, and ensure non-blocking flow.
    We do not leave traps for the future.

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
--------------------------------------------------------------------------------
*/

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	MessageProcessingTimeout = 1000 * time.Second

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

	// AudioChunkKeyFormat defines the naming convention for individual page audio chunks in the Object Store.
	AudioChunkKeyFormat = "%s_page_%d.pcm"

	// FinalAudioKeyFormat defines the naming convention for the aggregated WAV file.
	FinalAudioKeyFormat = "%s.wav"

	// MusicKeyFormat defines the naming convention for the background music file.
	MusicKeyFormat = "%s_music.wav"

	// MusicGenerationDuration defines the duration of the background music to generate (in seconds).
	MusicGenerationDuration = 180 // 3 minutes

	// NoSpeechMarker is the token used in text to indicate silence should be generated.
	NoSpeechMarker = "[NO_SPEECH]"
)

// JetStreamPublisher defines the interface for publishing messages to JetStream.
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
) (*NatsWorker, error) {
	if workerCount < 1 {
		workerCount = 1
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
	}, nil
}

// Run executes the main worker loop.
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

	worker.systemLogger.Infof("Worker running on subject %s with %d concurrent workers.", worker.subscriptionSubject, worker.workerCount)

	var wg sync.WaitGroup

	for i := 0; i < worker.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker.consumeLoop(ctx, consumer, workerID)
		}(i)
	}

	wg.Wait()
	return nil
}

func (worker *NatsWorker) consumeLoop(ctx context.Context, consumer jetstream.Consumer, workerID int) {
	for {
		if ctx.Err() != nil {
			return
		}

		messageBatch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(NatsFetchMaxWaitDuration))
		if fetchErr != nil {
			if !errors.Is(fetchErr, nats.ErrTimeout) {
				worker.systemLogger.Errorf("[Worker %d] Failed to fetch message batch: %v", workerID, fetchErr)
				// Prevent tight loop on persistent errors (e.g. stream not found)
				time.Sleep(1 * time.Second)
			}
			continue
		}

		for message := range messageBatch.Messages() {
			if ctx.Err() != nil {
				return
			}
			worker.processMessage(ctx, message)
		}
	}
}

func (worker *NatsWorker) processMessage(ctx context.Context, message jetstream.Msg) {
	processingContext, cancelProcessing := context.WithTimeout(ctx, MessageProcessingTimeout)
	defer cancelProcessing()

	event, parseErr := worker.parseAndValidateEvent(message)
	if parseErr != nil {
		worker.systemLogger.Errorf("Event validation failed: %v", parseErr)
		worker.handleProcessingFailure(processingContext, message, message.Data())
		return
	}

	worker.systemLogger.Infof("Processing Page %d for Workflow %s", event.PageNumber, event.Header.WorkflowID)

	if err := message.InProgress(); err != nil {
		worker.systemLogger.Warnf("Failed to signal InProgress: %v", err)
	}

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

func (worker *NatsWorker) keepAlive(ctx context.Context, msg jetstream.Msg) func() {
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

func (worker *NatsWorker) handleProcessingFailure(ctx context.Context, message jetstream.Msg, payload []byte) {
	if worker.deadLetterQueueSubject == "" {
		_ = message.Nak()
		return
	}

	for attempt := 1; attempt <= DeadLetterQueuePublishMaxRetries; attempt++ {
		if _, err := worker.jetstreamPublisher.Publish(ctx, worker.deadLetterQueueSubject, payload); err == nil {
			_ = message.Ack()
			return
		}
		time.Sleep(DeadLetterQueueBackoffDuration)
	}

	_ = message.NakWithDelay(DeadLetterQueueBackoffDuration)
}

func (worker *NatsWorker) executeTTSJob(ctx context.Context, event *events.TextProcessedEvent) error {
	// 0. Trigger Background Music Generation (Page 1 Only)
	if event.PageNumber == 1 && event.Settings != nil && event.Settings.AudioSessionConfig != nil && event.Settings.AudioSessionConfig.MusicPrompt != "" {
		worker.systemLogger.Infof("Triggering background music generation for Workflow %s", event.Header.WorkflowID)
		go worker.generateAndStoreMusic(context.Background(), event.Header.WorkflowID, event.Settings.AudioSessionConfig.MusicPrompt)
	}

	// 1. Get Text
	cleanText, err := worker.retrieveAndCleanText(ctx, event.TextKey)
	if err != nil {
		return err
	}

	// 2. Create TTS Configuration from event
	var ttsConfiguration core.TTSConfig
	if event.Settings != nil && event.Settings.AudioSessionConfig != nil {
		ttsConfiguration = core.TTSConfig{
			SessionID:   event.Settings.AudioSessionConfig.SessionID,
			VoiceID:     event.Settings.AudioSessionConfig.VoiceID,
			MusicPrompt: event.Settings.AudioSessionConfig.MusicPrompt,
		}
	} else {
		// This case should ideally not happen if events are constructed correctly upstream.
		worker.systemLogger.Warnf("AudioSessionConfig not found in event for workflow %s. Proceeding with empty TTS config.", event.Header.WorkflowID)
		// Let the orchestrator handle the zero-valued config.
		ttsConfiguration = core.TTSConfig{}
	}

	// 3. Generate Audio
	var audioData []byte
	if strings.Contains(string(cleanText), NoSpeechMarker) {
		worker.systemLogger.Infof("Skipping TTS for Page %d (Marked as %s). Using 1s silence.", event.PageNumber, NoSpeechMarker)
		audioData = generateSilentWav(1*time.Second, AudioSampleRateHz, 1, 32)
	} else {
		var err error
		audioData, err = worker.ttsProcessor.Process(ctx, cleanText, ttsConfiguration)
		if err != nil {
			return fmt.Errorf("TTS generation failed: %w", err)
		}
	}

	// 4. Store Audio Chunk (Now a WAV file, not raw PCM)
	audioChunkKey := fmt.Sprintf(AudioChunkKeyFormat, event.Header.WorkflowID, event.PageNumber)
	if !strings.HasSuffix(audioChunkKey, ".wav") {
		audioChunkKey = strings.Replace(audioChunkKey, ".pcm", ".wav", 1)
	}

	if err := worker.audioObjectStore.Upload(ctx, audioChunkKey, audioData); err != nil {
		return fmt.Errorf("audio upload failed: %w", err)
	}

	// 5. Update Progress
	if err := worker.updateProgress(ctx, event.Header.WorkflowID, event.PageNumber); err != nil {
		return err
	}

	// 6. Check Completeness & Aggregate
	isComplete, err := worker.checkCompleteness(ctx, event.Header.WorkflowID, event.TotalPages)
	if err != nil {
		return err
	}

	if isComplete {
		worker.systemLogger.Infof("All pages complete for %s. Aggregating audio.", event.Header.WorkflowID)
		musicPrompt := ""
		if event.Settings != nil && event.Settings.AudioSessionConfig != nil {
			musicPrompt = event.Settings.AudioSessionConfig.MusicPrompt
		}
		return worker.aggregateAndFinalizeWorkflow(ctx, event.Header.WorkflowID, event.TotalPages, event.Header, musicPrompt)
	}

	return nil
}

func (worker *NatsWorker) generateAndStoreMusic(ctx context.Context, workflowID, prompt string) {
	// Detach from original context to avoid cancellation if the page finishes early,
	// but respect a reasonable timeout for music generation.
	genCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	musicData, err := worker.ttsProcessor.GenerateMusic(genCtx, prompt, MusicGenerationDuration)
	if err != nil {
		worker.systemLogger.Errorf("Background music generation failed for %s: %v", workflowID, err)
		return
	}

	key := fmt.Sprintf(MusicKeyFormat, workflowID)
	if err := worker.audioObjectStore.Upload(genCtx, key, musicData); err != nil {
		worker.systemLogger.Errorf("Failed to upload background music for %s: %v", workflowID, err)
		return
	}
	worker.systemLogger.Infof("Background music stored for %s", workflowID)
}

func (worker *NatsWorker) retrieveAndCleanText(ctx context.Context, textKey string) ([]byte, error) {
	textContent, err := worker.textObjectStore.Download(ctx, textKey)
	if err != nil {
		return nil, fmt.Errorf("download text failed: %w", err)
	}

	var textSegments []string
	if jsonErr := json.Unmarshal(textContent, &textSegments); jsonErr == nil {
		joinedText := ""
		for i, segment := range textSegments {
			if i > 0 {
				joinedText += "\n\n"
			}
			joinedText += segment
		}
		return []byte(joinedText), nil
	}

	return textContent, nil
}

func (worker *NatsWorker) updateProgress(ctx context.Context, workflowID string, pageNumber int) error {
	progressKey := fmt.Sprintf(KeyValueKeyFormat, workflowID, pageNumber)
	if _, err := worker.progressKeyValueStore.Put(ctx, progressKey, []byte(ProgressCompletedValue)); err != nil {
		return fmt.Errorf("KV put failed for %s: %w", progressKey, err)
	}
	return nil
}

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

func (worker *NatsWorker) aggregateAndFinalizeWorkflow(
	ctx context.Context,
	workflowID string,
	totalPages int,
	header events.EventHeader,
	musicPrompt string,
) error {
	tmpDir, err := os.MkdirTemp("", "tts-aggregate")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// 1. Download all chunks
	var chunkPaths []string
	for pageIndex := 1; pageIndex <= totalPages; pageIndex++ {
		chunkKey := fmt.Sprintf(AudioChunkKeyFormat, workflowID, pageIndex)
		if !strings.HasSuffix(chunkKey, ".wav") {
			chunkKey = strings.Replace(chunkKey, ".pcm", ".wav", 1)
		}

		chunkData, err := worker.audioObjectStore.Download(ctx, chunkKey)
		if err != nil {
			return fmt.Errorf("download chunk %s failed: %w", chunkKey, err)
		}

		chunkPath := filepath.Join(tmpDir, fmt.Sprintf("page_%d.wav", pageIndex))
		if err := os.WriteFile(chunkPath, chunkData, 0644); err != nil {
			return err
		}
		chunkPaths = append(chunkPaths, chunkPath)
	}

	// 2. Concatenate (via Processor -> Mixer -> FFmpeg)
	combinedWavData, err := worker.ttsProcessor.CombineAudio(ctx, chunkPaths)
	if err != nil {
		return fmt.Errorf("wav concatenation failed: %w", err)
	}

	// 3. Finalize (Remote Mix)
	// Try to find pre-generated music
	var finalWavData []byte
	var musicData []byte

	if musicPrompt != "" {
		musicKey := fmt.Sprintf(MusicKeyFormat, workflowID)
		musicData, err = worker.audioObjectStore.Download(ctx, musicKey)
		
		if err == nil && len(musicData) > 0 {
			worker.systemLogger.Infof("Found pre-generated music for %s. Mixing...", workflowID)
			mixedData, err := worker.ttsProcessor.MixAudio(ctx, combinedWavData, musicData)
			if err != nil {
				worker.systemLogger.Errorf("Mixing failed: %v. Fallback to speech only.", err)
				finalWavData = combinedWavData
			} else {
				finalWavData = mixedData
			}
		} else {
			worker.systemLogger.Warnf("Pre-generated music not found for %s (err: %v). Fallback to standard finalization.", workflowID, err)
			mixedData, err := worker.ttsProcessor.FinalizeAudio(ctx, combinedWavData, musicPrompt)
			if err != nil {
				worker.systemLogger.Errorf("Remote finalization failed: %v. Using speech only.", err)
				finalWavData = combinedWavData
			} else {
				finalWavData = mixedData
			}
		}
	} else {
		finalWavData = combinedWavData
	}

	finalKey := fmt.Sprintf(FinalAudioKeyFormat, workflowID)

	if err := worker.audioObjectStore.Upload(ctx, finalKey, finalWavData); err != nil {
		return fmt.Errorf("upload final WAV failed: %w", err)
	}

	completionEvent := &events.AudioChunkCreatedEvent{
		Header:     header,
		AudioKey:   finalKey,
		PageNumber: 0,
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
