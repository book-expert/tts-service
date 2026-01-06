/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS, RELIABILITY, CONSISTENCY
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
6.  RELIABILITY: We act as guardians of the work. We value safety over speed.
    We do not break what works to fix what isn't broken.
7.  CONSISTENCY: We uphold standards even when no one is watching.
    Predictability is the soil in which trust grows.

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
-   Reliability = Safety: we protect the user's investment; stability is paramount.
-   Consistency = Integrity: uniform quality reduces cognitive load and fear.
--------------------------------------------------------------------------------
*/

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
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
	userDatabaseBaseURL       string
	httpClient                *http.Client
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
	userDatabaseBaseURL string,
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
		userDatabaseBaseURL:       userDatabaseBaseURL,
		httpClient:                &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// Run executes the main worker loop.
func (worker *Worker) Run(context context.Context) error {
	stream, err := worker.jetStream.Stream(context, worker.subscriptionStream)
	if err != nil {
		return fmt.Errorf("failed to retrieve stream %s: %w", worker.subscriptionStream, err)
	}

	consumerConfig := jetstream.ConsumerConfig{
		Durable:       worker.consumerDurableName,
		FilterSubject: worker.subscriptionSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	consumer, err := stream.CreateOrUpdateConsumer(context, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create or update consumer %s: %w", worker.consumerDurableName, err)
	}

	worker.logger.Infof("Worker running on subject %s with %d concurrent workers.", worker.subscriptionSubject, worker.workerCount)

	var waitGroup sync.WaitGroup

	for i := 0; i < worker.workerCount; i++ {
		waitGroup.Add(1)
		go func(workerID int) {
			defer waitGroup.Done()
			worker.consumeLoop(context, consumer, workerID)
		}(i)
	}

	waitGroup.Wait()
	return nil
}

func (worker *Worker) consumeLoop(context context.Context, consumer jetstream.Consumer, workerID int) {
	for {
		if context.Err() != nil {
			return
		}

		messageBatch, fetchError := consumer.Fetch(1, jetstream.FetchMaxWait(NatsFetchMaxWaitDuration))
		if fetchError != nil {
			if !errors.Is(fetchError, nats.ErrTimeout) {
				worker.logger.Errorf("[Worker %d] Failed to fetch message batch: %v", workerID, fetchError)
				// Prevent tight loop on persistent errors (e.g. stream not found)
				time.Sleep(1 * time.Second)
			}
			continue
		}

		for message := range messageBatch.Messages() {
			if context.Err() != nil {
				return
			}
			worker.processMessage(context, message)
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

	if err := message.InProgress(); err != nil {
		worker.logger.Warnf("Failed to signal InProgress: %v", err)
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
				if err := message.InProgress(); err != nil {
					worker.logger.Warnf("Failed to send keep-alive signal: %v", err)
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (worker *Worker) handleProcessingFailure(context context.Context, message jetstream.Msg, payload []byte) {
	if worker.deadLetterQueueSubject == "" {
		_ = message.Nak()
		return
	}

	for attempt := 1; attempt <= DeadLetterQueuePublishMaxRetries; attempt++ {
		if _, err := worker.jetStreamPublisher.Publish(context, worker.deadLetterQueueSubject, payload); err == nil {
			_ = message.Ack()
			return
		}
		time.Sleep(DeadLetterQueueBackoffDuration)
	}

	_ = message.NakWithDelay(DeadLetterQueueBackoffDuration)
}

func (worker *Worker) executeTTSJob(context context.Context, event *events.TextProcessedEvent) error {
	// 0a. Publish TTS Started
	if err := worker.publishTTSStarted(context, event); err != nil {
		worker.logger.Warnf("Failed to publish TTS started event: %v", err)
	}

	// 0b. Request Background Music Generation (Page 1 Only)
	if event.PageNumber == 1 && event.Settings != nil && event.Settings.AudioSessionConfig != nil && event.Settings.AudioSessionConfig.MusicPrompt != "" {
		worker.logger.Infof("Requesting background music generation for Workflow %s", event.Header.WorkflowID)
		// We default to 180s (3 mins) for now, as per standard
		if err := worker.publishMusicRequest(context, event.Header, event.Settings.AudioSessionConfig.MusicPrompt, 180); err != nil {
			worker.logger.Errorf("Failed to publish music request: %v", err)
		}
	}

	// 1. Get Text
	cleanText, err := worker.retrieveAndCleanText(context, event.TextKey)
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
		worker.logger.Warnf("AudioSessionConfig not found in event for workflow %s. Proceeding with empty TTS config.", event.Header.WorkflowID)
		// Let the orchestrator handle the zero-valued config.
		ttsConfiguration = core.TTSConfig{}
	}

	// 3. Generate Audio
	var audioData []byte
	if strings.Contains(string(cleanText), NoSpeechMarker) {
		worker.logger.Infof("Skipping TTS for Page %d (Marked as %s). Using 1s silence.", event.PageNumber, NoSpeechMarker)
		audioData = audio.GenerateSilentWav(1*time.Second, AudioSampleRateHz, 1, 32)
	} else {
		var err error
		audioData, err = worker.ttsProcessor.Process(context, cleanText, ttsConfiguration)
		if err != nil {
			return fmt.Errorf("TTS generation failed: %w", err)
		}
	}

	// 4. Store Audio Chunk (Now a WAV file, not raw PCM)
	audioChunkKey := fmt.Sprintf(AudioChunkKeyFormat, event.Header.WorkflowID, event.PageNumber)
	if !strings.HasSuffix(audioChunkKey, ".wav") {
		audioChunkKey = strings.Replace(audioChunkKey, ".pcm", ".wav", 1)
	}

	if err := worker.audioObjectStore.Upload(context, audioChunkKey, audioData); err != nil {
		return fmt.Errorf("audio upload failed: %w", err)
	}

	// 5. Update Progress
	if err := worker.updateProgress(context, event.Header.WorkflowID, event.PageNumber); err != nil {
		return err
	}

	// 6. Check Completeness & Aggregate
	isComplete, err := worker.checkCompleteness(context, event.Header.WorkflowID, event.TotalPages)
	if err != nil {
		return err
	}

	if isComplete {
		worker.logger.Infof("All pages complete for %s. Aggregating audio.", event.Header.WorkflowID)
		musicPrompt := ""
		if event.Settings != nil && event.Settings.AudioSessionConfig != nil {
			musicPrompt = event.Settings.AudioSessionConfig.MusicPrompt
		}
		return worker.aggregateAndFinalizeWorkflow(context, event.Header.WorkflowID, event.TotalPages, event.Header, musicPrompt)
	}

	return nil
}

func (worker *Worker) retrieveAndCleanText(context context.Context, textKey string) ([]byte, error) {
	textContent, err := worker.textObjectStore.Download(context, textKey)
	if err != nil {
		return nil, fmt.Errorf("download text failed: %w", err)
	}

	var textSegments []string
	if jsonError := json.Unmarshal(textContent, &textSegments); jsonError == nil {
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

func (worker *Worker) updateProgress(context context.Context, workflowID string, pageNumber int) error {
	progressKey := fmt.Sprintf(KeyValueKeyFormat, workflowID, pageNumber)
	if _, err := worker.progressKeyValueStore.Put(context, progressKey, []byte(ProgressCompletedValue)); err != nil {
		return fmt.Errorf("KV put failed for %s: %w", progressKey, err)
	}
	return nil
}

func (worker *Worker) checkCompleteness(context context.Context, workflowID string, totalPages int) (bool, error) {
	keyFilter := fmt.Sprintf(KeyValueFilterPatternFormat, workflowID)
	keyLister, err := worker.progressKeyValueStore.ListKeysFiltered(context, keyFilter)
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

func (worker *Worker) aggregateAndFinalizeWorkflow(
	context context.Context,
	workflowID string,
	totalPages int,
	header events.EventHeader,
	musicPrompt string,
) error {
	// 0. Publish Aggregation Started
	if err := worker.publishAggregationStarted(context, header, totalPages); err != nil {
		worker.logger.Warnf("Failed to publish aggregation started event: %v", err)
	}

	temporaryDirectory, err := os.MkdirTemp("", "tts-aggregate")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()

	// 1. Download all chunks
	var chunkPaths []string
	for pageIndex := 1; pageIndex <= totalPages; pageIndex++ {
		chunkKey := fmt.Sprintf(AudioChunkKeyFormat, workflowID, pageIndex)
		if !strings.HasSuffix(chunkKey, ".wav") {
			chunkKey = strings.Replace(chunkKey, ".pcm", ".wav", 1)
		}

		chunkData, err := worker.audioObjectStore.Download(context, chunkKey)
		if err != nil {
			return fmt.Errorf("download chunk %s failed: %w", chunkKey, err)
		}

		chunkPath := filepath.Join(temporaryDirectory, fmt.Sprintf("page_%d.wav", pageIndex))
		if err := os.WriteFile(chunkPath, chunkData, 0644); err != nil {
			return err
		}
		chunkPaths = append(chunkPaths, chunkPath)
	}

	// 2. Concatenate (via Processor -> Mixer -> FFmpeg)
	combinedWavData, err := worker.ttsProcessor.CombineAudio(context, chunkPaths)
	if err != nil {
		return fmt.Errorf("wav concatenation failed: %w", err)
	}

	// 3. Finalize (Remote Mix)
	// Try to find pre-generated music
	var finalWavData []byte
	var musicData []byte

	if musicPrompt != "" {
		musicKey := fmt.Sprintf(MusicKeyFormat, workflowID)
		musicData, err = worker.audioObjectStore.Download(context, musicKey)
		
		if err == nil && len(musicData) > 0 {
			worker.logger.Infof("Found pre-generated music for %s. Mixing...", workflowID)
			mixedData, err := worker.ttsProcessor.MixAudio(context, combinedWavData, musicData)
			if err != nil {
				worker.logger.Errorf("Mixing failed: %v. Fallback to speech only.", err)
				finalWavData = combinedWavData
			} else {
				finalWavData = mixedData
			}
		} else {
			worker.logger.Warnf("Pre-generated music not found for %s (err: %v). Fallback to standard finalization.", workflowID, err)
			mixedData, err := worker.ttsProcessor.FinalizeAudio(context, combinedWavData, musicPrompt)
			if err != nil {
				worker.logger.Errorf("Remote finalization failed: %v. Using speech only.", err)
				finalWavData = combinedWavData
			} else {
				finalWavData = mixedData
			}
		}
	} else {
		finalWavData = combinedWavData
	}

	finalKey := fmt.Sprintf(FinalAudioKeyFormat, workflowID)

    // Upload to user-database-service (Artifacts Storage)
    if worker.userDatabaseBaseURL != "" {
        uploadURL := fmt.Sprintf("%s/v1/artifacts/%s", worker.userDatabaseBaseURL, workflowID)
        request, err := http.NewRequestWithContext(context, http.MethodPost, uploadURL, bytes.NewReader(finalWavData))
        if err != nil {
             // Log error but try to continue to NATS fallback or just log
             worker.logger.Errorf("Failed to create upload request to user-db: %v", err)
        } else {
            request.Header.Set("Content-Type", "audio/wav")
            response, err := worker.httpClient.Do(request)
            if err != nil {
                 worker.logger.Errorf("Failed to upload artifact to user-db: %v", err)
            } else {
                 defer func() {
                     _ = response.Body.Close()
                 }()
                 if response.StatusCode != http.StatusCreated {
                      worker.logger.Errorf("User-db upload failed with status: %d", response.StatusCode)
                 } else {
                      worker.logger.Infof("Successfully moved artifact %s to user-database-service", workflowID)
                      
                      completionEvent := &events.AudioChunkCreatedEvent{
                           Header:     header,
                           AudioKey:   workflowID, // We use ID as key
                           PageNumber: 0,
                           TotalPages: totalPages,
                      }
                      
                      return worker.publishCompletionEvent(context, completionEvent)
                 }
            }
        }
    }

    // Fallback or Standard: Upload to NATS if UserDB upload failed or not configured
	if err := worker.audioObjectStore.Upload(context, finalKey, finalWavData); err != nil {
		return fmt.Errorf("upload final WAV failed: %w", err)
	}

	completionEvent := &events.AudioChunkCreatedEvent{
		Header:     header,
		AudioKey:   finalKey,
		PageNumber: 0,
		TotalPages: totalPages,
	}

	if err := worker.publishCompletionEvent(context, completionEvent); err != nil {
		return err
	}

	return nil
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

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = worker.jetStreamPublisher.Publish(context, worker.ttsStartedSubject, data)
	return err
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

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = worker.jetStreamPublisher.Publish(context, worker.musicRequestSubject, data)
	return err
}

func (worker *Worker) publishAggregationStarted(context context.Context, header events.EventHeader, totalPages int) error {
	if worker.aggregationStartedSubject == "" {
		return nil
	}

	event := events.AggregationStartedEvent{
		Header:     header,
		TotalPages: totalPages,
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = worker.jetStreamPublisher.Publish(context, worker.aggregationStartedSubject, data)
	return err
}

func (worker *Worker) publishCompletionEvent(context context.Context, event *events.AudioChunkCreatedEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event failed: %w", err)
	}
	if _, err := worker.jetStreamPublisher.Publish(context, worker.producerSubject, data); err != nil {
		return fmt.Errorf("publish event failed: %w", err)
	}
	return nil
}

func (worker *Worker) parseAndValidateEvent(message jetstream.Msg) (*events.TextProcessedEvent, error) {
	var event events.TextProcessedEvent
	if err := json.Unmarshal(message.Data(), &event); err != nil {
		return nil, err
	}
	return &event, nil
}