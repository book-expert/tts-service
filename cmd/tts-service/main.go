// main package for the tts-service
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/config"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/objectstore"
	"github.com/book-expert/tts-service/internal/tts"
	"github.com/book-expert/tts-service/internal/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func setupLogger(logPath string) (*logger.Logger, error) {
	log, err := logger.New(logPath, "tts-service.log")
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return log, nil
}

func bootstrap() (*config.Config, *logger.Logger, error) {
	bootstrapLog, err := setupLogger(os.TempDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to create bootstrap logger: %v\n", err)

		return nil, nil, err
	}

	bootstrapLog.Info("Bootstrap logger created.")

	cfg, err := config.Load(bootstrapLog)
	if err != nil {
		bootstrapLog.Error("Failed to load configuration: %v", err)

		return nil, nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	bootstrapLog.Info("Configuration loaded successfully.")

	return cfg, bootstrapLog, nil
}

const (
	natsConnectTimeout = 30 * time.Second
	natsStreamTimeout  = 60 * time.Second
)

func setupNATS(cfg *config.Config) (*nats.Conn, error) {
	natsConnection, err := nats.Connect(cfg.NATS.URL,
		nats.Timeout(natsConnectTimeout),
		nats.RetryOnFailedConnect(true))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return natsConnection, nil
}

// streamAdmin defines the minimal capability needed to manage streams.
// Using a narrow interface here makes the function easy to unit test.
type streamCreator func(ctx context.Context, cfg jetstream.StreamConfig) error

func ensureStreamForSubject(ctx context.Context, create streamCreator, streamName, subject string) error {
	var streamCfg jetstream.StreamConfig

	streamCfg.Name = streamName
	streamCfg.Subjects = []string{subject}
	streamCfg.Retention = jetstream.WorkQueuePolicy
	streamCfg.MaxConsumers = -1
	streamCfg.MaxMsgs = -1
	streamCfg.MaxBytes = -1
	streamCfg.Discard = jetstream.DiscardOld
	streamCfg.Storage = jetstream.FileStorage
	streamCfg.Replicas = 1
	streamCfg.Compression = jetstream.NoCompression
	streamCfg.ConsumerLimits = jetstream.StreamConsumerLimits{InactiveThreshold: 0, MaxAckPending: 0}

	// Bound the server-side request latency for creating the stream.
	err := create(ctx, streamCfg)
	if err != nil {
		return fmt.Errorf("failed to add stream '%s' for subject '%s': %w", streamName, subject, err)
	}

	return nil
}

// streamNameForSubject removed; explicit stream names come from configuration.

func createTTSProcessor(cfg *config.Config, log *logger.Logger) (*tts.ChatLLMProcessor, error) {
	processor, err := tts.New(core.TTSConfig{
		ModelPath:         cfg.TTS.ModelPath,
		SnacModelPath:     cfg.TTS.SnacModelPath,
		Voice:             cfg.TTS.Voice,
		Seed:              cfg.TTS.Seed,
		NGL:               cfg.TTS.NGL,
		TopP:              cfg.TTS.TopP,
		RepetitionPenalty: cfg.TTS.RepetitionPenalty,
		Temperature:       cfg.TTS.Temperature,
		AllowedVoices:     cfg.TTS.AllowedVoices,
	}, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create TTS processor: %w", err)
	}

	return processor, nil
}

func verifyJetStreamAvailable(ctx context.Context, jetstreamContext jetstream.JetStream) error {
	_, jsInfoErr := jetstreamContext.AccountInfo(ctx)
	if jsInfoErr != nil {
		return fmt.Errorf("jetstream not available or unresponsive: %w", jsInfoErr)
	}

	return nil
}

func ensureAudioProcessingStream(ctx context.Context, jetstreamContext jetstream.JetStream, cfg *config.Config) error {
	streamErr := ensureStreamForSubject(
		ctx,
		func(c context.Context, cfg jetstream.StreamConfig) error {
			_, createErr := jetstreamContext.CreateStream(c, cfg)
			if createErr != nil {
				return fmt.Errorf("create stream: %w", createErr)
			}

			return nil
		},
		cfg.NATS.AudioProcessingStreamName,
		cfg.NATS.AudioChunkCreatedSubject,
	)
	if streamErr != nil {
		return fmt.Errorf("failed to ensure audio chunk stream: %w", streamErr)
	}

	return nil
}

func initStoresAndProcessor(
	ctx context.Context,
	jetstreamContext jetstream.JetStream,
	cfg *config.Config,
	log *logger.Logger,
) (*objectstore.NatsObjectStore, *tts.ChatLLMProcessor, error) {
	store, storeErr := objectstore.New(ctx, jetstreamContext, cfg.NATS.AudioObjectStoreBucket)
	if storeErr != nil {
		return nil, nil, fmt.Errorf("failed to create object store: %w", storeErr)
	}

	processor, procErr := createTTSProcessor(cfg, log)
	if procErr != nil {
		return nil, nil, procErr
	}

	return store, processor, nil
}

func launchWorker(
	ctx context.Context,
	natsConnection *nats.Conn,
	jetstreamContext jetstream.JetStream,
	cfg *config.Config,
	store core.ObjectStore,
	processor core.TTSProcessor,
	log *logger.Logger,
) (context.CancelFunc, error) {
	natsWorker, workerErr := worker.NewNatsWorker(
		natsConnection,
		jetstreamContext,
		jetstreamContext,
		cfg.NATS.TTSStreamName,
		cfg.NATS.TextProcessedSubject,
		cfg.NATS.TTSConsumerName,
		cfg.NATS.AudioChunkCreatedSubject,
		store,
		processor,
		log,
	)
	if workerErr != nil {
		return nil, fmt.Errorf("failed to create NATS worker: %w", workerErr)
	}

	workerCtx, workerCancel := context.WithCancel(ctx)

	go func() {
		defer natsConnection.Close()

		runErr := natsWorker.Run(workerCtx)
		if runErr != nil {
			log.Error("NATS worker stopped with error: %v", runErr)
			workerCancel()
		}
	}()

	log.System("TTS-Service successfully initialized. Listening for jobs on subject: %s", cfg.NATS.TextProcessedSubject)

	return workerCancel, nil
}

func startWorker(ctx context.Context, cfg *config.Config, log *logger.Logger) (context.CancelFunc, error) {
	natsConnection, err := setupNATS(cfg)
	if err != nil {
		return nil, err
	}

	jetstreamContext, jsErr := jetstream.New(natsConnection)
	if jsErr != nil {
		natsConnection.Close()

		return nil, fmt.Errorf("failed to get JetStream context: %w", jsErr)
	}

	verifyErr := verifyJetStreamAvailable(ctx, jetstreamContext)
	if verifyErr != nil {
		natsConnection.Close()

		return nil, verifyErr
	}

	ensureStreamErr := ensureAudioProcessingStream(ctx, jetstreamContext, cfg)
	if ensureStreamErr != nil {
		natsConnection.Close()

		return nil, ensureStreamErr
	}

	store, processor, initErr := initStoresAndProcessor(ctx, jetstreamContext, cfg, log)
	if initErr != nil {
		natsConnection.Close()

		return nil, initErr
	}

	workerCancel, launchErr := launchWorker(ctx, natsConnection, jetstreamContext, cfg, store, processor, log)
	if launchErr != nil {
		natsConnection.Close()

		return nil, launchErr
	}

	return workerCancel, nil
}

func waitForShutdownSignal(log *logger.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Info("Shutdown signal received, gracefully shutting down...")
}

func run() error {
	cfg, bootstrapLog, err := bootstrap()
	if err != nil {
		return err
	}

	log, err := setupLogger(os.TempDir())
	if err != nil {
		bootstrapLog.Error("Failed to create final logger: %v", err)

		return fmt.Errorf("failed to create final logger: %w", err)
	}

	defer func() {
		closeErr := log.Close()
		if closeErr != nil {
			fmt.Fprintf(os.Stderr, "error closing logger: %v\n", closeErr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerCancel, err := startWorker(ctx, cfg, log)
	if err != nil {
		log.Error("Failed to start worker: %v", err)

		return err
	}

	waitForShutdownSignal(log)
	workerCancel()

	log.Info("Shutdown complete.")

	return nil
}

func main() {
	err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Service exited with error: %v\n", err)
		os.Exit(1)
	}
}
