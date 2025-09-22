// main package for the tts-service
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/config"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/objectstore"
	"github.com/book-expert/tts-service/internal/tts"
	"github.com/book-expert/tts-service/internal/worker"
	"github.com/nats-io/nats.go"
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

//nolint:ireturn
func setupNATS(cfg *config.Config) (*nats.Conn, jetstream, error) {
	natsConnection, err := nats.Connect(cfg.NATS.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	jetstreamContext, err := natsConnection.JetStream()
	if err != nil {
		natsConnection.Close()

		return nil, nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	return natsConnection, jetstreamContext, nil
}

func startWorker(ctx context.Context, cfg *config.Config, log *logger.Logger) (context.CancelFunc, error) {
	natsConnection, jetstreamContext, err := setupNATS(cfg)
	if err != nil {
		return nil, err
	}

	store, err := objectstore.New(jetstreamContext, cfg.NATS.AudioObjectStoreBucket)
	if err != nil {
		natsConnection.Close()

		return nil, fmt.Errorf("failed to create object store: %w", err)
	}

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
		natsConnection.Close()

		return nil, fmt.Errorf("failed to create TTS processor: %w", err)
	}

	natsWorker, err := worker.NewNatsWorker(
		natsConnection,
		jetstreamContext,
		cfg.NATS.TextProcessedSubject,
		cfg.NATS.AudioChunkCreatedSubject,
		store,
		processor,
		log,
	)
	if err != nil {
		natsConnection.Close()

		return nil, fmt.Errorf("failed to create NATS worker: %w", err)
	}

	workerCtx, workerCancel := context.WithCancel(ctx)

	go func() {
		defer natsConnection.Close()

		runErr := natsWorker.Run(workerCtx)
		if runErr != nil {
			log.Error("NATS worker stopped with error: %v", runErr)
			workerCancel() // Ensure other dependent goroutines are stopped
		}
	}()

	log.System("TTS-Service successfully initialized. Listening for jobs on subject: %s", cfg.NATS.TextProcessedSubject)

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
