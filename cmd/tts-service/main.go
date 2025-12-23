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

// Package main serves as the entry point for the TTS Service.
// It initializes configuration, logging, NATS connections, object stores, and the worker process.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/config"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/objectstore"
	"github.com/book-expert/tts-service/internal/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// NatsConnectionTimeout defines the maximum duration to wait for a NATS connection.
	NatsConnectionTimeout = 30 * time.Second

	// ConfigFileName defines the standard name of the configuration file.
	ConfigFileName = "project.toml"

	// LogFileName defines the name of the log file.
	LogFileName = "tts-service.log"
)

// Application holds the dependencies and state of the running service.
// Why: Centralizes state management and allows for clean dependency injection and cleanup.
type Application struct {
	configuration    *config.Config
	logger           *logger.Logger
	natsConnection   *nats.Conn
	jetStreamContext jetstream.JetStream
	workerInstance   *worker.NatsWorker
}

func main() {
	// Create a context that listens for system interruption signals.
	signalContext, cancelSignalContext := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignalContext()

	if runError := runService(signalContext); runError != nil {
		fmt.Fprintf(os.Stderr, "Service exited with fatal error: %v\n", runError)
		os.Exit(1)
	}
}

// runService orchestrates the startup, execution, and shutdown of the application.
func runService(serviceContext context.Context) error {
	// 1. Initialize Application
	serviceApplication, initializationError := newApplication()
	if initializationError != nil {
		return initializationError
	}
	defer serviceApplication.cleanup()

	serviceApplication.logger.Systemf("TTS Service initialization complete. Starting worker message loop...")

	// 2. Run Worker
	// This blocks until the context is canceled or a fatal error occurs.
	return serviceApplication.workerInstance.Run(serviceContext)
}

// newApplication initializes all service dependencies.
//
// Flow: Load Config -> Init Logger -> Connect NATS -> Bind Stores -> Init TTS -> Create Worker
func newApplication() (*Application, error) {
	// 1. Load Configuration
	serviceConfig, configLoadError := config.Load(ConfigFileName)
	if configLoadError != nil {
		return nil, fmt.Errorf("failed to load service configuration: %w", configLoadError)
	}

	// 2. Initialize Logger
	systemLogger, loggerInitError := logger.New(serviceConfig.Service.LogDirectory, LogFileName)
	if loggerInitError != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", loggerInitError)
	}
	systemLogger.Infof("Starting TTS Service. Configuration loaded. Worker count: %d", serviceConfig.Service.WorkerCount)

	// 3. Connect to NATS
	natsConnection, jetStreamContext, natsConnectError := setupNatsConnection(serviceConfig)
	if natsConnectError != nil {
		// Attempt to close logger before returning since defer in runService won't run yet
		_ = systemLogger.Close()
		return nil, fmt.Errorf("failed to establish NATS infrastructure connection: %w", natsConnectError)
	}

	// 4. Bind Object Stores
	textObjectStore, ttsObjectStore, objectStoreBindError := setupObjectStores(context.Background(), jetStreamContext, serviceConfig)
	if objectStoreBindError != nil {
		natsConnection.Close()
		_ = systemLogger.Close()
		return nil, fmt.Errorf("failed to bind to required Object Stores: %w", objectStoreBindError)
	}

	// 5. Initialize Key-Value Store
	progressKeyValueStore, kvStoreInitError := setupProgressStore(context.Background(), jetStreamContext, serviceConfig)
	if kvStoreInitError != nil {
		natsConnection.Close()
		_ = systemLogger.Close()
		return nil, fmt.Errorf("failed to initialize Progress Key-Value Store: %w", kvStoreInitError)
	}

	// 6. Initialize Audio Orchestrator (Dependencies)
	
	// 6a. Audio Client (Speech)
	// Rename to SpeechClient to match new interface
	speechClient := audio.NewSpeechClient(serviceConfig.TTS.AudioServerURL, systemLogger)

	// 6b. Music Client (Lyria)
	musicApiKey := os.Getenv(serviceConfig.TTS.APIKeyEnvironmentVariable)
	if musicApiKey == "" {
		systemLogger.Warnf("Music API Key (%s) not found in env. Music generation will fail.", serviceConfig.TTS.APIKeyEnvironmentVariable)
	}
	// Use consolidated audio package
	musicClient, musicErr := audio.NewMusicClient(context.Background(), musicApiKey, systemLogger)
	if musicErr != nil {
		systemLogger.Errorf("Failed to initialize Music Client: %v", musicErr)
		// We verify at runtime
	}

	// 6c. Mixer (FFmpeg)
	// Use consolidated audio package
	audioMixer := audio.NewMixer(systemLogger)

	// 6d. Processor (formerly Orchestrator)
	// Use consolidated audio package
	ttsProcessor := audio.NewProcessor(
		speechClient,
		musicClient,
		audioMixer,
		systemLogger,
		serviceConfig.TTS.SpeechConcurrency,
	)

	// 7. Create Worker
	natsWorker, workerInitError := worker.NewNatsWorker(
		natsConnection,
		jetStreamContext,
		jetStreamContext, // Passed twice as ConsumerContext and PublisherContext
		serviceConfig.NATS.Consumer.StreamName,
		serviceConfig.NATS.Consumer.SubjectFilter,
		serviceConfig.NATS.Consumer.DurableName,
		serviceConfig.NATS.Producer.SubjectName,
		serviceConfig.NATS.DeadLetterQueueSubject,
		textObjectStore,
		ttsObjectStore, // CORRECT VARIABLE
		progressKeyValueStore,
		ttsProcessor, // CORRECT VARIABLE
		systemLogger,
		1, // Forced to 1 to process one page at a time
        serviceConfig.Service.UserDatabaseURL,
	)
	if workerInitError != nil {
		natsConnection.Close()
		_ = systemLogger.Close()
		return nil, fmt.Errorf("failed to create NATS worker instance: %w", workerInitError)
	}

	return &Application{
		configuration:    serviceConfig,
		logger:           systemLogger,
		natsConnection:   natsConnection,
		jetStreamContext: jetStreamContext,
		workerInstance:   natsWorker,
	}, nil
}

// cleanup ensures resources are released properly on shutdown.
func (serviceApplication *Application) cleanup() {
	if serviceApplication.natsConnection != nil {
		serviceApplication.natsConnection.Close()
	}
	if serviceApplication.logger != nil {
		if closeError := serviceApplication.logger.Close(); closeError != nil {
			fmt.Fprintf(os.Stderr, "failed to close logger cleanly: %v\n", closeError)
		}
	}
}

// setupNatsConnection establishes the NATS connection and initializes the JetStream context.
func setupNatsConnection(configuration *config.Config) (*nats.Conn, jetstream.JetStream, error) {
	natsConnection, connectionError := nats.Connect(configuration.NATS.URL, nats.Timeout(NatsConnectionTimeout))
	if connectionError != nil {
		return nil, nil, fmt.Errorf("nats connect failed: %w", connectionError)
	}

	jetStreamContext, jetStreamError := jetstream.New(natsConnection)
	if jetStreamError != nil {
		natsConnection.Close()
		return nil, nil, fmt.Errorf("jetstream initialization failed: %w", jetStreamError)
	}

	return natsConnection, jetStreamContext, nil
}

// setupObjectStores binds to the necessary JetStream Object Stores.
func setupObjectStores(serviceContext context.Context, jetStreamContext jetstream.JetStream, configuration *config.Config) (core.ObjectStore, core.ObjectStore, error) {
	textStore, textStoreError := objectstore.New(serviceContext, jetStreamContext, configuration.NATS.ObjectStore.TextBucketName)
	if textStoreError != nil {
		return nil, nil, fmt.Errorf("failed to bind to Text Object Store (%s): %w", configuration.NATS.ObjectStore.TextBucketName, textStoreError)
	}

	ttsStore, ttsStoreError := objectstore.New(serviceContext, jetStreamContext, configuration.NATS.ObjectStore.TTSBucketName)
	if ttsStoreError != nil {
		return nil, nil, fmt.Errorf("failed to bind to TTS Object Store (%s): %w", configuration.NATS.ObjectStore.TTSBucketName, ttsStoreError)
	}

	return textStore, ttsStore, nil
}

// setupProgressStore initializes or retrieves the Key-Value bucket for tracking progress.
func setupProgressStore(serviceContext context.Context, jetStreamContext jetstream.JetStream, configuration *config.Config) (jetstream.KeyValue, error) {
	// Attempt to create the bucket. Use CreateKeyValue as it is generally idempotent.
	keyValueStore, createError := jetStreamContext.CreateKeyValue(serviceContext, jetstream.KeyValueConfig{
		Bucket: configuration.NATS.KeyValueStore.ProgressBucketName,
	})
	if createError != nil {
		// Fallback: Attempt to bind to an existing bucket if creation reported an error
		// (though standard NATS clients handle existing buckets gracefully in Create).
		var bindError error
		keyValueStore, bindError = jetStreamContext.KeyValue(serviceContext, configuration.NATS.KeyValueStore.ProgressBucketName)
		if bindError != nil {
			return nil, fmt.Errorf("failed to get or create KV bucket %s. Create error: %v, Bind error: %w", configuration.NATS.KeyValueStore.ProgressBucketName, createError, bindError)
		}
	}
	return keyValueStore, nil
}