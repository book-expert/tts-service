/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/book-expert/common-events"
	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/config"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/objectstore"
	"github.com/book-expert/tts-service/internal/processor"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	NatsConnectionTimeout = 30 * time.Second
	LogFileName           = "tts-service.log"
)

type Application struct {
	configuration    *config.Config
	serviceLogger    *logger.Logger
	natsConnection   *nats.Conn
	jetStreamContext jetstream.JetStream
	processor        *processor.Processor
}

func main() {
	systemContext, cancelSystemContext := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelSystemContext()

	serviceConfiguration, configurationLoadError := config.Load("")
	if configurationLoadError != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", configurationLoadError)
		os.Exit(1)
	}

	serviceLogger, loggerInitializationError := logger.New(serviceConfiguration.Service.LogDirectory, LogFileName)
	if loggerInitializationError != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", loggerInitializationError)
		os.Exit(1)
	}

	serviceApplication, applicationInitializationError := newApplication(systemContext, serviceConfiguration, serviceLogger)
	if applicationInitializationError != nil {
		serviceLogger.Errorf("Failed to initialize application: %v", applicationInitializationError)
		os.Exit(1)
	}
	defer serviceApplication.cleanup()

	if executionError := serviceApplication.processor.Start(systemContext); executionError != nil {
		serviceLogger.Errorf("Service execution failed: %v", executionError)
		os.Exit(1)
	}
}

func newApplication(systemContext context.Context, serviceConfiguration *config.Config, serviceLogger *logger.Logger) (*Application, error) {
	natsConnection, jetStreamContext, natsConnectionError := setupNatsConnection(serviceConfiguration)
	if natsConnectionError != nil {
		return nil, natsConnectionError
	}

	textObjectStore, textToSpeechObjectStore, objectStoreBindingError := setupObjectStores(systemContext, jetStreamContext)
	if objectStoreBindingError != nil {
		natsConnection.Close()
		return nil, objectStoreBindingError
	}

	speechClient := audio.NewSpeechClient(serviceConfiguration.TTS.AudioServerURL, serviceLogger)
	audioStitcher := audio.NewStitcher(serviceLogger)
	textToSpeechProcessor := audio.NewProcessor(
		speechClient,
		audioStitcher,
		serviceLogger,
		serviceConfiguration.TTS.SpeechConcurrency,
	)

	processorInstance, processorInitializationError := processor.NewProcessor(
		natsConnection,
		jetStreamContext,
		jetStreamContext,
		events.StreamTextFiles,
		events.SubjectTextCreated,
		serviceConfiguration.NATS.Consumer.DurableName,
		events.SubjectTextToSpeechCompleted,
		events.SubjectTextToSpeechStarted,
		events.SubjectMusicCreated,
		serviceConfiguration.NATS.DeadLetterQueueSubject,
		textObjectStore,
		textToSpeechObjectStore,
		textToSpeechProcessor,
		serviceLogger,
		serviceConfiguration.Service.WorkerCount,
	)
	if processorInitializationError != nil {
		natsConnection.Close()
		return nil, processorInitializationError
	}

	return &Application{
		configuration:    serviceConfiguration,
		serviceLogger:    serviceLogger,
		natsConnection:   natsConnection,
		jetStreamContext: jetStreamContext,
		processor:        processorInstance,
	}, nil
}

func (application *Application) cleanup() {
	if application.natsConnection != nil {
		application.natsConnection.Close()
	}
	if application.serviceLogger != nil {
		_ = application.serviceLogger.Close()
	}
}

func setupNatsConnection(configuration *config.Config) (*nats.Conn, jetstream.JetStream, error) {
	natsConnection, connectionError := nats.Connect(configuration.NATS.URL, nats.Timeout(NatsConnectionTimeout))
	if connectionError != nil {
		return nil, nil, connectionError
	}

	jetStreamContext, jetStreamError := jetstream.New(natsConnection)
	if jetStreamError != nil {
		natsConnection.Close()
		return nil, nil, jetStreamError
	}

	return natsConnection, jetStreamContext, nil
}

func setupObjectStores(systemContext context.Context, jetStreamContext jetstream.JetStream) (core.ObjectStore, core.ObjectStore, error) {
	textObjectStore, textStoreError := objectstore.New(systemContext, jetStreamContext, events.BucketTextFiles)
	if textStoreError != nil {
		return nil, nil, textStoreError
	}

	textToSpeechObjectStore, textToSpeechObjectStoreError := objectstore.New(systemContext, jetStreamContext, events.BucketTextToSpeech)
	if textToSpeechObjectStoreError != nil {
		return nil, nil, textToSpeechObjectStoreError
	}

	return textObjectStore, textToSpeechObjectStore, nil
}
