/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package main

import (
	"context"
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
	NatsConnectionTimeout = 30 * time.Second
	ConfigFileName        = "project.toml"
	LogFileName           = "tts-service.log"
)

type Application struct {
	configuration    *config.Config
	logger           *logger.Logger
	natsConnection   *nats.Conn
	jetStreamContext jetstream.JetStream
	workerInstance   *worker.Worker
}

func main() {
	systemContext, cancelSystemContext := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelSystemContext()

	serviceConfiguration, configurationError := config.Load(ConfigFileName)
	if configurationError != nil {
		os.Exit(1)
	}

	serviceLogger, loggerError := logger.New(serviceConfiguration.Service.LogDirectory, LogFileName)
	if loggerError != nil {
		os.Exit(1)
	}

	serviceApplication, applicationError := newApplication(systemContext, serviceConfiguration, serviceLogger)
	if applicationError != nil {
		serviceLogger.Errorf("Failed to initialize application: %v", applicationError)
		os.Exit(1)
	}
	defer serviceApplication.cleanup()

	if runError := serviceApplication.workerInstance.Run(systemContext); runError != nil {
		serviceLogger.Errorf("Service execution failed: %v", runError)
		os.Exit(1)
	}
}

func newApplication(systemContext context.Context, serviceConfiguration *config.Config, serviceLogger *logger.Logger) (*Application, error) {
	natsConnection, jetStreamContext, natsConnectError := setupNatsConnection(serviceConfiguration)
	if natsConnectError != nil {
		return nil, natsConnectError
	}

	textObjectStore, ttsObjectStore, objectStoreBindError := setupObjectStores(systemContext, jetStreamContext, serviceConfiguration)
	if objectStoreBindError != nil {
		natsConnection.Close()
		return nil, objectStoreBindError
	}

	progressKeyValueStore, kvStoreInitError := setupProgressStore(systemContext, jetStreamContext, serviceConfiguration)
	if kvStoreInitError != nil {
		natsConnection.Close()
		return nil, kvStoreInitError
	}

	speechClient := audio.NewSpeechClient(serviceConfiguration.TTS.AudioServerURL, serviceLogger)
	audioMixer := audio.NewMixer(serviceLogger)
	ttsProcessor := audio.NewProcessor(
		speechClient,
		audioMixer,
		serviceLogger,
		serviceConfiguration.TTS.SpeechConcurrency,
	)

	workerInstance, workerInitError := worker.New(
		natsConnection,
		jetStreamContext,
		jetStreamContext,
		serviceConfiguration.NATS.Consumer.StreamName,
		serviceConfiguration.NATS.Consumer.SubjectFilter,
		serviceConfiguration.NATS.Consumer.DurableName,
		serviceConfiguration.NATS.Producer.SubjectName,
		serviceConfiguration.NATS.Producer.TTSStartedSubject,
		serviceConfiguration.NATS.Producer.MusicStartedSubject,
		serviceConfiguration.NATS.Producer.MusicRequestSubject,
		serviceConfiguration.NATS.Producer.MusicCreatedSubject,
		serviceConfiguration.NATS.Producer.AggregationStartedSubject,
		serviceConfiguration.NATS.DeadLetterQueueSubject,
		textObjectStore,
		ttsObjectStore,
		progressKeyValueStore,
		ttsProcessor,
		serviceLogger,
		serviceConfiguration.Service.WorkerCount,
	)
	if workerInitError != nil {
		natsConnection.Close()
		return nil, workerInitError
	}

	return &Application{
		configuration:    serviceConfiguration,
		logger:           serviceLogger,
		natsConnection:   natsConnection,
		jetStreamContext: jetStreamContext,
		workerInstance:   workerInstance,
	}, nil
}

func (serviceApplication *Application) cleanup() {
	if serviceApplication.natsConnection != nil {
		serviceApplication.natsConnection.Close()
	}
	if serviceApplication.logger != nil {
		_ = serviceApplication.logger.Close()
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

func setupObjectStores(serviceContext context.Context, jetStreamContext jetstream.JetStream, configuration *config.Config) (core.ObjectStore, core.ObjectStore, error) {
	textStore, textStoreError := objectstore.New(serviceContext, jetStreamContext, configuration.NATS.ObjectStore.TextBucketName)
	if textStoreError != nil {
		return nil, nil, textStoreError
	}

	ttsStore, ttsObjectStoreError := objectstore.New(serviceContext, jetStreamContext, configuration.NATS.ObjectStore.TTSBucketName)
	if ttsObjectStoreError != nil {
		return nil, nil, ttsObjectStoreError
	}

	return textStore, ttsStore, nil
}

func setupProgressStore(serviceContext context.Context, jetStreamContext jetstream.JetStream, configuration *config.Config) (jetstream.KeyValue, error) {
	return jetStreamContext.KeyValue(serviceContext, configuration.NATS.KeyValueStore.ProgressBucketName)
}