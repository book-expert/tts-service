// main package for the tts-service
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/book-expert/configurator"
	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/config"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/objectstore"
	"github.com/book-expert/tts-service/internal/tts"
	"github.com/book-expert/tts-service/internal/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	// ErrNoStreamsConfigured indicates that no streams were defined in ServiceNATS config.
	ErrNoStreamsConfigured = errors.New("no streams configured for service")
	// ErrNoConsumersConfigured indicates that no consumers were defined in ServiceNATS config.
	ErrNoConsumersConfigured = errors.New("no consumers configured for service")
	// ErrNoObjectStoresConfigured indicates that no object stores were defined in ServiceNATS config.
	ErrNoObjectStoresConfigured = errors.New("no object stores configured for service")
	// ErrDeadLetterSubjectEmpty indicates the DLQ subject is missing.
	ErrDeadLetterSubjectEmpty = errors.New("dead letter subject must be configured")
	// ErrInvalidConsumerConfig indicates the durable consumer entry is malformed.
	ErrInvalidConsumerConfig = errors.New(
		"invalid consumer configuration: stream, consumer, and filter subject must be set",
	)
	// ErrConsumerStreamNotFound indicates the referenced consumer stream is missing.
	ErrConsumerStreamNotFound = errors.New("consumer stream not found in streams")
	// ErrConsumerFilterMismatch indicates the consumer filter subject is not part of its stream.
	ErrConsumerFilterMismatch = errors.New("consumer filter subject not present in referenced stream subjects")
	// ErrPublishSubjectDerive indicates we could not derive a publish subject from configuration.
	ErrPublishSubjectDerive = errors.New("publish subject could not be derived from streams configuration")
	// ErrPublishSubjectUnknown indicates the derived publish subject is not present in any stream.
	ErrPublishSubjectUnknown = errors.New("publish subject not found in any configured stream subjects")
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
	natsConnection, err := nats.Connect(cfg.ServiceConfig.NATS.NATS.URL,
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
	streamName, subject := derivePublishStreamAndSubject(&cfg.ServiceConfig.NATS)

	streamErr := ensureStreamForSubject(
		ctx,
		func(c context.Context, sCfg jetstream.StreamConfig) error {
			_, createErr := jetstreamContext.CreateStream(c, sCfg)
			if createErr != nil {
				return fmt.Errorf("create stream: %w", createErr)
			}

			return nil
		},
		streamName,
		subject,
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
	bucket := deriveFirstObjectStoreBucket(&cfg.ServiceConfig.NATS)

	store, storeErr := objectstore.New(ctx, jetstreamContext, bucket)
	if storeErr != nil {
		return nil, nil, fmt.Errorf("failed to create object store: %w", storeErr)
	}

	processor, procErr := createTTSProcessor(cfg, log)
	if procErr != nil {
		return nil, nil, procErr
	}

	return store, processor, nil
}

func deriveFirstObjectStoreBucket(nc *configurator.ServiceNATSConfig) string {
	if len(nc.ObjectStores) > 0 {
		return nc.ObjectStores[0].BucketName
	}

	return ""
}

func derivePublishStreamAndSubject(nc *configurator.ServiceNATSConfig) (string, string) {
	if len(nc.Streams) > 0 {
		stream := nc.Streams[0]
		if len(stream.Subjects) > 0 {
			return stream.Name, stream.Subjects[0]
		}

		return stream.Name, ""
	}

	return "", ""
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
	consumerStream, consumerSubject, consumerName := deriveConsumerBinding(&cfg.ServiceConfig.NATS)
	publishStream, publishSubject := derivePublishStreamAndSubject(&cfg.ServiceConfig.NATS)

	_ = publishStream // stream name is used for ensuring stream only

	natsWorker, workerErr := worker.NewNatsWorker(
		natsConnection,
		jetstreamContext,
		jetstreamContext,
		consumerStream,
		consumerSubject,
		consumerName,
		publishSubject,
		cfg.TTS.DeadLetterSubject,
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

	_, consumerSubject, _ = deriveConsumerBinding(&cfg.ServiceConfig.NATS)
	log.System("TTS-Service successfully initialized. Listening for jobs on subject: %s", consumerSubject)

	return workerCancel, nil
}

func deriveConsumerBinding(nc *configurator.ServiceNATSConfig) (string, string, string) {
	if len(nc.Consumers) > 0 {
		c := nc.Consumers[0]

		return c.StreamName, c.FilterSubject, c.ConsumerName
	}

	return "", "", ""
}

func startWorker(ctx context.Context, cfg *config.Config, log *logger.Logger) (context.CancelFunc, error) {
	validateErr := validateServiceNATSConfig(cfg)
	if validateErr != nil {
		return nil, validateErr
	}

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

// validateServiceNATSConfig performs fast-fail validation of the tts-service NATS configuration.
// It ensures the presence and cross-consistency of streams, consumer, object stores, and DLQ subject.
func validateServiceNATSConfig(cfg *config.Config) error {
	basicErr := validateDLQAndBasics(cfg)
	if basicErr != nil {
		return basicErr
	}

	consumerErr := validateConsumerCrossReference(&cfg.ServiceConfig.NATS)
	if consumerErr != nil {
		return consumerErr
	}

	publishErr := validatePublishSubjectExists(&cfg.ServiceConfig.NATS)
	if publishErr != nil {
		return publishErr
	}

	return nil
}

func validateDLQAndBasics(cfg *config.Config) error {
	if cfg.TTS.DeadLetterSubject == "" {
		return ErrDeadLetterSubjectEmpty
	}

	serviceNATS := &cfg.ServiceConfig.NATS
	if len(serviceNATS.Streams) == 0 {
		return ErrNoStreamsConfigured
	}

	if len(serviceNATS.Consumers) == 0 {
		return ErrNoConsumersConfigured
	}

	if len(serviceNATS.ObjectStores) == 0 || serviceNATS.ObjectStores[0].BucketName == "" {
		return ErrNoObjectStoresConfigured
	}

	return nil
}

func validateConsumerCrossReference(serviceNATS *configurator.ServiceNATSConfig) error {
	consumer := serviceNATS.Consumers[0]
	if consumer.StreamName == "" || consumer.ConsumerName == "" || consumer.FilterSubject == "" {
		return ErrInvalidConsumerConfig
	}

	var matchedStream *configurator.StreamConfig

	for i := range serviceNATS.Streams {
		if serviceNATS.Streams[i].Name == consumer.StreamName {
			matchedStream = &serviceNATS.Streams[i]

			break
		}
	}

	if matchedStream == nil {
		return ErrConsumerStreamNotFound
	}

	if !subjectInList(consumer.FilterSubject, matchedStream.Subjects) {
		return ErrConsumerFilterMismatch
	}

	return nil
}

func validatePublishSubjectExists(serviceNATS *configurator.ServiceNATSConfig) error {
	_, publishSubject := derivePublishStreamAndSubject(serviceNATS)
	if publishSubject == "" {
		return ErrPublishSubjectDerive
	}

	for _, s := range serviceNATS.Streams {
		if subjectInList(publishSubject, s.Subjects) {
			return nil
		}
	}

	return ErrPublishSubjectUnknown
}

func subjectInList(needle string, haystack []string) bool {
	for _, candidate := range haystack {
		if candidate == needle {
			return true
		}
	}

	return false
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
