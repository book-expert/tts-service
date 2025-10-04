// Package worker_test contains the unit tests for the worker package.
package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/book-expert/events"
	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/worker"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockJetStreamContext is a mock implementation of the jetstream.JetStream interface.
type MockJetStreamContext struct {
	mock.Mock
}

// Publish is a mock implementation of the Publish method.
func (m *MockJetStreamContext) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	args := m.Called(ctx, subject, data)

	var pubAck *jetstream.PubAck

	ackValue := args.Get(0)
	if ackValue != nil {
		if convertedAck, ok := ackValue.(*jetstream.PubAck); ok {
			pubAck = convertedAck
		}
	}

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return pubAck, fmt.Errorf("mock publish returned error: %w", returnedErr)
	}

	return pubAck, nil
}

// Stream is a mock implementation of the Stream method.
func (m *MockJetStreamContext) Stream(ctx context.Context, streamName string) (jetstream.Stream, error) {
	args := m.Called(ctx, streamName)
	stream, _ := args.Get(0).(jetstream.Stream)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return stream, fmt.Errorf("mock stream returned error for stream %s: %w", streamName, returnedErr)
	}

	return stream, nil
}

// AccountInfo is a mock implementation of the AccountInfo method.
func (m *MockJetStreamContext) AccountInfo(ctx context.Context) (*jetstream.AccountInfo, error) {
	args := m.Called(ctx)
	accountInfo, _ := args.Get(0).(*jetstream.AccountInfo)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return accountInfo, fmt.Errorf("mock account info returned error: %w", returnedErr)
	}

	return accountInfo, nil
}

// Additional methods for convenience; only Stream and Publish are required.
func (m *MockJetStreamContext) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	args := m.Called(ctx, cfg)
	stream, _ := args.Get(0).(jetstream.Stream)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return stream, fmt.Errorf("mock create stream returned error: %w", returnedErr)
	}

	return stream, nil
}

// CreateConsumer provides a stub to satisfy the jetstream.JetStream interface in tests.
func (m *MockJetStreamContext) CreateConsumer(_ context.Context, _ string, _ jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, nil
}

// Consumer methods.
func (m *MockJetStreamContext) Consumer(ctx context.Context, streamName, consumerName string) (jetstream.Consumer, error) {
	args := m.Called(ctx, streamName, consumerName)
	consumer, _ := args.Get(0).(jetstream.Consumer)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return consumer, fmt.Errorf("mock consumer returned error: %w", returnedErr)
	}

	return consumer, nil
}

func (m *MockJetStreamContext) GetMetaData(ctx context.Context) (*jetstream.StreamInfo, error) {
	args := m.Called(ctx)
	streamInfo, _ := args.Get(0).(*jetstream.StreamInfo)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return streamInfo, fmt.Errorf("mock get metadata returned error: %w", returnedErr)
	}

	return streamInfo, nil
}

func (m *MockJetStreamContext) ListStreams(ctx context.Context) <-chan jetstream.StreamInfo {
	args := m.Called(ctx)
	streamInfoChan, _ := args.Get(0).(<-chan jetstream.StreamInfo)

	return streamInfoChan
}

func (m *MockJetStreamContext) ListConsumers(ctx context.Context, streamName string) <-chan jetstream.ConsumerInfo {
	args := m.Called(ctx, streamName)
	consumerInfoChan, _ := args.Get(0).(<-chan jetstream.ConsumerInfo)

	return consumerInfoChan
}

// CleanupPublisher exists in the current jetstream.JetStream interface without parameters.
func (m *MockJetStreamContext) CleanupPublisher() {}

// MockObjectStore is a mock implementation of the core.ObjectStore interface.
type MockObjectStore struct {
	mock.Mock
}

// Download is a mock implementation of the Download method.
func (m *MockObjectStore) Download(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)
	data, _ := args.Get(0).([]byte)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return nil, fmt.Errorf("mock download returned error for key %s: %w", key, returnedErr)
	}

	return data, nil
}

// Upload is a mock implementation of the Upload method.
func (m *MockObjectStore) Upload(ctx context.Context, key string, data []byte) error {
	args := m.Called(ctx, key, data)

	returnedErr := args.Error(0)
	if returnedErr != nil {
		return fmt.Errorf("mock upload returned error for key %s: %w", key, returnedErr)
	}

	return nil
}

// MockTTSProcessor is a mock implementation of the core.TTSProcessor interface.
type MockTTSProcessor struct {
	mock.Mock
}

// Process is a mock implementation of the Process method.
func (m *MockTTSProcessor) Process(ctx context.Context, text []byte, cfg core.TTSConfig) ([]byte, error) {
	args := m.Called(ctx, text, cfg)
	data, _ := args.Get(0).([]byte)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return nil, fmt.Errorf("mock process returned error: %w", returnedErr)
	}

	return data, nil
}

// GetConfig is a mock implementation of the GetConfig method.
func (m *MockTTSProcessor) GetConfig() core.TTSConfig {
	args := m.Called()
	cfg, _ := args.Get(0).(core.TTSConfig)

	return cfg
}

// MockJetStreamMsg is a mock implementation of the jetstream.Msg interface.
type MockJetStreamMsg struct {
	DataFunc func() []byte
	AckFunc  func() error
	NakFunc  func() error
}

func (m *MockJetStreamMsg) Data() []byte {
	return m.DataFunc()
}

func (m *MockJetStreamMsg) Ack() error                         { return m.AckFunc() }
func (m MockJetStreamMsg) Nak() error                          { return m.NakFunc() }
func (m *MockJetStreamMsg) DoubleAck(_ context.Context) error  { return nil }
func (m *MockJetStreamMsg) NakWithDelay(_ time.Duration) error { return m.NakFunc() }

// Additional methods required by jetstream.Msg interface.
func (m *MockJetStreamMsg) Subject() string                           { return "test.subject" }
func (m *MockJetStreamMsg) Headers() nats.Header                      { return nil }
func (m *MockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *MockJetStreamMsg) Reply() string                             { return "" }
func (m *MockJetStreamMsg) Term() error                               { return nil }
func (m *MockJetStreamMsg) TermWithReason(_ string) error             { return nil }
func (m *MockJetStreamMsg) InProgress() error                         { return nil }

func newTestEvent(workflowID string) *events.TextProcessedEvent {
	return &events.TextProcessedEvent{
		Header: events.EventHeader{
			EventID:    uuid.New().String(),
			Timestamp:  time.Now(),
			WorkflowID: workflowID,
			UserID:     "test-user",
			TenantID:   "test-tenant",
		},
		PNGKey:            "png-key",
		TextKey:           "text-key",
		PageNumber:        1,
		TotalPages:        1,
		Voice:             "default",
		Seed:              12345,
		NGL:               0,
		TopP:              0.9,
		RepetitionPenalty: 1.1,
		Temperature:       0.7,
	}
}

func TestNatsWorker_HandleMessage_Success(t *testing.T) {
	t.Parallel()
	// Arrange
	mockJetStream := new(MockJetStreamContext)
	mockStore := new(MockObjectStore)
	mockProcessor := new(MockTTSProcessor)
	log, err := logger.New(t.TempDir(), "test.log")
	require.NoError(t, err)

	streamName := "test-stream"
	subject := "test.subject"
	consumerName := "test-consumer"
	audioChunkCreatedSubject := "test.audio.created"

	worker, err := worker.NewNatsWorker(
		nil,
		mockJetStream,
		streamName,
		subject,
		consumerName,
		audioChunkCreatedSubject,
		mockStore,
		mockProcessor,
		log,
	)
	require.NoError(t, err)

	workflowID := uuid.New().String()
	event := newTestEvent(workflowID)

	eventData, err := json.Marshal(event)
	require.NoError(t, err)

	msg := &MockJetStreamMsg{
		DataFunc: func() []byte {
			return eventData
		},
		AckFunc: func() error {
			return nil
		},
		NakFunc: func() error {
			return nil
		},
	}

	mockStore.On("Download", mock.Anything, event.TextKey).Return([]byte("hello world"), nil)
	mockProcessor.On("GetConfig").Return(core.TTSConfig{
		ModelPath:         "path",
		SnacModelPath:     "snac_path",
		Voice:             "default",
		Seed:              12345,
		NGL:               0,
		TopP:              0.9,
		RepetitionPenalty: 1.1,
		Temperature:       0.7,
		AllowedVoices:     []string{"default"},
	})
	mockProcessor.On("Process", mock.Anything, []byte("hello world"), mock.Anything).Return([]byte("audio data"), nil)
	mockStore.On("Upload", mock.Anything, mock.Anything, []byte("audio data")).Return(nil)
	mockJetStream.On("Publish", mock.Anything, audioChunkCreatedSubject, mock.Anything).Return(&nats.PubAck{
		Stream:    "test-stream",
		Sequence:  1,
		Duplicate: false,
		Domain:    "",
	}, nil)

	// Act
	worker.HandleMessage(msg)

	mockJetStream.AssertCalled(t, "Publish", mock.Anything, audioChunkCreatedSubject, mock.Anything)

	// Assert
	mockStore.AssertCalled(t, "Download", mock.Anything, event.TextKey)
	mockProcessor.AssertCalled(t, "Process", mock.Anything, []byte("hello world"), mock.Anything)
	mockStore.AssertCalled(t, "Upload", mock.Anything, mock.Anything, []byte("audio data"))
}

// MockJetStreamStream is a mock implementation of the jetstream.Stream interface.
type MockJetStreamStream struct {
	mock.Mock
}

func (m *MockJetStreamStream) CreateOrUpdateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	args := m.Called(ctx, cfg)
	consumer, _ := args.Get(0).(jetstream.Consumer)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return consumer, fmt.Errorf("mock CreateOrUpdateConsumer returned error: %w", returnedErr)
	}

	return consumer, nil
}

// Additional methods needed by jetstream.Stream interface would go here

// MockJetStreamConsumer is a mock implementation of the jetstream.Consumer interface.
type MockJetStreamConsumer struct {
	mock.Mock
}

// NextMsg is a mock implementation of the NextMsg method.
func (m *MockJetStreamConsumer) NextMsg(ctx context.Context) (jetstream.Msg, error) {
	args := m.Called(ctx)
	msg, _ := args.Get(0).(jetstream.Msg)

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return msg, fmt.Errorf("mock NextMsg returned error: %w", returnedErr)
	}

	return msg, nil
}

// Additional methods needed by jetstream.Consumer interface would go here
// Conn returns the underlying NATS connection (nil for tests).
func (m *MockJetStreamContext) Conn() *nats.Conn { return nil }
