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
func (m *MockJetStreamContext) Publish(
	ctx context.Context,
	subject string,
	data []byte,
	_ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
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

// Note: For worker tests we only require Publish.

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
func (m *MockJetStreamMsg) Nak() error                         { return m.NakFunc() }
func (m *MockJetStreamMsg) DoubleAck(_ context.Context) error  { return nil }
func (m *MockJetStreamMsg) NakWithDelay(_ time.Duration) error { return m.NakFunc() }

// Additional methods required by jetstream.Msg interface.
func (m *MockJetStreamMsg) Subject() string      { return "test.subject" }
func (m *MockJetStreamMsg) Headers() nats.Header { return nil }
func (m *MockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return new(jetstream.MsgMetadata), nil
}
func (m *MockJetStreamMsg) Reply() string                 { return "" }
func (m *MockJetStreamMsg) Term() error                   { return nil }
func (m *MockJetStreamMsg) TermWithReason(_ string) error { return nil }
func (m *MockJetStreamMsg) InProgress() error             { return nil }

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

    jetstreamMock := new(MockJetStreamContext)
	store := new(MockObjectStore)
	proc := new(MockTTSProcessor)
	log, err := logger.New(t.TempDir(), "test.log")
	require.NoError(t, err)

    nw := buildTestWorker(t, jetstreamMock, store, proc, log)
	event := newTestEvent(uuid.New().String())
	msg := buildMsgFromEvent(t, event)
    arrangeHappyPath(store, proc, jetstreamMock, event)

	nw.HandleMessage(context.Background(), msg)

    jetstreamMock.AssertCalled(t, "Publish", mock.Anything, "test.audio.created", mock.Anything)
	store.AssertCalled(t, "Download", mock.Anything, event.TextKey)
	proc.AssertCalled(t, "Process", mock.Anything, []byte("hello world"), mock.Anything)
	store.AssertCalled(t, "Upload", mock.Anything, mock.Anything, []byte("audio data"))
}

func buildTestWorker(
    t *testing.T,
    jetstreamMock *MockJetStreamContext,
    store *MockObjectStore,
    proc *MockTTSProcessor,
    log *logger.Logger,
) *worker.NatsWorker {
	t.Helper()

    natsWorker, err := worker.NewNatsWorker(
        nil,
        nil,
        jetstreamMock,
        "test-stream",
        "test.subject",
        "test-consumer",
        "test.audio.created",
        store,
        proc,
        log,
    )
	require.NoError(t, err)

    return natsWorker
}

func buildMsgFromEvent(t *testing.T, event *events.TextProcessedEvent) *MockJetStreamMsg {
	t.Helper()

	data, err := json.Marshal(event)
	require.NoError(t, err)

    return &MockJetStreamMsg{
        DataFunc: func() []byte { return data },
        AckFunc:  func() error { return nil },
        NakFunc:  func() error { return nil },
    }
}

func arrangeHappyPath(
    store *MockObjectStore,
    proc *MockTTSProcessor,
    jetstreamMock *MockJetStreamContext,
    event *events.TextProcessedEvent,
) {
	store.On("Download", mock.Anything, event.TextKey).Return([]byte("hello world"), nil)
    proc.On("GetConfig").Return(core.TTSConfig{
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
	proc.On("Process", mock.Anything, []byte("hello world"), mock.Anything).Return([]byte("audio data"), nil)
	store.On("Upload", mock.Anything, mock.Anything, []byte("audio data")).Return(nil)

    ack := new(jetstream.PubAck)
    ack.Stream = "test-stream"
    ack.Sequence = 1
    jetstreamMock.On("Publish", mock.Anything, "test.audio.created", mock.Anything).Return(ack, nil)
}

// Conn returns the underlying NATS connection (nil for tests).
func (m *MockJetStreamContext) Conn() *nats.Conn { return nil }
