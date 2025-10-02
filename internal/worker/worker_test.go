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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockJetStreamContext is a mock implementation of the nats.JetStreamContext interface.
type MockJetStreamContext struct {
	mock.Mock
}

// Publish is a mock implementation of the Publish method.
func (m *MockJetStreamContext) Publish(subj string, data []byte, _ ...nats.PubOpt) (*nats.PubAck, error) {
	args := m.Called(subj, data)

	var pubAck *nats.PubAck

	ackValue := args.Get(0)
	if ackValue != nil {
		if convertedAck, ok := ackValue.(*nats.PubAck); ok {
			pubAck = convertedAck
		}
	}

	returnedErr := args.Error(1)
	if returnedErr != nil {
		return pubAck, fmt.Errorf("mock publish returned error: %w", returnedErr)
	}

	return pubAck, nil
}

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

    subject := "test.subject"
    audioChunkCreatedSubject := "test.audio.created"

    worker, err := worker.NewNatsWorker(
        nil,
        mockJetStream,
        subject,
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

	msg := &nats.Msg{
		Subject: subject,
		Reply:   "",
		Header:  nil,
		Data:    eventData,
		Sub:     nil,
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
    mockJetStream.On("Publish", audioChunkCreatedSubject, mock.Anything).Return(&nats.PubAck{
        Stream:    "test-stream",
        Sequence:  1,
        Duplicate: false,
        Domain:    "",
    }, nil)

	// Act
	worker.HandleMessage(msg)

	// Assert
    mockJetStream.AssertCalled(t, "Publish", audioChunkCreatedSubject, mock.Anything)
}
