package main

import (
    "context"
    "testing"

    "github.com/nats-io/nats.go/jetstream"
    "github.com/stretchr/testify/require"
)

// fakeStreamAdmin is a minimal fake implementing streamAdmin for testing.
type fakeStreamAdmin struct {
    lastCfg jetstream.StreamConfig
    called  bool
    retErr  error
}

func (f *fakeStreamAdmin) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	f.called = true
	f.lastCfg = cfg

	return nil, f.retErr
}

func TestEnsureStreamForSubject_ConfigIsRespected(t *testing.T) {
	t.Parallel()

	const (
		streamName = "AUDIO_PROCESSING"
		subject    = "audio.chunk.created"
	)

	fake := new(fakeStreamAdmin)

	err := ensureStreamForSubject(context.Background(), fake, streamName, subject)
	require.NoError(t, err)
	require.True(t, fake.called)
	require.NotNil(t, fake.lastCfg)
	require.Equal(t, streamName, fake.lastCfg.Name)
	require.Equal(t, []string{subject}, fake.lastCfg.Subjects)
	// Note: lastOpts checking removed as the new jetstream interface doesn't use JS options
}
