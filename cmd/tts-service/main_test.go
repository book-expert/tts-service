package main

import (
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"testing"
)

// fakeStreamAdmin is a minimal fake implementing streamAdmin for testing.
type fakeStreamAdmin struct {
	lastCfg  *nats.StreamConfig
	lastOpts []nats.JSOpt
	called   bool
	retErr   error
}

func (f *fakeStreamAdmin) AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	f.called = true
	f.lastCfg = cfg
	f.lastOpts = opts

	return nil, f.retErr
}

func TestEnsureStreamForSubject_ConfigIsRespected(t *testing.T) {
	t.Parallel()

	const (
		streamName = "AUDIO_PROCESSING"
		subject    = "audio.chunk.created"
	)

	fake := new(fakeStreamAdmin)

	err := ensureStreamForSubject(fake, streamName, subject)
	require.NoError(t, err)
	require.True(t, fake.called)
	require.NotNil(t, fake.lastCfg)
	require.Equal(t, streamName, fake.lastCfg.Name)
	require.Equal(t, []string{subject}, fake.lastCfg.Subjects)
	require.NotEmpty(t, fake.lastOpts) // ensure we pass at least one JS option (e.g., MaxWait)
}
