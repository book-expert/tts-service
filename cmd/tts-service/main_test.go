package main

import (
    "context"
    "testing"

    "github.com/book-expert/configurator"
    ttsconfig "github.com/book-expert/tts-service/internal/config"
    "github.com/nats-io/nats.go/jetstream"
    "github.com/stretchr/testify/require"
)

// fakeStreamAdmin is a minimal fake implementing streamAdmin for testing.
type fakeCreate struct {
	lastCfg jetstream.StreamConfig
	called  bool
	retErr  error
}

func (f *fakeCreate) fn(_ context.Context, cfg jetstream.StreamConfig) error {
    f.called = true
    f.lastCfg = cfg

    return f.retErr
}

func TestEnsureStreamForSubject_ConfigIsRespected(t *testing.T) {
	t.Parallel()

	const (
		streamName = "AUDIO_PROCESSING"
		subject    = "audio.chunk.created"
	)

	fake := new(fakeCreate)

	err := ensureStreamForSubject(context.Background(), fake.fn, streamName, subject)
	require.NoError(t, err)
	require.True(t, fake.called)
	require.NotNil(t, fake.lastCfg)
	require.Equal(t, streamName, fake.lastCfg.Name)
	require.Equal(t, []string{subject}, fake.lastCfg.Subjects)
	// Note: lastOpts checking removed as the new jetstream interface doesn't use JS options
}

func TestValidateServiceNATSConfig_Success(t *testing.T) {
    t.Parallel()

    cfg := &ttsconfig.Config{
        ServiceNATS: configurator.ServiceNATSConfig{
            NATS: configurator.NATSConfig{URL: ""},
            Streams: []configurator.StreamConfig{
                {Name: "text", Subjects: []string{"text.processed"}},
                {Name: "audio", Subjects: []string{"audio.chunk.created"}},
            },
            Consumers: []configurator.ConsumerConfig{
                {StreamName: "text", ConsumerName: "tts-workers", FilterSubject: "text.processed"},
            },
            ObjectStores: []configurator.ObjectStoreConfig{{BucketName: "audio-files"}},
            KeyValue:     nil,
        },
        TTS: ttsconfig.TTSServiceConfig{
            ModelPath:         "m",
            SnacModelPath:     "s",
            Voice:             "default",
            Temperature:       0.7,
            TimeoutSeconds:    10,
            Seed:              1,
            NGL:               0,
            TopP:              0.9,
            RepetitionPenalty: 1.1,
            AllowedVoices:     []string{"default"},
            DeadLetterSubject: "book-expert.tts.dlq",
        },
    }

    err := validateServiceNATSConfig(cfg)
    require.NoError(t, err)
}

func TestValidateServiceNATSConfig_FailsWithoutDLQ(t *testing.T) {
    t.Parallel()

    cfg := &ttsconfig.Config{
        ServiceNATS: configurator.ServiceNATSConfig{
            NATS:         configurator.NATSConfig{URL: ""},
            Streams:      []configurator.StreamConfig{{Name: "s", Subjects: []string{"a"}}},
            Consumers:    []configurator.ConsumerConfig{{StreamName: "s", ConsumerName: "c", FilterSubject: "a"}},
            ObjectStores: []configurator.ObjectStoreConfig{{BucketName: "b"}},
            KeyValue:     nil,
        },
        TTS: ttsconfig.TTSServiceConfig{
            ModelPath:         "m",
            SnacModelPath:     "s",
            Voice:             "default",
            Temperature:       0.7,
            TimeoutSeconds:    10,
            Seed:              1,
            NGL:               0,
            TopP:              0.9,
            RepetitionPenalty: 1.1,
            AllowedVoices:     []string{"default"},
            DeadLetterSubject: "",
        },
    }
    err := validateServiceNATSConfig(cfg)
    require.Error(t, err)
    require.Equal(t, ErrDeadLetterSubjectEmpty, err)
}

func TestValidateServiceNATSConfig_ConsumerStreamMissing(t *testing.T) {
    t.Parallel()

    cfg := &ttsconfig.Config{
        ServiceNATS: configurator.ServiceNATSConfig{
            NATS:     configurator.NATSConfig{URL: ""},
            Streams:  []configurator.StreamConfig{{Name: "text", Subjects: []string{"text.processed"}}},
            Consumers: []configurator.ConsumerConfig{
                {
                    StreamName:    "missing",
                    ConsumerName:  "tts-workers",
                    FilterSubject: "text.processed",
                },
            },
            ObjectStores: []configurator.ObjectStoreConfig{{BucketName: "audio-files"}},
            KeyValue:     nil,
        },
        TTS: ttsconfig.TTSServiceConfig{
            ModelPath:         "m",
            SnacModelPath:     "s",
            Voice:             "default",
            Temperature:       0.7,
            TimeoutSeconds:    10,
            Seed:              1,
            NGL:               0,
            TopP:              0.9,
            RepetitionPenalty: 1.1,
            AllowedVoices:     []string{"default"},
            DeadLetterSubject: "book-expert.tts.dlq",
        },
    }
    err := validateServiceNATSConfig(cfg)
    require.Error(t, err)
    require.Equal(t, ErrConsumerStreamNotFound, err)
}

func TestValidateServiceNATSConfig_FilterSubjectNotInStream(t *testing.T) {
    t.Parallel()

    cfg := &ttsconfig.Config{
        ServiceNATS: configurator.ServiceNATSConfig{
            NATS:    configurator.NATSConfig{URL: ""},
            Streams: []configurator.StreamConfig{{Name: "text", Subjects: []string{"other.subject"}}},
            Consumers: []configurator.ConsumerConfig{
                {
                    StreamName:    "text",
                    ConsumerName:  "tts-workers",
                    FilterSubject: "text.processed",
                },
            },
            ObjectStores: []configurator.ObjectStoreConfig{{BucketName: "audio-files"}},
            KeyValue:     nil,
        },
        TTS: ttsconfig.TTSServiceConfig{
            ModelPath:         "m",
            SnacModelPath:     "s",
            Voice:             "default",
            Temperature:       0.7,
            TimeoutSeconds:    10,
            Seed:              1,
            NGL:               0,
            TopP:              0.9,
            RepetitionPenalty: 1.1,
            AllowedVoices:     []string{"default"},
            DeadLetterSubject: "book-expert.tts.dlq",
        },
    }
    err := validateServiceNATSConfig(cfg)
    require.Error(t, err)
    require.Equal(t, ErrConsumerFilterMismatch, err)
}

func TestValidateServiceNATSConfig_ObjectStoreMissing(t *testing.T) {
    t.Parallel()

    cfg := &ttsconfig.Config{
        ServiceNATS: configurator.ServiceNATSConfig{
            NATS:         configurator.NATSConfig{URL: ""},
            Streams:      []configurator.StreamConfig{{Name: "s", Subjects: []string{"a"}}},
            Consumers:    []configurator.ConsumerConfig{{StreamName: "s", ConsumerName: "c", FilterSubject: "a"}},
            ObjectStores: nil,
            KeyValue:     nil,
        },
        TTS: ttsconfig.TTSServiceConfig{
            ModelPath:         "m",
            SnacModelPath:     "s",
            Voice:             "default",
            Temperature:       0.7,
            TimeoutSeconds:    10,
            Seed:              1,
            NGL:               0,
            TopP:              0.9,
            RepetitionPenalty: 1.1,
            AllowedVoices:     []string{"default"},
            DeadLetterSubject: "book-expert.tts.dlq",
        },
    }
    err := validateServiceNATSConfig(cfg)
    require.Error(t, err)
    require.Equal(t, ErrNoObjectStoresConfigured, err)
}

func TestValidateServiceNATSConfig_PublishSubjectMustExist(t *testing.T) {
    t.Parallel()

    cfg := &ttsconfig.Config{
        ServiceNATS: configurator.ServiceNATSConfig{
            NATS: configurator.NATSConfig{URL: ""},
            Streams: []configurator.StreamConfig{
                {Name: "only", Subjects: []string{}},
                {Name: "text", Subjects: []string{"text.processed"}},
            },
            Consumers: []configurator.ConsumerConfig{
                {StreamName: "text", ConsumerName: "c", FilterSubject: "text.processed"},
            },
            ObjectStores: []configurator.ObjectStoreConfig{{BucketName: "b"}},
            KeyValue:     nil,
        },
        TTS: ttsconfig.TTSServiceConfig{
            ModelPath:         "m",
            SnacModelPath:     "s",
            Voice:             "default",
            Temperature:       0.7,
            TimeoutSeconds:    10,
            Seed:              1,
            NGL:               0,
            TopP:              0.9,
            RepetitionPenalty: 1.1,
            AllowedVoices:     []string{"default"},
            DeadLetterSubject: "book-expert.tts.dlq",
        },
    }
    // Force a failure by removing subjects so publish subject cannot be derived.
    cfg.ServiceNATS.Streams[0].Subjects = []string{}
    err := validateServiceNATSConfig(cfg)
    require.Error(t, err)
    require.Equal(t, ErrPublishSubjectDerive, err)
}
