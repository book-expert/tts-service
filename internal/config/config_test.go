// Package config_test tests the configuration loading for the tts-service.
package config_test

import (
	"testing"

	"github.com/book-expert/tts-service/internal/config"
	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
    t.Parallel()

tomlData := `
[tts-service.nats]
url = "nats://127.0.0.1:4222"

[[tts-service.streams]]
name = "tts-jobs"
subjects = ["text.processed"]

[[tts-service.streams]]
name = "audio"
subjects = ["audio.chunk.created"]

[[tts-service.consumers]]
stream_name = "tts-jobs"
consumer_name = "tts-workers"
filter_subject = "text.processed"

[[tts-service.object_stores]]
bucket_name = "AUDIO_FILES"

[tts_service]
model_path = "models/outetts.bin"
temperature = 0.7
timeout_seconds = 300
`

	var cfg config.Config

	err := toml.Unmarshal([]byte(tomlData), &cfg)
	require.NoError(t, err)

    require.NotNil(t, cfg.ServiceNATS.NATS)
    assert.Equal(t, "nats://127.0.0.1:4222", cfg.ServiceNATS.NATS.URL)
    require.GreaterOrEqual(t, len(cfg.ServiceNATS.Streams), 2)
    require.GreaterOrEqual(t, len(cfg.ServiceNATS.Consumers), 1)
    require.GreaterOrEqual(t, len(cfg.ServiceNATS.ObjectStores), 1)
    assert.Equal(t, "AUDIO_FILES", cfg.ServiceNATS.ObjectStores[0].BucketName)
	assert.Equal(t, "models/outetts.bin", cfg.TTS.ModelPath)
	assert.InEpsilon(t, 0.7, cfg.TTS.Temperature, 0.001)
	assert.Equal(t, 300, cfg.TTS.TimeoutSeconds)
}
