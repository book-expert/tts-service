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
[nats]
url = "nats://127.0.0.1:4222"
tts_stream_name = "TTS_JOBS"
tts_consumer_name = "tts-workers"
text_processed_subject = "text.processed"
audio_chunk_created_subject = "audio.chunk.created"
audio_object_store_bucket = "AUDIO_FILES"
audio_processing_stream_name = "AUDIO_PROCESSING"

[tts_service]
model_path = "models/outetts.bin"
temperature = 0.7
timeout_seconds = 300
`

	var cfg config.Config

	err := toml.Unmarshal([]byte(tomlData), &cfg)
	require.NoError(t, err)

	assert.Equal(t, "nats://127.0.0.1:4222", cfg.NATS.URL)
	assert.Equal(t, "TTS_JOBS", cfg.NATS.TTSStreamName)
	assert.Equal(t, "tts-workers", cfg.NATS.TTSConsumerName)
	assert.Equal(t, "text.processed", cfg.NATS.TextProcessedSubject)
	assert.Equal(t, "audio.chunk.created", cfg.NATS.AudioChunkCreatedSubject)
	assert.Equal(t, "AUDIO_FILES", cfg.NATS.AudioObjectStoreBucket)
    assert.Equal(t, "AUDIO_PROCESSING", cfg.NATS.AudioProcessingStreamName)
	assert.Equal(t, "models/outetts.bin", cfg.TTS.ModelPath)
	assert.InEpsilon(t, 0.7, cfg.TTS.Temperature, 0.001)
	assert.Equal(t, 300, cfg.TTS.TimeoutSeconds)
}
