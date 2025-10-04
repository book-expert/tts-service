// Package config provides the configuration structure for the tts-service.
package config

import (
    "fmt"
    "github.com/book-expert/configurator"
    "github.com/book-expert/logger"
)

// TTSServiceConfig holds the specific configuration for the TTS service.
type TTSServiceConfig struct {
	ModelPath         string   `toml:"model_path"`
	SnacModelPath     string   `toml:"snac_model_path"`
	Voice             string   `toml:"voice"`
	Temperature       float64  `toml:"temperature"`
	TimeoutSeconds    int      `toml:"timeout_seconds"`
	Seed              int      `toml:"seed"`
	NGL               int      `toml:"ngl"`
	TopP              float64  `toml:"top_p"`
	RepetitionPenalty float64  `toml:"repetition_penalty"`
	AllowedVoices     []string `toml:"allowed_voices"`
}

// NATSConfig holds NATS and JetStream subject/stream configuration
// specific to the tts-service.
type NATSConfig struct {
    URL                       string `toml:"url"`
    TTSStreamName             string `toml:"tts_stream_name"`
    TTSConsumerName           string `toml:"tts_consumer_name"`
    TextProcessedSubject      string `toml:"text_processed_subject"`
    AudioChunkCreatedSubject  string `toml:"audio_chunk_created_subject"`
    AudioObjectStoreBucket    string `toml:"audio_object_store_bucket"`
    AudioProcessingStreamName string `toml:"audio_processing_stream_name"`
}

// Config is the root configuration structure.
type Config struct {
    NATS NATSConfig       `toml:"nats"`
    TTS  TTSServiceConfig `toml:"tts_service"`
}

// Load loads the configuration for the tts-service.
func Load(log *logger.Logger) (*Config, error) {
	var cfg Config

	err := configurator.Load(&cfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration from configurator: %w", err)
	}

	return &cfg, nil
}
