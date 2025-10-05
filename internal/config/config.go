// Package config provides the configuration structure for the tts-service.
package config

import (
    "fmt"
    "github.com/book-expert/configurator"
    "github.com/book-expert/logger"
)

// TTSServiceConfig holds the specific configuration for the TTS service. This
// struct is used to configure the TTS processor and the NATS worker.
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
    // DeadLetterSubject is the subject to which failed messages are published.
    // This is mandatory for robust failure handling.
    DeadLetterSubject string   `toml:"dead_letter_subject"`
}

// Config is the root configuration structure. This struct is the main data
// structure that is passed to the application to configure it.
type Config struct {
    ServiceNATS configurator.ServiceNATSConfig `toml:"tts-service"`
    TTS         TTSServiceConfig               `toml:"tts_service"`
}

// Load loads the configuration for the tts-service. This function is the single
// entry point for loading the configuration and ensures that the application
// starts with a valid and complete configuration.
func Load(log *logger.Logger) (*Config, error) {
	var cfg Config

	err := configurator.Load(&cfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration from configurator: %w", err)
	}

	return &cfg, nil
}
