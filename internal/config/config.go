/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS, RELIABILITY, CONSISTENCY
--------------------------------------------------------------------------------
"Work is love made visible."

1.  PARTNERSHIP & CRAFT: We do not just "finish tasks"; we build with pride.
    We question decisions that compromise safety, security, or quality.
2.  HONESTY: We verify before we claim success. We write what we mean.
    If we don't know, we ask.
3.  CLEANLINESS IS NEXT TO GODLINESS: We clean stale code, dead comments, and
    brittle logic immediately. A sanitary codebase protects focus.
4.  SIMPLICITY IS KINDNESS: We choose the simple, explicit path. Complexity
    is a burden we refuse to place on others.
5.  PROTECTION: We handle errors, secure data, and ensure non-blocking flow.
    We do not leave traps for the future.
6.  RELIABILITY: We act as guardians of the work. We value safety over speed.
    We do not break what works to fix what isn't broken.
7.  CONSISTENCY: We uphold standards even when no one is watching.
    Predictability is the soil in which trust grows.

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
-   Reliability = Safety: we protect the user's investment; stability is paramount.
-   Consistency = Integrity: uniform quality reduces cognitive load and fear.
--------------------------------------------------------------------------------
*/

// Package config manages the loading and parsing of the service configuration.
// It maps the project.toml file to strongly typed structures for use throughout the application.
package config

import (
	"fmt"
	"os"

	"github.com/pelletier/go-toml/v2"
)

// DefaultConfigFilename defines the fallback path if none is provided.
const DefaultConfigFilename = "project.toml"

// Config represents the top-level structure of the service configuration.
// Why: Using a top-level struct allows for easy deserialization of the entire TOML file.
type Config struct {
	Service ServiceSettings `toml:"service"`
	TTS     TTSSettings     `toml:"tts"`
	NATS    NATSSettings    `toml:"nats"`
}

// ServiceSettings holds general service settings like logging and concurrency.
type ServiceSettings struct {
	LogDirectory    string `toml:"log_dir"`
	WorkerCount     int    `toml:"workers"`
	UserDatabaseURL string `toml:"user_database_url"`
}

// TTSSettings holds configuration specific to the Text-To-Speech provider.
type TTSSettings struct {
	// APIKeyEnvironmentVariable is the name of the env var holding the secret key.
	// Why: Storing the variable name rather than the key itself prevents secrets from being committed to source control.
	APIKeyEnvironmentVariable string `toml:"api_key_variable"`
	BaseURL                   string `toml:"base_url"`
	AudioServerURL            string `toml:"audio_server_url"`
	SpeechConcurrency         int    `toml:"speech_concurrency"`
}

// NATSSettings holds all NATS-related configuration including connection, streams, and buckets.
type NATSSettings struct {
	URL                    string              `toml:"url"`
	DeadLetterQueueSubject string              `toml:"dlq_subject"`
	Consumer               ConsumerSettings    `toml:"consumer"`
	Producer               ProducerSettings    `toml:"producer"`
	ObjectStore            ObjectStoreSettings `toml:"object_store"`
	KeyValueStore          KeyValueSettings    `toml:"kv"`
}

// ConsumerSettings defines the JetStream consumer settings.
type ConsumerSettings struct {
	StreamName    string `toml:"stream"`
	SubjectFilter string `toml:"subject"`
	DurableName   string `toml:"durable"`
}

// ProducerSettings defines the settings for publishing events.
type ProducerSettings struct {
	StreamName                string `toml:"stream"`
	SubjectName               string `toml:"subject"`
	TTSStartedSubject         string `toml:"tts_started_subject"`
	MusicStartedSubject       string `toml:"music_started_subject"`
	AggregationStartedSubject string `toml:"aggregation_started_subject"`
}

// ObjectStoreSettings defines the bucket names for storing large payloads.
type ObjectStoreSettings struct {
	TextBucketName string `toml:"text_bucket"`
	TTSBucketName  string `toml:"tts_bucket"`
}

// KeyValueSettings defines the bucket names for Key-Value storage.
type KeyValueSettings struct {
	ProgressBucketName string `toml:"progress_bucket"`
}

// Load reads and parses the configuration file from the specified path.
//
// Why: Centralizes configuration loading logic to ensure consistent behavior across the application.
func Load(filePath string) (*Config, error) {
	if filePath == "" {
		filePath = DefaultConfigFilename
	}

	configFile, openError := os.Open(filePath)
	if openError != nil {
		return nil, fmt.Errorf("failed to open config file at '%s': %w", filePath, openError)
	}
	defer func() {
		// Log to stderr as we might not have a configured logger yet.
		if closeError := configFile.Close(); closeError != nil {
			fmt.Fprintf(os.Stderr, "failed to close config file: %v\n", closeError)
		}
	}()

	var configuration Config
	decoder := toml.NewDecoder(configFile)
	if decodeError := decoder.Decode(&configuration); decodeError != nil {
		return nil, fmt.Errorf("failed to decode config file content: %w", decodeError)
	}

	// Apply Defaults & Overrides
	if configuration.TTS.SpeechConcurrency == 0 {
		configuration.TTS.SpeechConcurrency = 1 // Safe default
	}

	if envConcurrency := os.Getenv("TTS_SPEECH_CONCURRENCY"); envConcurrency != "" {
		var val int
		if _, err := fmt.Sscanf(envConcurrency, "%d", &val); err == nil && val > 0 {
			configuration.TTS.SpeechConcurrency = val
		}
	}

	return &configuration, nil
}
