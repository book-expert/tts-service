/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
"Work is love made visible. And if you cannot work with love but only with
distaste, it is better that you should leave your work and sit at the gate of
the temple and take alms of those who work with joy." — Kahlil Gibran

1.  LOVE AND CARE (Primary Driver)
    - This is a craft. Build with pride, honesty, and kindness.
    - If you put love in your work, you build something deserving of love.
    - Be helpful: Code is read more than written; optimize for the reader.

2.  WRITE WHAT YOU MEAN (Explicit > Implicit)
    - Use WHOLE WORDS: `RequestIdentifier` not `ReqID`.
    - No magic numbers: Move application settings to `project.toml`.
    - Secure by design: Keep API keys and secrets strictly in `.env`.
    - No ambiguity: If you assume something, document it.

3.  SIMPLE IS EFFICIENT (Minimal Viable Elegance)
    - Avoid over-engineering. Small interfaces, clear structs.
    - If a design requires a hack, stop. Redesign it with elegance.
    - Lean, Clean, Mean: Delete dead code immediately.

4.  NO BASELESS ASSUMPTIONS (Scientific Rigor)
    - Do not guess. Base decisions on documentation and proven patterns.
    - If you do not know, ask or verify.

5.  NON-BLOCKING & ROBUST
    - Never block the main goroutine. Use Context for cancellation.
    - Handle errors explicitly: Don't just return them, wrap them with context.

--------------------------------------------------------------------------------
EXAMPLES OF "LOVE AND CARE" IN THIS CONTEXT:
--------------------------------------------------------------------------------
(A) NAMING
    Indifferent:  func Gen(t string, v string)
    With Love:    func GenerateSoundscape(ctx context.Context, textPrompt string, voiceID string)
    *Why: The Agent reading this next year will know exactly what it does and that it is cancellable.*

(B) CONFIGURATION
    Indifferent:  const Timeout = 30 // Hardcoded
    With Love:    config.App.TimeoutSeconds // Loaded from project.toml
    *Why: Allows behavior tuning without recompiling or touching the codebase.*

(C) ERROR HANDLING
    Indifferent:  if err != nil { return err }
    With Love:    if err != nil { return fmt.Errorf("failed to initialize vox engine: %w", err) }
    *Why: Wrapping the error gives the user the 'trace of breadcrumbs' they need to fix it. That is kindness.*
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
	LogDirectory string `toml:"log_dir"`
	WorkerCount  int    `toml:"workers"`
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
	StreamName  string `toml:"stream"`
	SubjectName string `toml:"subject"`
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
