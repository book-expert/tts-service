// Package core defines the core business logic and interfaces for the TTS service.
package core

import "context"

// ObjectStore defines the interface for interacting with a key-value blob store.
type ObjectStore interface {
	Download(ctx context.Context, key string) ([]byte, error)
	Upload(ctx context.Context, key string, data []byte) error
}

// TTSConfig holds the configuration for a single TTS processing job.
// This allows for per-request customization of the TTS output.
type TTSConfig struct {
	ModelPath         string
	SnacModelPath     string
	Voice             string
	Seed              int
	NGL               int
	TopP              float64
	RepetitionPenalty float64
	Temperature       float64
	AllowedVoices     []string
}

// TTSProcessor defines the interface for a text-to-speech processing engine.
type TTSProcessor interface {
	Process(ctx context.Context, text []byte, cfg TTSConfig) ([]byte, error)
	GetConfig() TTSConfig
}
