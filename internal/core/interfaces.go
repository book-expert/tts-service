// Package core defines the core business logic and interfaces for the TTS service.
package core

import "context"

type ObjectStore interface {
	Download(ctx context.Context, key string) ([]byte, error)
	Upload(ctx context.Context, key string, data []byte) error
}

// TTSConfig holds configuration for a specific TTS request, derived from events.AudioSessionConfig.
type TTSConfig struct {
	SessionID   string
	VoiceID     string
	MusicPrompt string
}

// TTSProcessor defines the interface for text-to-speech processing.
type TTSProcessor interface {
	Process(ctx context.Context, text []byte, config TTSConfig) ([]byte, error)
}
