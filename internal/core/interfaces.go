/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

// Package core defines the core business logic and interfaces for the TTS service.
package core

import "context"

type ObjectStore interface {
	Download(requestContext context.Context, key string) ([]byte, error)
	Upload(requestContext context.Context, key string, data []byte) error
}

type TTSProcessor interface {
	Process(requestContext context.Context, text []byte, configuration TTSConfig) ([]byte, error)
}

// TTSConfig holds parameters for a single TTS processing request.
type TTSConfig struct {
	SessionID     string
	VoiceID       string
	VoiceStyle    string
	MusicPrompt   string
	TextDirective string
}
