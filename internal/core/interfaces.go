/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

// Package core defines the core business logic and interfaces for the TTS service.
package core

import "context"

// ObjectStore defines the high-integrity storage interface for artifacts.
type ObjectStore interface {
	Download(requestContext context.Context, objectKey string) ([]byte, error)
	Upload(requestContext context.Context, objectKey string, byteData []byte) error
}

// TTSProcessor defines the high-fidelity conversion interface for text-to-speech.
type TTSProcessor interface {
	Process(requestContext context.Context, textContent []byte, configuration TextToSpeechConfiguration) ([]byte, error)
}

// TextToSpeechConfiguration holds parameters for a single high-fidelity generation request.
type TextToSpeechConfiguration struct {
	SessionIdentifier string
	VoiceIdentifier   string
	VoiceStyle        string
	MusicPrompt       string
	TextDirective     string
}
