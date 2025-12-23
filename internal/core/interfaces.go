/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS
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

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
--------------------------------------------------------------------------------
*/

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
	GenerateMusic(ctx context.Context, prompt string, duration int) ([]byte, error)
	MixAudio(ctx context.Context, speechData, musicData []byte) ([]byte, error)
	CombineAudio(ctx context.Context, inputPaths []string) ([]byte, error)
	FinalizeAudio(ctx context.Context, speechData []byte, musicPrompt string) ([]byte, error)
}
