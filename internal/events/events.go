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

package events

import "time"

// EventHeader contains metadata common to all events.
type EventHeader struct {
	Timestamp  time.Time `json:"Timestamp"`
	WorkflowID string    `json:"WorkflowID"`
	UserID     string    `json:"UserID"`
	TenantID   string    `json:"TenantID"`
	EventID    string    `json:"EventID"`
}

type AudioSessionConfig struct {
	SessionID        string `json:"SessionID"`
	SourceDocumentID string `json:"SourceDocumentID"`
	VoiceID          string `json:"VoiceID"`    // The parsed voice name, e.g., "niko"
	VoiceStyle       string `json:"VoiceStyle"` // The parsed voice style, e.g., "calm, deep, mature"
	MusicPrompt      string `json:"MusicPrompt"`
	TextDirective    string `json:"TextDirective"`
}

type JobSettings struct {
	SoundscapePrompt   string              `json:"SoundscapePrompt,omitempty"`
	AugmentationPrompt string              `json:"AugmentationPrompt,omitempty"`
	Exclusions         string              `json:"Exclusions,omitempty"`
	Voice              string              `json:"Voice,omitempty"` // The raw voice string from the UI
	AudioSessionConfig *AudioSessionConfig `json:"AudioSessionConfig,omitempty"`
}

// TextProcessedEvent is triggered after text has been extracted from a PNG.
type TextProcessedEvent struct {
	Header     EventHeader  `json:"Header"`
	PNGKey     string       `json:"PNGKey"`
	TextKey    string       `json:"TextKey"`
	PageNumber int          `json:"PageNumber"`
	TotalPages int          `json:"TotalPages"`
	Settings   *JobSettings `json:"Settings,omitempty"`
}

// AudioChunkCreatedEvent is triggered when a single audio chunk (page) is created.
// In the simplified context, it can also represent the final audio creation.
type AudioChunkCreatedEvent struct {
	Header     EventHeader `json:"Header"`
	AudioKey   string      `json:"AudioKey"`
	PageNumber int         `json:"PageNumber"`
	TotalPages int         `json:"TotalPages"`
}

// TTSStartedEvent is triggered when the text-to-speech process begins for a page.
type TTSStartedEvent struct {
	Header     EventHeader `json:"Header"`
	PageNumber int         `json:"PageNumber"`
	TotalPages int         `json:"TotalPages"`
}

// MusicStartedEvent is triggered when background music generation begins.
type MusicStartedEvent struct {
	Header     EventHeader `json:"Header"`
}

// AggregationStartedEvent is triggered when the aggregation and finalization of audio chunks begins.
type AggregationStartedEvent struct {
	Header     EventHeader `json:"Header"`
	TotalPages int         `json:"TotalPages"`
}

// MusicRequestEvent is triggered by the tts-service to request a soundscape.
type MusicRequestEvent struct {
	Header          EventHeader `json:"Header"`
	Prompt          string      `json:"Prompt"`
	DurationSeconds int         `json:"DurationSeconds"`
}

// MusicCreatedEvent is triggered when the soundscape has been generated and stored.
type MusicCreatedEvent struct {
	Header   EventHeader `json:"Header"`
	MusicKey string      `json:"MusicKey"`
}
