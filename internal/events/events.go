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
