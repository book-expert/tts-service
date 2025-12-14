package events

import "time"

// EventHeader contains metadata common to all events.
type EventHeader struct {
	Timestamp  time.Time `json:"timestamp"`
	WorkflowID string    `json:"workflow_id"`
	UserID     string    `json:"user_id"`
	TenantID   string    `json:"tenant_id"`
	EventID    string    `json:"event_id"`
}


type AudioSessionConfig struct {
	SessionID        string `json:"session_id"`
	SourceDocumentID string `json:"source_document_id"`
	VoiceID          string `json:"voice_id"`
	MasterDirective  string `json:"master_directive"`
}

type JobSettings struct {
	TranscriptionMode  string              `json:"transcription_mode,omitempty"`
	StyleProfile       string              `json:"style_profile,omitempty"`
	CustomInstructions string              `json:"custom_instructions,omitempty"`
	Exclusions         string              `json:"exclusions,omitempty"`
	Voice              string              `json:"voice,omitempty"`
	Language           string              `json:"language,omitempty"`
	Scene              string              `json:"scene,omitempty"`
	Style              string              `json:"style,omitempty"`
	Accent             string              `json:"accent,omitempty"`
	Articulation       string              `json:"articulation,omitempty"`
	Pace               string              `json:"pace,omitempty"`
	Personality        string              `json:"personality,omitempty"`
	AudioSessionConfig *AudioSessionConfig `json:"audio_session_config,omitempty"`
}

// TextProcessedEvent is triggered after text has been extracted from a PNG.
type TextProcessedEvent struct {
	Header     EventHeader  `json:"header"`
	PNGKey     string       `json:"png_key"`
	TextKey    string       `json:"text_key"`
	PageNumber int          `json:"page_number"`
	TotalPages int          `json:"total_pages"`
	Settings   *JobSettings `json:"settings,omitempty"`
}

// AudioChunkCreatedEvent is triggered when a single audio chunk (page) is created.
// In the simplified context, it can also represent the final audio creation.
type AudioChunkCreatedEvent struct {
	Header     EventHeader `json:"header"`
	AudioKey   string      `json:"audio_key"`
	PageNumber int         `json:"page_number"`
	TotalPages int         `json:"total_pages"`
}
