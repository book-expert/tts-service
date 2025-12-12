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

// TextProcessedEvent is triggered after text has been extracted from a PNG.
type TextProcessedEvent struct {
	Header     EventHeader `json:"header"`
	PNGKey     string      `json:"png_key"`
	TextKey    string      `json:"text_key"`
	PageNumber int         `json:"page_number"`
	TotalPages int         `json:"total_pages"`
	Voice      string      `json:"voice,omitempty"`
}

// AudioChunkCreatedEvent is triggered when a single audio chunk (page) is created.
// In the simplified context, it can also represent the final audio creation.
type AudioChunkCreatedEvent struct {
	Header     EventHeader `json:"header"`
	AudioKey   string      `json:"audio_key"`
	PageNumber int         `json:"page_number"`
	TotalPages int         `json:"total_pages"`
}
