// Package tts provides the implementation for the TTSProcessor interface.
package tts

import (
	"context"
	"errors"
)

// ErrNotImplemented is returned when a method is not yet implemented.
var ErrNotImplemented = errors.New("not yet implemented")

// Config holds the configuration for the ChatLLMProcessor.
type Config struct {
	BinaryPath    string
	ModelPath     string
	SnacModelPath string
	Voice         string
}

// ChatLLMProcessor implements the core.TTSProcessor interface by calling the chatllm binary.
type ChatLLMProcessor struct {
	config Config
}

// New creates a new ChatLLMProcessor.
func New(cfg Config) (*ChatLLMProcessor, error) {
	// We can add checks here to ensure the binary exists and is executable.
	return &ChatLLMProcessor{
		config: cfg,
	}, nil
}

// Process takes text and returns the raw audio data by calling the chatllm binary.
func (p *ChatLLMProcessor) Process(_ context.Context, _ []byte) ([]byte, error) {
	return nil, ErrNotImplemented
}
