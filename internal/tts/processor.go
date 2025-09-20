// Package tts provides the implementation for the TTSProcessor interface.
package tts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/core"
)

// ErrNotImplemented is returned when a method is not yet implemented.
var ErrNotImplemented = errors.New("not yet implemented")

// ChatLLMProcessor implements the core.TTSProcessor interface by calling the chatllm binary.
type ChatLLMProcessor struct {
	config core.TTSConfig
	log    *logger.Logger
}

// New creates a new ChatLLMProcessor.
func New(cfg core.TTSConfig, log *logger.Logger) (*ChatLLMProcessor, error) {
	return &ChatLLMProcessor{
		config: cfg,
		log:    log,
	}, nil
}

// GetConfig returns the TTS configuration.
func (p *ChatLLMProcessor) GetConfig() core.TTSConfig {
	return p.config
}

// Process takes text and returns the raw audio data by calling the chatllm binary.
func (p *ChatLLMProcessor) Process(ctx context.Context, text []byte, cfg core.TTSConfig) ([]byte, error) {
	tempFile, err := os.CreateTemp("", "tts-output-*.pcm")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file for tts output: %w", err)
	}

	defer func() {
		removeErr := os.Remove(tempFile.Name())
		if removeErr != nil {
			p.log.Warn("Failed to remove temp file '%s': %v", tempFile.Name(), removeErr)
		}
	}()

	args := []string{
		"-m", p.config.ModelPath,
		"--snac_model", p.config.SnacModelPath,
		"-p", fmt.Sprintf("{%s}: %s", cfg.Voice, string(text)),
		"--tts_export", tempFile.Name(),
		"--seed", strconv.Itoa(cfg.Seed),
		"-ngl", strconv.Itoa(cfg.NGL),
		"--top_p", fmt.Sprintf("%.2f", cfg.TopP),
		"--repetition_penalty", fmt.Sprintf("%.2f", cfg.RepetitionPenalty),
		"--temp", fmt.Sprintf("%.2f", cfg.Temperature),
	}

	// #nosec G204 -- arguments are validated via core.TTSConfig validation
	cmd := exec.CommandContext(ctx, "chatllm", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("chatllm binary execution failed: %w - output: %s", err, string(output))
	}

	audioData, err := os.ReadFile(tempFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read audio data from temp file: %w", err)
	}

	return audioData, nil
}
