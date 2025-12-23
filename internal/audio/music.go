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

package audio

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/book-expert/logger"
)

type MusicClient struct {
	apiKey      string
	wrapperPath string
	logger      *logger.Logger
}

func NewMusicClient(ctx context.Context, apiKey string, log *logger.Logger) (*MusicClient, error) {
	// Locate wrapper.py
	// We assume CWD is the service root (tts-service/)
	wrapperPath, err := filepath.Abs("internal/audio/wrapper.py")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve wrapper path: %w", err)
	}
	
	if _, err := os.Stat(wrapperPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("wrapper.py not found at %s", wrapperPath)
	}

	return &MusicClient{
		apiKey:      apiKey,
		wrapperPath: wrapperPath,
		logger:      log,
	}, nil
}

// GenerateMusic generates a music track from the prompt using Lyria RealTime (via Python Wrapper).
func (c *MusicClient) GenerateMusic(ctx context.Context, prompt string, durationSeconds int) ([]byte, error) {
	c.logger.Infof("Generating music via wrapper. Prompt: %s, Duration: %ds", prompt, durationSeconds)

	// Temp Output File
	tmpFile, err := os.CreateTemp("", "lyria_music_*.wav")
	if err != nil {
		return nil, fmt.Errorf("create temp file failed: %w", err)
	}
	outputFile := tmpFile.Name()
	_ = tmpFile.Close() // Close immediately so python can write to it
	defer func() { _ = os.Remove(outputFile) }()

	// Execute Python Wrapper
	// We use the 'python3' from the environment (assuming sourced .venv or global)
	cmd := exec.CommandContext(ctx, "python3", c.wrapperPath,
		"--api-key", c.apiKey,
		"--prompt", prompt,
		"--output", outputFile,
		"--duration", fmt.Sprintf("%d", durationSeconds),
	)

	// Capture stderr for debugging
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("wrapper execution failed: %w, output: %s", err, string(out))
	}

	c.logger.Infof("Wrapper finished. Output: %s", string(out))

	// Read Result
	data, err := os.ReadFile(outputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read generated music file: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("generated music file is empty")
	}

	return data, nil
}