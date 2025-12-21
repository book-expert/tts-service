/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
1.  LOVE AND CARE
    - Robust execution of external Python wrapper.
    - Clear error messages.

2.  SIMPLE IS EFFICIENT
    - Delegation to Python for experimental Lyria features.
*/

package music

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/book-expert/logger"
)

type Client struct {
	apiKey      string
	wrapperPath string
	logger      *logger.Logger
}

func NewClient(ctx context.Context, apiKey string, log *logger.Logger) (*Client, error) {
	// Locate wrapper.py
	// We assume CWD is the service root (tts-service/)
	wrapperPath, err := filepath.Abs("internal/music/wrapper.py")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve wrapper path: %w", err)
	}
	
	if _, err := os.Stat(wrapperPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("wrapper.py not found at %s", wrapperPath)
	}

	return &Client{
		apiKey:      apiKey,
		wrapperPath: wrapperPath,
		logger:      log,
	}, nil
}

// GenerateMusic generates a music track from the prompt using Lyria RealTime (via Python Wrapper).
func (c *Client) GenerateMusic(ctx context.Context, prompt string, durationSeconds int) ([]byte, error) {
	c.logger.Infof("Generating music via wrapper. Prompt: %s, Duration: %ds", prompt, durationSeconds)

	// Temp Output File
	tmpFile, err := os.CreateTemp("", "lyria_music_*.wav")
	if err != nil {
		return nil, fmt.Errorf("create temp file failed: %w", err)
	}
	outputFile := tmpFile.Name()
	tmpFile.Close() // Close immediately so python can write to it
	defer os.Remove(outputFile)

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