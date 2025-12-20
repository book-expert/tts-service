/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
"Work is love made visible. And if you cannot work with love but only with
distaste, it is better that you should leave your work and sit at the gate of
the temple and take alms of those who work with joy." — Kahlil Gibran

1.  LOVE AND CARE (Primary Driver)
    - This is a craft. Build with pride, honesty, and kindness.
    - If you put love in your work, you build something deserving of love.
    - Be helpful: Code is read more than written; optimize for the reader.

2.  WRITE WHAT YOU MEAN (Explicit > Implicit)
    - Use WHOLE WORDS: `RequestIdentifier` not `ReqID`.
    - No magic numbers: Move application settings to `project.toml`.
    - Secure by design: Keep API keys and secrets strictly in `.env`.
    - No ambiguity: If you assume something, document it.

3.  SIMPLE IS EFFICIENT (Minimal Viable Elegance)
    - Avoid over-engineering. Small interfaces, clear structs.
    - If a design requires a hack, stop. Redesign it with elegance.
    - Lean, Clean, Mean: Delete dead code immediately.

4.  NO BASELESS ASSUMPTIONS (Scientific Rigor)
    - Do not guess. Base decisions on documentation and proven patterns.
    - If you do not know, ask or verify.

5.  NON-BLOCKING & ROBUST
    - Never block the main goroutine. Use Context for cancellation.
    - Handle errors explicitly: Don't just return them, wrap them with context.

--------------------------------------------------------------------------------
EXAMPLES OF "LOVE AND CARE" IN THIS CONTEXT:
--------------------------------------------------------------------------------
(A) NAMING
    Indifferent:  func Gen(t string, v string)
    With Love:    func GenerateSoundscape(ctx context.Context, textPrompt string, voiceID string)
    *Why: The Agent reading this next year will know exactly what it does and that it is cancellable.*

(B) CONFIGURATION
    Indifferent:  const Timeout = 30 // Hardcoded
    With Love:    config.App.TimeoutSeconds // Loaded from project.toml
    *Why: Allows behavior tuning without recompiling or touching the codebase.*

(C) ERROR HANDLING
    Indifferent:  if err != nil { return err }
    With Love:    if err != nil { return fmt.Errorf("failed to initialize vox engine: %w", err) }
    *Why: Wrapping the error gives the user the 'trace of breadcrumbs' they need to fix it. That is kindness.*
--------------------------------------------------------------------------------
*/

package mixer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/book-expert/logger"
)

type Mixer struct {
	logger *logger.Logger
}

func New(log *logger.Logger) *Mixer {
	return &Mixer{logger: log}
}

// Mix merges speech and background music into a final wav.
func (m *Mixer) Mix(ctx context.Context, speechPath, musicPath string) ([]byte, error) {
	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("mixed_%s.wav", filepath.Base(speechPath)))
	defer func() {
		if err := os.Remove(outputFile); err != nil {
			m.logger.Warnf("failed to remove temp file %s: %v", outputFile, err)
		}
	}()

	// FFmpeg Command
	// 1. Speech (Input 0)
	// 2. Music (Input 1) - Looped, Silence Removed
	// Filter:
	// - [0:a] Boost speech volume to 1.5 [speech]
	// - [1:a] Remove silence, set volume to 0.15 [music]
	// - [speech][music] Mix, cut to shortest (speech length)
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", speechPath,
		"-stream_loop", "-1", "-i", musicPath,
		"-filter_complex", `[0:a]volume=1.5[speech];[1:a]silenceremove=start_periods=1:start_duration=0:start_threshold=-50dB:stop_periods=1:stop_duration=0:stop_threshold=-50dB,volume=0.15[music];[speech][music]amix=inputs=2:duration=first:dropout_transition=2`,
		"-c:a", "pcm_s16le",
		"-ar", "48000",
		outputFile,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		m.logger.Errorf("FFmpeg mix failed: %s\nOutput: %s", err, string(output))
		return nil, fmt.Errorf("ffmpeg mix failed: %w", err)
	}

	return os.ReadFile(outputFile)
}

// ConvertTo48k simply ensures the speech is 48kHz if no music is provided.
func (m *Mixer) ConvertTo48k(ctx context.Context, inputPath string) ([]byte, error) {
	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("conv_%s.wav", filepath.Base(inputPath)))
	defer func() {
		if err := os.Remove(outputFile); err != nil {
			m.logger.Warnf("failed to remove temp file %s: %v", outputFile, err)
		}
	}()

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", inputPath,
		"-c:a", "pcm_s16le",
		"-ar", "48000",
		outputFile,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("ffmpeg convert failed: %w, out: %s", err, string(out))
	}

	return os.ReadFile(outputFile)
}

// Concatenate merges multiple WAV files into one.
func (m *Mixer) Concatenate(ctx context.Context, filePaths []string) (string, error) {
	if len(filePaths) == 0 {
		return "", fmt.Errorf("no files to concatenate")
	}
	if len(filePaths) == 1 {
		return filePaths[0], nil
	}

	// Create list file for ffmpeg
	listFile, err := os.CreateTemp("", "concat_list_*.txt")
	if err != nil {
		return "", fmt.Errorf("create temp list: %w", err)
	}
	defer func() {
		if err := os.Remove(listFile.Name()); err != nil {
			m.logger.Warnf("failed to remove temp list file %s: %v", listFile.Name(), err)
		}
	}()

	for _, path := range filePaths {
		// Use absolute path to be safe
		absPath, err := filepath.Abs(path)
		if err != nil {
			_ = listFile.Close()
			return "", fmt.Errorf("get abs path failed for %s: %w", path, err)
		}

		if _, err := fmt.Fprintf(listFile, "file '%s'\n", absPath); err != nil {
			_ = listFile.Close()
			return "", fmt.Errorf("write to list file failed: %w", err)
		}
	}

	if err := listFile.Close(); err != nil {
		return "", fmt.Errorf("close list file failed: %w", err)
	}

	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("merged_%s.wav", filepath.Base(filePaths[0])))

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c", "copy",
		"-y",
		outputFile,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		m.logger.Errorf("FFmpeg concat failed: %s\nOutput: %s", err, string(out))
		return "", fmt.Errorf("ffmpeg concat failed: %w", err)
	}

	return outputFile, nil
}
