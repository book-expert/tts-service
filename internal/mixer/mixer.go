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

// Mix merges speech and background music into a final wav.
// It loops the music track to match speech duration and ensures channel compatibility.
func (m *Mixer) Mix(ctx context.Context, speechPath, musicPath string) ([]byte, error) {
	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("mixed_%s.wav", filepath.Base(speechPath)))
	defer func() {
		// Clean up the output file on error, or rely on caller to manage the bytes.
		// If we successfully read it into memory, we can delete it here.
		if err := os.Remove(outputFile); err != nil && !os.IsNotExist(err) {
			m.logger.Warnf("failed to remove temp file %s: %v", outputFile, err)
		}
	}()

	// FFmpeg Filter Complex Breakdown:
	// 1. [0:a] (Speech): Force Stereo, 48kHz, Boost Volume -> [s]
	// 2. [1:a] (Music):  Force Stereo, 48kHz, Lower Volume -> [m]
	// 3. [s][m] amix:    Mix them, end when speech ends (duration=first).
	//
	// Why 'aformat'? Mixing Mono speech with Stereo music often crashes ffmpeg
	// or produces silent channels without explicit layout definitions.
	filterComplex := `[0:a]aformat=sample_rates=48000:channel_layouts=stereo,volume=1.5[s];[1:a]aformat=sample_rates=48000:channel_layouts=stereo,volume=0.2[m];[s][m]amix=inputs=2:duration=first:dropout_transition=2`

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", speechPath,
		"-stream_loop", "-1", "-i", musicPath,
		"-filter_complex", filterComplex,
		"-c:a", "pcm_s16le",
		"-ar", "48000",
		outputFile,
	)

	// Capture combined output for debugging "With Love"
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Optimization: Return the actual ffmpeg error output in the error message
		// so the developer doesn't have to hunt for logs.
		// We truncate it to 500 chars to avoid blowing up logs, but keep the tail (where the error usually is).
		logMsg := string(output)
		if len(logMsg) > 500 {
			logMsg = "..." + logMsg[len(logMsg)-500:]
		}
		
		return nil, fmt.Errorf("ffmpeg mix failed: %w | Output: %s", err, logMsg)
	}

	return os.ReadFile(outputFile)
}

