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
	"strings"

	"github.com/book-expert/logger"
)

type Mixer struct {
	logger *logger.Logger
}

func NewMixer(log *logger.Logger) *Mixer {
	return &Mixer{logger: log}
}

// Sanitize enforces the "Gold Standard" format (48kHz, 24-bit, Stereo) using SoX.
// This is the "Surgeon" step of the Safe Stitch Protocol.
// It returns the path to the sanitized file. The caller is responsible for cleanup.
func (m *Mixer) Sanitize(ctx context.Context, inputPath string) (string, error) {
	outputPath := filepath.Join(os.TempDir(), fmt.Sprintf("clean_%s.wav", filepath.Base(inputPath)))

	// BKM-01: Sanitize using SoX
	// sox input.wav -r 48000 -c 2 -b 24 output.wav
	cmd := exec.CommandContext(ctx, "sox",
		inputPath,
		"-r", "48000",
		"-c", "2",
		"-b", "24",
		outputPath,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("sox sanitize failed: %w | Output: %s", err, string(out))
	}

	return outputPath, nil
}

// TruncateSilence reduces any silence longer than maxSeconds to exactly maxSeconds.
// It uses FFmpeg's silenceremove filter with stop_periods=-1 (process all silences).
// This ensures "natural" pauses are kept but excessive silence is removed.
func (m *Mixer) TruncateSilence(ctx context.Context, inputPath string, maxSeconds float64) (string, error) {
	outputPath := filepath.Join(os.TempDir(), fmt.Sprintf("truncated_%s.wav", filepath.Base(inputPath)))

	// silenceremove=stop_periods=-1:stop_duration=1:stop_threshold=-50dB:detection=peak
	// stop_periods=-1: Scan the entire stream
	// stop_duration=X: If silence > X, keep X and remove the rest
	// detection=peak: More accurate silence detection
	filter := fmt.Sprintf("silenceremove=stop_periods=-1:stop_duration=%f:stop_threshold=-50dB:detection=peak", maxSeconds)

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", inputPath,
		"-af", filter,
		"-c:a", "pcm_s24le", // High-Res 24-bit output
		"-ar", "48000",
		outputPath,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("ffmpeg truncate silence failed: %w | Output: %s", err, string(out))
	}

	return outputPath, nil
}

// Combine implements the "Safe Stitch" Protocol using FFmpeg Concat Demuxer.
// 1. Sanitizes all inputs first (using SoX).
// 2. Generates an inputs.txt list.
// 3. Concatenates using stream copy (no re-encoding).
func (m *Mixer) Combine(ctx context.Context, inputPaths []string) ([]byte, error) {
	if len(inputPaths) == 0 {
		return nil, fmt.Errorf("no inputs to combine")
	}

	// 1. Sanitize Inputs
	var cleanPaths []string
	defer func() {
		// Cleanup sanitized temp files
		for _, p := range cleanPaths {
			_ = os.Remove(p)
		}
	}()

	for _, p := range inputPaths {
		clean, err := m.Sanitize(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("failed to sanitize input %s: %w", p, err)
		}
		cleanPaths = append(cleanPaths, clean)
	}

	// 2. Generate inputs.txt
	listFile, err := os.CreateTemp("", "concat_list_*.txt")
	if err != nil {
		return nil, fmt.Errorf("failed to create concat list: %w", err)
	}
	defer func() { _ = os.Remove(listFile.Name()) }()

	var listContent strings.Builder
	for _, p := range cleanPaths {
		// Escape single quotes for ffmpeg concat file
		safePath := strings.ReplaceAll(p, "'", "'\\''")
		listContent.WriteString(fmt.Sprintf("file '%s'\n", safePath))
	}
	if _, err := listFile.WriteString(listContent.String()); err != nil {
		return nil, fmt.Errorf("failed to write concat list: %w", err)
	}
	_ = listFile.Close()

	// 3. Concat (Stream Copy)
	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("concat_%s.wav", filepath.Base(cleanPaths[0])))
	defer func() { _ = os.Remove(outputFile) }()

	// ffmpeg -f concat -safe 0 -i inputs.txt -c copy output.wav
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c", "copy",
		outputFile,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("ffmpeg concat failed: %w | Output: %s", err, string(out))
	}

	return os.ReadFile(outputFile)
}

// Mix merges speech and background music using FFmpeg with auto-ducking.
// It enforces 48kHz Stereo for both inputs before mixing.
func (m *Mixer) Mix(ctx context.Context, speechPath, musicPath string) ([]byte, error) {
	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("mixed_%s.wav", filepath.Base(speechPath)))
	defer func() { _ = os.Remove(outputFile) }()

	// FFmpeg Filter Complex (BKM from Manual):
	// [0:a] Speech -> Force Stereo/48k -> Volume 1.5 -> [s]
	// [1:a] Music  -> Force Stereo/48k -> Volume 0.2 -> [m]
	// [s][m] amix  -> Mix (Duration=First), No Dropout Pump -> Output
	filterComplex := `[0:a]aformat=sample_rates=48000:channel_layouts=stereo,volume=1.5[s];[1:a]aformat=sample_rates=48000:channel_layouts=stereo,volume=0.2[m];[s][m]amix=inputs=2:duration=first:dropout_transition=0`

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", speechPath,
		"-stream_loop", "-1", "-i", musicPath,
		"-filter_complex", filterComplex,
		"-c:a", "pcm_s24le", // High-Res 24-bit output
		"-ar", "48000",
		outputFile,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		logMsg := string(out)
		if len(logMsg) > 1000 {
			logMsg = "..." + logMsg[len(logMsg)-1000:]
		}
		return nil, fmt.Errorf("ffmpeg mix failed: %w | Output: %s", err, logMsg)
	}

	return os.ReadFile(outputFile)
}
