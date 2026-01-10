/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package audio

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/core"
)

// Processor implements core.TTSProcessor.
// It coordinates:
// 1. Text -> Speech (via Audio Client/Audio Server)
// 2. Speech + Music -> Mixed Audio (via Mixer/FFmpeg)
type Processor struct {
	speechClient      *SpeechClient
	audioMixer        *Mixer
	logger            *logger.Logger
	speechConcurrency int
}

// NewProcessor creates a new TTS Processor with all required dependencies.
func NewProcessor(
	speechClient *SpeechClient,
	audioMixer *Mixer,
	log *logger.Logger,
	concurrency int,
) *Processor {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Processor{
		speechClient:      speechClient,
		audioMixer:        audioMixer,
		logger:            log,
		speechConcurrency: concurrency,
	}
}

// Process converts text to speech (48kHz WAV).
// It implements the "Safe Stitch Protocol":
// 1. Split Text into logical chunks.
// 2. Generate discrete WAVs for each chunk (to avoid header corruption).
// 3. Use FFmpeg Concat Demuxer to stitch them safely.
// 4. Clean up combined audio and append uniform silence.
func (p *Processor) Process(ctx context.Context, text []byte, config core.TTSConfig) ([]byte, error) {
	textStr := string(text)
	if textStr == "" {
		return nil, fmt.Errorf("empty text input")
	}

	p.logger.Infof("Processor: Starting text processing. SessionID=%s, VoiceID=%s", config.SessionID, config.VoiceID)

	// 1. Split Text
	chunks := SplitText(textStr)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no text chunks found after splitting")
	}

	// 2. Generate Speech per Chunk (Safe Stitch)
	var chunkFiles []string

	// Ensure we cleanup all intermediate chunks even if we error out midway
	defer func() {
		for _, f := range chunkFiles {
			// We ignore errors here as the file might have been moved or already deleted
			_ = os.Remove(f)
		}
	}()

	for i, c := range chunks {
		p.logger.Infof("Processor: Generating chunk %d/%d (%d chars)", i+1, len(chunks), len(c.Text))

		start := time.Now()

		// Request specific chunk only (Single string in slice)
		stream, err := p.speechClient.GenerateSpeech(ctx, []string{c.Text}, config.VoiceID, "")
		if err != nil {
			return nil, fmt.Errorf("chunk %d generation failed: %w", i, err)
		}

		// Save stream to temp file immediately
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("chunk_%d_*.wav", i))
		if err != nil {
			_ = stream.Close()
			return nil, fmt.Errorf("create temp file for chunk %d failed: %w", i, err)
		}

		// Copy stream to file
		written, copyErr := io.Copy(tmpFile, stream)
		_ = stream.Close()  // Close network stream
		_ = tmpFile.Close() // Close file handle to flush buffer

		if copyErr != nil {
			return nil, fmt.Errorf("write chunk %d failed: %w", i, copyErr)
		}

		if written == 0 {
			p.logger.Warnf("Chunk %d resulted in 0 bytes audio", i)
			_ = os.Remove(tmpFile.Name())
			continue
		}

		p.logger.Infof("Chunk %d generated in %v (Size: %d bytes)", i+1, time.Since(start), written)
		chunkFiles = append(chunkFiles, tmpFile.Name())
	}

	if len(chunkFiles) == 0 {
		return nil, fmt.Errorf("no audio chunks were successfully generated")
	}

	// 3. Combine Chunks (Raw Content)
	// We do NOT trim here anymore. We rely on the "Sandwich & Press" strategy.
	p.logger.Infof("Processor: Combining %d chunks into page content...", len(chunkFiles))
	rawContentBytes, err := p.audioMixer.Combine(ctx, chunkFiles)
	if err != nil {
		return nil, fmt.Errorf("combine chunks failed: %w", err)
	}

	rawContentPath, err := writeTempFile("raw_content_*.wav", rawContentBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to write raw content: %w", err)
	}
	defer func() { _ = os.Remove(rawContentPath) }()

	// 4. Sandwich Strategy: [1s Silence] + [Content] + [1s Silence]
	silenceWav := GenerateSilentWav(1*time.Second, 48000, 1, 32)
	silencePath, err := writeTempFile("silence_pad_*.wav", silenceWav)
	if err != nil {
		return nil, fmt.Errorf("failed to create silence file: %w", err)
	}
	defer func() { _ = os.Remove(silencePath) }()

	p.logger.Infof("Processor: Applying 1s padding to start and end of page...")
	finalBytes, err := p.audioMixer.Combine(ctx, []string{silencePath, rawContentPath, silencePath})
	if err != nil {
		return nil, fmt.Errorf("final combine failed: %w", err)
	}

	p.logger.Infof("Processor: Successfully generated padded page audio (%d bytes)", len(finalBytes))
	return finalBytes, nil
}

// Helper to write bytes to a temp file and return the path.
func writeTempFile(pattern string, data []byte) (string, error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", fmt.Errorf("create temp file %s failed: %w", pattern, err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("write to temp file %s failed: %w", pattern, err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("close temp file %s failed: %w", pattern, err)
	}

	return f.Name(), nil
}
