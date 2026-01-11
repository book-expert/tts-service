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
	serviceLogger *logger.Logger,
	concurrency int,
) *Processor {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Processor{
		speechClient:      speechClient,
		audioMixer:        audioMixer,
		logger:            serviceLogger,
		speechConcurrency: concurrency,
	}
}

// Process converts text to speech (48kHz WAV).
// It implements the "Safe Stitch Protocol":
// 1. Split Text into logical chunks.
// 2. Generate discrete WAVs for each chunk (to avoid header corruption).
// 3. Use FFmpeg Concat Demuxer to stitch them safely.
// 4. Clean up combined audio and append uniform silence.
func (processor *Processor) Process(requestContext context.Context, text []byte, config core.TTSConfig) ([]byte, error) {
	textString := string(text)
	if textString == "" {
		return nil, fmt.Errorf("empty text input")
	}

	processor.logger.Infof("Processor: Starting text processing. SessionID=%s, VoiceID=%s", config.SessionID, config.VoiceID)

	// 1. Split Text
	chunks := SplitText(textString)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no text chunks found after splitting")
	}

	// 2. Generate Speech per Chunk (Safe Stitch)
	var chunkFiles []string

	// Ensure we cleanup all intermediate chunks even if we error out midway
	defer func() {
		for _, file := range chunkFiles {
			// We ignore errors here as the file might have been moved or already deleted
			_ = os.Remove(file)
		}
	}()

	for index, chunk := range chunks {
		processor.logger.Infof("Processor: Generating chunk %d/%d (%d chars)", index+1, len(chunks), len(chunk.Text))

		startTime := time.Now()

		// Request specific chunk only (Single string in slice)
		stream, generationError := processor.speechClient.GenerateSpeech(requestContext, []string{chunk.Text}, config.VoiceID, "")
		if generationError != nil {
			return nil, fmt.Errorf("chunk %d generation failed: %w", index, generationError)
		}

		// Save stream to temp file immediately
		temporaryFile, creationError := os.CreateTemp("", fmt.Sprintf("chunk_%d_*.wav", index))
		if creationError != nil {
			_ = stream.Close()
			return nil, fmt.Errorf("create temp file for chunk %d failed: %w", index, creationError)
		}

		// Copy stream to file
		bytesWritten, copyError := io.Copy(temporaryFile, stream)
		_ = stream.Close()        // Close network stream
		_ = temporaryFile.Close() // Close file handle to flush buffer

		if copyError != nil {
			return nil, fmt.Errorf("write chunk %d failed: %w", index, copyError)
		}

		if bytesWritten == 0 {
			processor.logger.Warnf("Chunk %d resulted in 0 bytes audio", index)
			_ = os.Remove(temporaryFile.Name())
			continue
		}

		processor.logger.Infof("Chunk %d generated in %v (Size: %d bytes)", index+1, time.Since(startTime), bytesWritten)
		chunkFiles = append(chunkFiles, temporaryFile.Name())
	}

	if len(chunkFiles) == 0 {
		return nil, fmt.Errorf("no audio chunks were successfully generated")
	}

	// 3. Combine Chunks (Raw Content)
	// We do NOT trim here anymore. We rely on the "Sandwich & Press" strategy.
	processor.logger.Infof("Processor: Combining %d chunks into page content...", len(chunkFiles))
	rawContentBytes, combineError := processor.audioMixer.Combine(requestContext, chunkFiles)
	if combineError != nil {
		return nil, fmt.Errorf("combine chunks failed: %w", combineError)
	}

	rawContentPath, writeError := writeTempFile("raw_content_*.wav", rawContentBytes)
	if writeError != nil {
		return nil, fmt.Errorf("failed to write raw content: %w", writeError)
	}
	defer func() { _ = os.Remove(rawContentPath) }()

	// 4. Sandwich Strategy: [1s Silence] + [Content] + [1s Silence]
	silenceWav := GenerateSilentWav(1*time.Second, 48000, 1, 32)
	silencePath, silenceCreationError := writeTempFile("silence_pad_*.wav", silenceWav)
	if silenceCreationError != nil {
		return nil, fmt.Errorf("failed to create silence file: %w", silenceCreationError)
	}
	defer func() { _ = os.Remove(silencePath) }()

	processor.logger.Infof("Processor: Applying 1s padding to start and end of page...")
	finalBytes, finalCombineError := processor.audioMixer.Combine(requestContext, []string{silencePath, rawContentPath, silencePath})
	if finalCombineError != nil {
		return nil, fmt.Errorf("final combine failed: %w", finalCombineError)
	}

	processor.logger.Infof("Processor: Successfully generated padded page audio (%d bytes)", len(finalBytes))
	return finalBytes, nil
}

// Helper to write bytes to a temp file and return the path.
func writeTempFile(pattern string, data []byte) (string, error) {
	temporaryFile, creationError := os.CreateTemp("", pattern)
	if creationError != nil {
		return "", fmt.Errorf("create temp file %s failed: %w", pattern, creationError)
	}

	if _, writeError := temporaryFile.Write(data); writeError != nil {
		_ = temporaryFile.Close()
		_ = os.Remove(temporaryFile.Name())
		return "", fmt.Errorf("write to temp file %s failed: %w", pattern, writeError)
	}

	if closeError := temporaryFile.Close(); closeError != nil {
		_ = os.Remove(temporaryFile.Name())
		return "", fmt.Errorf("close temp file %s failed: %w", pattern, closeError)
	}

	return temporaryFile.Name(), nil
}
