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
type Processor struct {
	speechClient      *SpeechClient
	audioStitcher     *Stitcher
	serviceLogger     *logger.Logger
	speechConcurrency int
}

// NewProcessor creates a new TTS Processor with all required dependencies.
func NewProcessor(
	speechClient *SpeechClient,
	audioStitcher *Stitcher,
	serviceLogger *logger.Logger,
	concurrency int,
) *Processor {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Processor{
		speechClient:      speechClient,
		audioStitcher:     audioStitcher,
		serviceLogger:     serviceLogger,
		speechConcurrency: concurrency,
	}
}

// Process converts text to speech (48kHz WAV) using a safe stitching protocol.
func (processor *Processor) Process(requestContext context.Context, text []byte, configuration core.TTSConfig) ([]byte, error) {
	textString := string(text)
	if textString == "" {
		return nil, fmt.Errorf("empty text input")
	}

	processor.serviceLogger.Infof("Processor: Starting text processing. SessionIdentifier=%s, VoiceIdentifier=%s", configuration.SessionIdentifier, configuration.VoiceIdentifier)

	chunks := SplitText(textString)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no text chunks found after splitting")
	}

	var chunkFiles []string
	defer func() {
		for _, file := range chunkFiles {
			_ = os.Remove(file)
		}
	}()

	for index, chunk := range chunks {
		processor.serviceLogger.Infof("Processor: Generating chunk %d/%d (%d chars)", index+1, len(chunks), len(chunk.Text))

		startTime := time.Now()

		stream, generationError := processor.speechClient.GenerateSpeech(requestContext, []string{chunk.Text}, configuration.VoiceIdentifier, "")
		if generationError != nil {
			return nil, fmt.Errorf("chunk %d generation failed: %w", index, generationError)
		}

		temporaryFile, creationError := os.CreateTemp("", fmt.Sprintf("chunk_%d_*.wav", index))
		if creationError != nil {
			_ = stream.Close()
			return nil, fmt.Errorf("create temp file for chunk %d failed: %w", index, creationError)
		}

		bytesWritten, copyError := io.Copy(temporaryFile, stream)
		_ = stream.Close()
		_ = temporaryFile.Close()

		if copyError != nil {
			return nil, fmt.Errorf("write chunk %d failed: %w", index, copyError)
		}

		if bytesWritten == 0 {
			processor.serviceLogger.Warnf("Chunk %d resulted in 0 bytes audio", index)
			_ = os.Remove(temporaryFile.Name())
			continue
		}

		processor.serviceLogger.Infof("Chunk %d generated in %v (Size: %d bytes)", index+1, time.Since(startTime), bytesWritten)
		chunkFiles = append(chunkFiles, temporaryFile.Name())
	}

	if len(chunkFiles) == 0 {
		return nil, fmt.Errorf("no audio chunks were successfully generated")
	}

	processor.serviceLogger.Infof("Processor: Stitching %d chunks into page content...", len(chunkFiles))
	rawContentBytes, stitchError := processor.audioStitcher.Stitch(requestContext, chunkFiles)
	if stitchError != nil {
		return nil, fmt.Errorf("stitch chunks failed: %w", stitchError)
	}

	rawContentPath, writeError := writeTempFile("raw_content_*.wav", rawContentBytes)
	if writeError != nil {
		return nil, fmt.Errorf("failed to write raw content: %w", writeError)
	}
	defer func() { _ = os.Remove(rawContentPath) }()

	// Sandwich Strategy: [1s Silence] + [Content] + [1s Silence]
	silenceWav := GenerateSilentWav(1*time.Second, 48000, 1, 32)
	silencePath, silenceCreationError := writeTempFile("silence_pad_*.wav", silenceWav)
	if silenceCreationError != nil {
		return nil, fmt.Errorf("failed to create silence file: %w", silenceCreationError)
	}
	defer func() { _ = os.Remove(silencePath) }()

	processor.serviceLogger.Infof("Processor: Applying 1s padding to start and end of page...")
	finalBytes, finalStitchError := processor.audioStitcher.Stitch(requestContext, []string{silencePath, rawContentPath, silencePath})
	if finalStitchError != nil {
		return nil, fmt.Errorf("final stitch failed: %w", finalStitchError)
	}

	processor.serviceLogger.Infof("Processor: Successfully generated padded page audio (%d bytes)", len(finalBytes))
	return finalBytes, nil
}

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
