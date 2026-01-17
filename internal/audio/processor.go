/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package audio

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/core"
)

// Processor implements core.TTSProcessor for high-fidelity audio generation.
type Processor struct {
	speechClient      *SpeechClient
	audioStitcher     *Stitcher
	serviceLogger     *logger.Logger
	speechConcurrency int
}

// NewProcessor creates a new high-integrity TTS Processor.
func NewProcessor(
	speechClient *SpeechClient,
	audioStitcher *Stitcher,
	serviceLogger *logger.Logger,
	speechConcurrency int,
) *Processor {
	if speechConcurrency <= 0 {
		speechConcurrency = 1
	}
	return &Processor{
		speechClient:      speechClient,
		audioStitcher:     audioStitcher,
		serviceLogger:     serviceLogger,
		speechConcurrency: speechConcurrency,
	}
}

// Process converts text to speech using a zero-disk in-memory pipeline for chunks.
func (processor *Processor) Process(requestContext context.Context, textContent []byte, configuration core.TextToSpeechConfiguration) ([]byte, error) {
	textString := string(textContent)
	if textString == "" {
		return nil, fmt.Errorf("empty text input provided for generation")
	}

	processor.serviceLogger.Infof("Processor: Starting high-fidelity generation. Session=%s, Voice=%s", configuration.SessionIdentifier, configuration.VoiceIdentifier)

	textChunks := SplitText(textString)
	if len(textChunks) == 0 {
		return nil, fmt.Errorf("no text chunks identified after segmentation")
	}

	// In-Memory Chunk Pipeline (Zero Disk)
	var generatedAudioChunks [][]byte

	for index, chunk := range textChunks {
		processor.serviceLogger.Infof("Processor: Generating chunk %d/%d (%d chars)", index+1, len(textChunks), len(chunk.Text))

		startTime := time.Now()

		audioStream, generationError := processor.speechClient.GenerateSpeech(requestContext, []string{chunk.Text}, configuration.VoiceIdentifier, "")
		if generationError != nil {
			return nil, fmt.Errorf("chunk %d generation failed at source: %w", index, generationError)
		}

		chunkByteData, readError := io.ReadAll(audioStream)
		_ = audioStream.Close()

		if readError != nil {
			return nil, fmt.Errorf("failed to read chunk %d stream: %w", index, readError)
		}

		if len(chunkByteData) == 0 {
			processor.serviceLogger.Warnf("Chunk %d resulted in empty audio payload", index)
			continue
		}

		processor.serviceLogger.Infof("Chunk %d generated in %v (Payload: %d bytes)", index+1, time.Since(startTime), len(chunkByteData))
		generatedAudioChunks = append(generatedAudioChunks, chunkByteData)
	}

	if len(generatedAudioChunks) == 0 {
		return nil, fmt.Errorf("zero valid audio chunks generated for page")
	}

	processor.serviceLogger.Infof("Processor: Stitching %d chunks into page artifact using Shared Memory...", len(generatedAudioChunks))
	
	// Create Padding Chunks in Memory
	sampleRateHz := 44100 // Production Standard for Generation
	silenceDuration := 1 * time.Second
	silencePaddingChunk := GenerateSilentWav(silenceDuration, sampleRateHz, 1, 32)

	// Final Assembler Pipeline: [Silence] + [Chunks...] + [Silence]
	var finalAssemblySequence [][]byte
	finalAssemblySequence = append(finalAssemblySequence, silencePaddingChunk)
	finalAssemblySequence = append(finalAssemblySequence, generatedAudioChunks...)
	finalAssemblySequence = append(finalAssemblySequence, silencePaddingChunk)

	finalPageArtifact, stitchError := processor.audioStitcher.Stitch(requestContext, finalAssemblySequence)
	if stitchError != nil {
		return nil, fmt.Errorf("shared memory stitch operation failed: %w", stitchError)
	}

	processor.serviceLogger.Infof("Processor: Successfully generated clean page artifact (%d bytes)", len(finalPageArtifact))
	return finalPageArtifact, nil
}
