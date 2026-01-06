/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS, RELIABILITY, CONSISTENCY
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
6.  RELIABILITY: We act as guardians of the work. We value safety over speed.
    We do not break what works to fix what isn't broken.
7.  CONSISTENCY: We uphold standards even when no one is watching.
    Predictability is the soil in which trust grows.

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
-   Reliability = Safety: we protect the user's investment; stability is paramount.
-   Consistency = Integrity: uniform quality reduces cognitive load and fear.
--------------------------------------------------------------------------------
*/

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
		_ = stream.Close() // Close network stream
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

// MixAudio combines speech and music using the local Mixer (FFmpeg).
func (p *Processor) MixAudio(ctx context.Context, speechData, musicData []byte) ([]byte, error) {
	p.logger.Infof("Processor: Mixing speech (%d bytes) and music (%d bytes)...", len(speechData), len(musicData))

	// Write buffers to temp files for FFmpeg
	speechFile, err := writeTempFile("mix_speech_*.wav", speechData)
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(speechFile) }()

	musicFile, err := writeTempFile("mix_music_*.wav", musicData)
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(musicFile) }()

	// Delegate to Mixer
	return p.audioMixer.Mix(ctx, speechFile, musicFile)
}

// CombineAudio merges multiple audio files into a single continuous wav file using the Mixer.
// It also enforces the "Press" strategy: Truncating any silence > 1s to exactly 1s.
func (p *Processor) CombineAudio(ctx context.Context, inputPaths []string) ([]byte, error) {
	p.logger.Infof("Processor: Combining %d audio files...", len(inputPaths))
	
	// 1. Concatenate (Draft)
	draftBytes, err := p.audioMixer.Combine(ctx, inputPaths)
	if err != nil {
		return nil, fmt.Errorf("initial concat failed: %w", err)
	}

	// 2. Write Draft to Temp
	draftPath, err := writeTempFile("draft_combine_*.wav", draftBytes)
	if err != nil {
		return nil, fmt.Errorf("write draft failed: %w", err)
	}
	defer func() { _ = os.Remove(draftPath) }()

	// 3. Clean (Truncate Silence > 1s)
	p.logger.Infof("Processor: Cleaning silence (Truncate > 1.0s)...")
	cleanPath, err := p.audioMixer.TruncateSilence(ctx, draftPath, 1.0)
	if err != nil {
		return nil, fmt.Errorf("silence truncation failed: %w", err)
	}
	defer func() { _ = os.Remove(cleanPath) }()

	// 4. Return Cleaned Bytes
	return os.ReadFile(cleanPath)
}

// FinalizeAudio is a fallback method.
func (p *Processor) FinalizeAudio(ctx context.Context, speechData []byte, musicPrompt string) ([]byte, error) {
	p.logger.Warnf("Processor: FinalizeAudio called (Fallback). Returning speech only (No Music).")
	return speechData, nil
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
