/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
1.  LOVE AND CARE
    - Robust orchestration of Audio, Music, and Mixing.
    - Clear variable naming.

2.  SIMPLE IS EFFICIENT
    - Direct delegation to specialized clients.
    - Local mixing via FFmpeg.
*/

package orchestrator

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/mixer"
	"github.com/book-expert/tts-service/internal/music"
	"github.com/book-expert/tts-service/internal/tts"
)

// Processor implements core.TTSProcessor.
// It coordinates:
// 1. Text -> Speech (via Audio Client/Audio Server)
// 2. Prompt -> Music (via Music Client/Lyria)
// 3. Speech + Music -> Mixed Audio (via Mixer/FFmpeg)
type Processor struct {
	audioClient       *audio.Client
	musicClient       *music.Client
	audioMixer        *mixer.Mixer
	logger            *logger.Logger
	speechConcurrency int
}

// New creates a new TTS Processor with all required dependencies.
func New(
	audioClient *audio.Client,
	musicClient *music.Client,
	audioMixer *mixer.Mixer,
	log *logger.Logger,
	concurrency int,
) *Processor {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Processor{
		audioClient:       audioClient,
		musicClient:       musicClient,
		audioMixer:        audioMixer,
		logger:            log,
		speechConcurrency: concurrency,
	}
}

// Process converts text to speech (48kHz WAV).
// It manages chunking, parallel processing (if enabled), and cleanup.
func (p *Processor) Process(ctx context.Context, text []byte, config core.TTSConfig) ([]byte, error) {
	textStr := string(text)
	if textStr == "" {
		return nil, fmt.Errorf("empty text input")
	}

	p.logger.Infof("Processor: Starting text processing. SessionID=%s, VoiceID=%s", config.SessionID, config.VoiceID)

	// 1. Split Text
	chunks := tts.SplitText(textStr)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no text chunks found after splitting")
	}

	// 2. Prepare for Generation
	var chunkStrings []string
	for _, c := range chunks {
		chunkStrings = append(chunkStrings, c.Text)
	}

	// 3. Generate Speech (Blocking/Streaming from Audio Server)
	// We use the audio client to stream the result directly.
	// Since audio-server now handles the concatenation of chunks internally,
	// we just receive a single stream for the whole page.
	
	start := time.Now()
	stream, err := p.audioClient.GenerateSpeech(ctx, chunkStrings, config.VoiceID, "") // No reference text prompt needed
	if err != nil {
		return nil, fmt.Errorf("audio-server speech generation failed: %w", err)
	}
	defer func() { _ = stream.Close() }()

	// 4. Save Stream to Temp File for conversion/verification
	tmpFile, err := os.CreateTemp("", "speech_raw_*.wav")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tempPath)
	}()

	if _, err := io.Copy(tmpFile, stream); err != nil {
		return nil, fmt.Errorf("failed to write speech stream to temp file: %w", err)
	}
	
	fi, _ := tmpFile.Stat()
	p.logger.Infof("Processor: Speech generated in %v. Size: %d bytes", time.Since(start), fi.Size())

	// 5. Ensure 48kHz (Standardization)
	return p.audioMixer.ConvertTo48k(ctx, tempPath)
}

// GenerateMusic calls the Music Client (Lyria RealTime via Wrapper) to generate a background track.
func (p *Processor) GenerateMusic(ctx context.Context, prompt string, duration int) ([]byte, error) {
	p.logger.Infof("Processor: Generating music with prompt: '%s', Duration: %ds", prompt, duration)
	return p.musicClient.GenerateMusic(ctx, prompt, duration)
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
func (p *Processor) CombineAudio(ctx context.Context, inputPaths []string) ([]byte, error) {
	p.logger.Infof("Processor: Combining %d audio files...", len(inputPaths))
	outputFile, err := p.audioMixer.Combine(ctx, inputPaths)
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(outputFile) }()

	return os.ReadFile(outputFile)
}

// FinalizeAudio is a fallback method.
// Ideally, MixAudio is used. If this is called, it implies music generation failed or was skipped.
// We just return the speech data directly.
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
	
	// We close explicitly after write, but defer strictly for safety if write panics/errors early?
	// The pattern is: write, then close.
	
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

// WaitGroup wrapper not strictly needed here as we use blocking calls now for simplicity (Golden Rule: Simple is Efficient).
// The parallelization happens at the Service Level (Worker processing Page 1 Music + Page 1 Speech in parallel goroutines).