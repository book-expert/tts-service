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

package orchestrator

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/mixer"
	"github.com/book-expert/tts-service/internal/tts"
)

type Processor struct {
	audioClient       *audio.Client
	mixer             *mixer.Mixer
	logger            *logger.Logger
	speechConcurrency int
}

func New(client *audio.Client, mix *mixer.Mixer, log *logger.Logger, concurrency int) *Processor {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Processor{
		audioClient:       client,
		mixer:             mix,
		logger:            log,
		speechConcurrency: concurrency,
	}
}

func (p *Processor) Process(ctx context.Context, text []byte, config core.TTSConfig) ([]byte, error) {
	textStr := string(text)
	if textStr == "" {
		return nil, fmt.Errorf("empty text")
	}

	p.logger.Infof("Processing Text. SessionID: %s, VoiceID: %s", config.SessionID, config.VoiceID)

	// Track temp files for cleanup
	var tempFiles []string
	var filesMu sync.Mutex

	// VoiceStyle is not used as prompt_text for VoxCPM, as prompt_text must be the transcript of the reference audio.
	// We rely on the reference audio itself for style cloning.
	promptText := ""

	addTempFile := func(path string) {
		if path == "" {
			return
		}
		filesMu.Lock()
		tempFiles = append(tempFiles, path)
		filesMu.Unlock()
	}

	defer func() {
		for _, path := range tempFiles {
			_ = os.Remove(path)
		}
	}()

	var (
		finalSpeechPath string
		speechErr       error
		wg              sync.WaitGroup
	)

	// 1. Generate Speech (Async - Full Page)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Use Smart Splitter
		chunks := tts.SplitText(textStr)
		p.logger.Infof("Split text into %d chunks for processing.", len(chunks))

		if len(chunks) == 0 {
			speechErr = fmt.Errorf("no text chunks to process")
			return
		}

		var chunkStrings []string
		for _, c := range chunks {
			chunkStrings = append(chunkStrings, c.Text)
		}

		// Pass the VoiceID directly to the client.
		voiceID := config.VoiceID

		start := time.Now()
		stream, err := p.audioClient.GenerateSpeech(ctx, chunkStrings, voiceID, promptText)
		if err != nil {
			speechErr = fmt.Errorf("generate speech failed: %w", err)
			return
		}
		defer func() { _ = stream.Close() }()

		// Stream to temp file
		tmpFile, err := os.CreateTemp("", "tts_page_*.wav")
		if err != nil {
			speechErr = fmt.Errorf("create temp file failed: %w", err)
			return
		}

		if _, err := io.Copy(tmpFile, stream); err != nil {
			_ = tmpFile.Close()
			speechErr = fmt.Errorf("stream copy failed: %w", err)
			return
		}
		finalSpeechPath = tmpFile.Name()
		if err := tmpFile.Close(); err != nil {
			p.logger.Warnf("failed to close temp file %s: %v", finalSpeechPath, err)
		}

		addTempFile(finalSpeechPath)
		p.logger.Infof("Page processed in %v", time.Since(start))
	}()

	wg.Wait()

	// Check errors
	if speechErr != nil {
		return nil, fmt.Errorf("speech gen failed: %w", speechErr)
	}

	// 2. Convert to Standard 48k (No Mixing)
	return p.mixer.ConvertTo48k(ctx, finalSpeechPath)
}

func (p *Processor) GenerateMusic(ctx context.Context, prompt string, duration int) ([]byte, error) {
	stream, err := p.audioClient.GenerateMusic(ctx, prompt, duration)
	if err != nil {
		return nil, fmt.Errorf("music gen failed: %w", err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			p.logger.Warnf("failed to close music stream: %v", err)
		}
	}()

	return io.ReadAll(stream)
}

func (p *Processor) MixAudio(ctx context.Context, speechData, musicData []byte) ([]byte, error) {
	stream, err := p.audioClient.MixAudio(ctx, speechData, musicData)
	if err != nil {
		return nil, fmt.Errorf("mix audio failed: %w", err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			p.logger.Warnf("failed to close music stream: %v", err)
		}
	}()

	return io.ReadAll(stream)
}

func (p *Processor) FinalizeAudio(ctx context.Context, speechData []byte, musicPrompt string) ([]byte, error) {
	stream, err := p.audioClient.FinalizeAudio(ctx, speechData, musicPrompt)
	if err != nil {
		return nil, fmt.Errorf("finalize audio failed: %w", err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			p.logger.Warnf("failed to close finalize stream: %v", err)
		}
	}()
	return io.ReadAll(stream)
}
