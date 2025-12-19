package orchestrator

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/book-expert/logger"
	"github.com/book-expert/tts-service/internal/audio"
	"github.com/book-expert/tts-service/internal/core"
	"github.com/book-expert/tts-service/internal/mixer"
	"github.com/book-expert/tts-service/internal/tts"
)

// MaxSpeechConcurrency defines the maximum number of concurrent speech generation requests per page.
// Set to 3 to improve throughput on GPU-enabled workers.
const MaxSpeechConcurrency = 3

type Processor struct {
	audioClient *audio.Client
	mixer       *mixer.Mixer
	logger      *logger.Logger

	// Music Suite Cache: SessionID -> []FilePaths
	musicCache map[string][]string
	cacheMu    sync.RWMutex
}

func New(client *audio.Client, mix *mixer.Mixer, log *logger.Logger) *Processor {
	return &Processor{
		audioClient: client,
		mixer:       mix,
		logger:      log,
		musicCache:  make(map[string][]string),
	}
}

func (p *Processor) Process(ctx context.Context, text []byte, config core.TTSConfig) ([]byte, error) {
	textStr := string(text)
	if textStr == "" {
		return nil, fmt.Errorf("empty text")
	}

	p.logger.Infof("Processing Text. SessionID: %s, VoiceID: %s, MusicPrompt: '%s'", config.SessionID, config.VoiceID, config.MusicPrompt)

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
		musicPath       string
		speechErr       error
		musicErr        error
		wg              sync.WaitGroup
	)

	// 1. Generate Speech (Async - Chunked)
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

		type chunkResult struct {
			index int
			path  string
			err   error
		}

		chunkResults := make([]string, len(chunks))
		resultChan := make(chan chunkResult, len(chunks))
		var chunkWg sync.WaitGroup

		sem := make(chan struct{}, MaxSpeechConcurrency)

		for _, chunk := range chunks {
			chunkWg.Add(1)
			sem <- struct{}{} // Acquire token

			go func(c tts.TextChunk) {
				defer chunkWg.Done()
				defer func() { <-sem }() // Release token

				// Pass the VoiceID and VoiceStyle directly to the client.
				voiceID := config.VoiceID

				start := time.Now()
				path, err := p.audioClient.GenerateSpeech(ctx, c.Text, voiceID, promptText)
				duration := time.Since(start)

				if err == nil {
					addTempFile(path)
					p.logger.Infof("Chunk %d processed in %v", c.ID, duration)
				} else {
					p.logger.Errorf("Chunk %d failed after %v: %v", c.ID, duration, err)
				}
				resultChan <- chunkResult{index: c.ID, path: path, err: err}
			}(chunk)
		}

		chunkWg.Wait()
		close(resultChan)

		for res := range resultChan {
			if res.err != nil {
				if speechErr == nil {
					speechErr = fmt.Errorf("chunk %d failed: %w", res.index, res.err)
				}
				continue
			}
			chunkResults[res.index] = res.path
		}

		if speechErr != nil {
			return
		}

		if len(chunkResults) == 1 {
			finalSpeechPath = chunkResults[0]
		} else {
			mergedPath, err := p.mixer.Concatenate(ctx, chunkResults)
			if err != nil {
				speechErr = fmt.Errorf("concatenation failed: %w", err)
				return
			}
			addTempFile(mergedPath)
			finalSpeechPath = mergedPath
		}
	}()

	// 2. Handle Music (Async)
	if config.MusicPrompt != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			musicPath, musicErr = p.getMusicTrack(ctx, config.SessionID, config.MusicPrompt)
		}()
	}

	wg.Wait()

	// Check errors
	if speechErr != nil {
		return nil, fmt.Errorf("speech gen failed: %w", speechErr)
	}
	if musicErr != nil {
		p.logger.Warnf("Failed to get music suite, falling back to dry speech: %v", musicErr)
	}

	// 3. Mix
	if musicPath == "" {
		return p.mixer.ConvertTo48k(ctx, finalSpeechPath)
	}

	return p.mixer.Mix(ctx, finalSpeechPath, musicPath)
}

func (p *Processor) getMusicTrack(ctx context.Context, sessionID, prompt string) (string, error) {
	// Check Cache
	p.cacheMu.RLock()
	files, ok := p.musicCache[sessionID]
	p.cacheMu.RUnlock()

	if ok && len(files) > 0 {
		// Pick random
		return files[rand.Intn(len(files))], nil
	}

	// Generate Suite
	// Lock for writing to prevent double-generation
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()

	// Double check
	if files, ok = p.musicCache[sessionID]; ok && len(files) > 0 {
		return files[rand.Intn(len(files))], nil
	}

	p.logger.Infof("Generating new Music Suite for Session %s...", sessionID)
	files, err := p.audioClient.GenerateMusicSuite(ctx, prompt)
	if err != nil {
		return "", err
	}

	p.musicCache[sessionID] = files
	if len(files) == 0 {
		return "", fmt.Errorf("no music files generated")
	}

	return files[0], nil
}
