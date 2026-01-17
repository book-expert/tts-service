/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package audio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/book-expert/logger"
)

const (
	sharedMemoryWorkspace = "/dev/shm"
)

// Stitcher handles the high-fidelity concatenation of audio chunks using Shared Memory.
type Stitcher struct {
	serviceLogger *logger.Logger
}

// NewStitcher initializes a new high-integrity audio Stitcher.
func NewStitcher(serviceLogger *logger.Logger) *Stitcher {
	return &Stitcher{serviceLogger: serviceLogger}
}

// GenerateSilentWav creates a high-fidelity silent WAV artifact in-memory.
func GenerateSilentWav(duration time.Duration, sampleRate, channels, bitsPerSample int) []byte {
	var buffer bytes.Buffer
	numberSamples := int(duration.Seconds() * float64(sampleRate))
	dataSize := numberSamples * channels * (bitsPerSample / 8)
	fileSize := 36 + dataSize

	buffer.WriteString("RIFF")
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(fileSize))
	buffer.WriteString("WAVE")

	buffer.WriteString("fmt ")
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(16))
	if bitsPerSample == 32 {
		_ = binary.Write(&buffer, binary.LittleEndian, uint16(3)) // PCM Float
	} else {
		_ = binary.Write(&buffer, binary.LittleEndian, uint16(1)) // PCM Integer
	}
	_ = binary.Write(&buffer, binary.LittleEndian, uint16(channels))
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(sampleRate))
	byteRate := sampleRate * channels * (bitsPerSample / 8)
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(byteRate))
	blockAlign := channels * (bitsPerSample / 8)
	_ = binary.Write(&buffer, binary.LittleEndian, uint16(blockAlign))
	_ = binary.Write(&buffer, binary.LittleEndian, uint16(bitsPerSample))

	buffer.WriteString("data")
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(dataSize))
	buffer.Write(make([]byte, dataSize))

	return buffer.Bytes()
}

// Stitch combines multiple audio byte slices into a single WAV artifact using /dev/shm for FFmpeg interop.
func (audioStitcher *Stitcher) Stitch(requestContext context.Context, audioChunks [][]byte) ([]byte, error) {
	if len(audioChunks) == 0 {
		return nil, fmt.Errorf("no audio chunks provided for stitching")
	}

	// 1. Prepare Workspace in Shared Memory
	timestampIdentifier := time.Now().UnixNano()
	workspaceDirectory := filepath.Join(sharedMemoryWorkspace, fmt.Sprintf("stitch_%d", timestampIdentifier))
	if makeDirectoryError := os.MkdirAll(workspaceDirectory, 0755); makeDirectoryError != nil {
		return nil, fmt.Errorf("failed to create shared memory workspace: %w", makeDirectoryError)
	}
	defer func() { _ = os.RemoveAll(workspaceDirectory) }()

	// 2. Write Chunks to Shared Memory
	var chunkPaths []string
	for index, chunkData := range audioChunks {
		chunkPath := filepath.Join(workspaceDirectory, fmt.Sprintf("chunk_%d.wav", index))
		if writeError := os.WriteFile(chunkPath, chunkData, 0644); writeError != nil {
			return nil, fmt.Errorf("failed to write chunk %d to shared memory: %w", index, writeError)
		}
		chunkPaths = append(chunkPaths, chunkPath)
	}

	// 3. Generate FFmpeg Concat List in Shared Memory
	concatListPath := filepath.Join(workspaceDirectory, "chunks.txt")
	var concatListContent strings.Builder
	for _, path := range chunkPaths {
		// Escape single quotes for FFmpeg concat format
		safePath := strings.ReplaceAll(path, "'", "'\\''")
		concatListContent.WriteString(fmt.Sprintf("file '%s'\n", safePath))
	}
	if writeListError := os.WriteFile(concatListPath, []byte(concatListContent.String()), 0644); writeListError != nil {
		return nil, fmt.Errorf("failed to write concat list to shared memory: %w", writeListError)
	}

	// 4. Execute FFmpeg Stitching in Shared Memory
	outputFilePath := filepath.Join(workspaceDirectory, "stitched.wav")
	
	sampleRate := os.Getenv("AUDIO_SAMPLE_RATE_TTS")
	if sampleRate == "" {
		sampleRate = "44100"
	}
	
	// We strictly use pcm_s24le for high-fidelity production standards.
	ffmpegCommand := exec.CommandContext(requestContext, "ffmpeg",
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", concatListPath,
		"-ar", sampleRate,
		"-c:a", "pcm_s24le",
		outputFilePath,
	)

	if terminalOutput, executionError := ffmpegCommand.CombinedOutput(); executionError != nil {
		return nil, fmt.Errorf("ffmpeg shared memory stitch failed: %w | Output: %s", executionError, string(terminalOutput))
	}

	// 5. Read Final Artifact from Shared Memory
	return os.ReadFile(outputFilePath)
}
