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

// Stitcher handles the concatenation of audio chunks for a single page.
type Stitcher struct {
	serviceLogger *logger.Logger
}

// NewStitcher initializes a new audio Stitcher.
func NewStitcher(serviceLogger *logger.Logger) *Stitcher {
	return &Stitcher{serviceLogger: serviceLogger}
}

// GenerateSilentWav creates a silent WAV file byte slice.
func GenerateSilentWav(duration time.Duration, sampleRate, channels, bitsPerSample int) []byte {
	var buffer bytes.Buffer
	numSamples := int(duration.Seconds() * float64(sampleRate))
	dataSize := numSamples * channels * (bitsPerSample / 8)
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

// Stitch combines multiple audio files into a single WAV byte slice using FFmpeg.
func (stitcher *Stitcher) Stitch(requestContext context.Context, inputPaths []string) ([]byte, error) {
	if len(inputPaths) == 0 {
		return nil, fmt.Errorf("no inputs to stitch")
	}

	listFile, creationError := os.CreateTemp("", "stitch_list_*.txt")
	if creationError != nil {
		return nil, fmt.Errorf("failed to create stitch list: %w", creationError)
	}
	defer func() { _ = os.Remove(listFile.Name()) }()

	var listContent strings.Builder
	for _, path := range inputPaths {
		safePath := strings.ReplaceAll(path, "'", "'\\''")
		listContent.WriteString(fmt.Sprintf("file '%s'\n", safePath))
	}
	if _, writeError := listFile.WriteString(listContent.String()); writeError != nil {
		return nil, fmt.Errorf("failed to write stitch list: %w", writeError)
	}
	_ = listFile.Close()

	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("stitched_%d.wav", time.Now().UnixNano()))
	defer func() { _ = os.Remove(outputFile) }()

	// We use the concat filter instead of '-c copy' to ensure stream consistency
	// and avoid header mismatches that cause the 'helium effect'.
	sampleRate := os.Getenv("AUDIO_SAMPLE_RATE_TTS")
	if sampleRate == "" {
		sampleRate = "44100"
	}
	bits := os.Getenv("AUDIO_BITS_PER_SAMPLE")
	codec := "pcm_s24le"
	switch bits {
	case "32":
		codec = "pcm_s32le"
	case "16":
		codec = "pcm_s16le"
	}

	command := exec.CommandContext(requestContext, "ffmpeg",
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-ar", sampleRate,
		"-c:a", codec,
		outputFile,
	)

	if output, executionError := command.CombinedOutput(); executionError != nil {
		return nil, fmt.Errorf("ffmpeg stitch failed: %w | Output: %s", executionError, string(output))
	}

	return os.ReadFile(outputFile)
}
