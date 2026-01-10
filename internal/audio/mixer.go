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

type Mixer struct {
	logger *logger.Logger
}

func NewMixer(serviceLogger *logger.Logger) *Mixer {
	return &Mixer{logger: serviceLogger}
}

// GenerateSilentWav creates a silent WAV file byte slice with the specified parameters.
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
		_ = binary.Write(&buffer, binary.LittleEndian, uint16(3))
	} else {
		_ = binary.Write(&buffer, binary.LittleEndian, uint16(1))
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

func (mixer *Mixer) Sanitize(requestContext context.Context, inputPath string) (string, error) {
	outputPath := filepath.Join(os.TempDir(), fmt.Sprintf("clean_%s.wav", filepath.Base(inputPath)))

	command := exec.CommandContext(requestContext, "sox",
		inputPath,
		"-r", "48000",
		"-c", "2",
		"-b", "24",
		outputPath,
	)

	if output, executionError := command.CombinedOutput(); executionError != nil {
		return "", fmt.Errorf("sox sanitize failed: %w | Output: %s", executionError, string(output))
	}

	return outputPath, nil
}

func (mixer *Mixer) Combine(requestContext context.Context, inputPaths []string) ([]byte, error) {
	if len(inputPaths) == 0 {
		return nil, fmt.Errorf("no inputs to combine")
	}

	var cleanPaths []string
	defer func() {
		for _, path := range cleanPaths {
			_ = os.Remove(path)
		}
	}()

	for _, path := range inputPaths {
		cleanPath, sanitizeError := mixer.Sanitize(requestContext, path)
		if sanitizeError != nil {
			return nil, fmt.Errorf("failed to sanitize input %s: %w", path, sanitizeError)
		}
		cleanPaths = append(cleanPaths, cleanPath)
	}

	listFile, creationError := os.CreateTemp("", "concat_list_*.txt")
	if creationError != nil {
		return nil, fmt.Errorf("failed to create concat list: %w", creationError)
	}
	defer func() { _ = os.Remove(listFile.Name()) }()

	var listContent strings.Builder
	for _, path := range cleanPaths {
		// Escape single quotes for ffmpeg concat file
		safePath := strings.ReplaceAll(path, "'", "'\\''")
		listContent.WriteString(fmt.Sprintf("file '%s'\n", safePath))
	}
	if _, writeError := listFile.WriteString(listContent.String()); writeError != nil {
		return nil, fmt.Errorf("failed to write concat list: %w", writeError)
	}
	_ = listFile.Close()

	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("concat_%s.wav", filepath.Base(cleanPaths[0])))
	defer func() { _ = os.Remove(outputFile) }()

	command := exec.CommandContext(requestContext, "ffmpeg",
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c", "copy",
		outputFile,
	)

	if output, executionError := command.CombinedOutput(); executionError != nil {
		return nil, fmt.Errorf("ffmpeg concat failed: %w | Output: %s", executionError, string(output))
	}

	return os.ReadFile(outputFile)
}