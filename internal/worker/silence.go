package worker

import (
	"bytes"
	"encoding/binary"
	"time"
)

// generateSilentWav creates a silent WAV file byte slice with the specified parameters.
// Using 32-bit float for high quality and to prevent clipping during processing.
func generateSilentWav(duration time.Duration, sampleRate, channels, bitsPerSample int) []byte {
	var buf bytes.Buffer
	numSamples := int(duration.Seconds() * float64(sampleRate))
	dataSize := numSamples * channels * (bitsPerSample / 8)
	fileSize := 36 + dataSize

	// RIFF header
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(fileSize))
	buf.WriteString("WAVE")

	// "fmt " sub-chunk
	buf.WriteString("fmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16)) // Sub-chunk 1 size (16 for PCM)
	if bitsPerSample == 32 {
		_ = binary.Write(&buf, binary.LittleEndian, uint16(3)) // Audio format (3 for IEEE float)
	} else {
		_ = binary.Write(&buf, binary.LittleEndian, uint16(1)) // Audio format (1 for PCM)
	}
	_ = binary.Write(&buf, binary.LittleEndian, uint16(channels))   // Number of channels
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate)) // Sample rate
	byteRate := sampleRate * channels * (bitsPerSample / 8)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(byteRate)) // Byte rate
	blockAlign := channels * (bitsPerSample / 8)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(blockAlign))    // Block align
	_ = binary.Write(&buf, binary.LittleEndian, uint16(bitsPerSample)) // Bits per sample

	// "data" sub-chunk
	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(dataSize))

	// Silent audio data (zeros)
	buf.Write(make([]byte, dataSize))

	return buf.Bytes()
}
