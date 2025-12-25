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
