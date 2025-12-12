// Package worker (wav.go) provides utilities for audio file manipulation.
// It handles the creation of valid WAV headers for raw PCM data.
package worker

import (
	"encoding/binary"
)

const (
	// WavHeaderSize defines the standard length of a RIFF WAV header in bytes.
	WavHeaderSize = 44

	// WavFormatPCM is the format code for uncompressed Linear PCM audio.
	WavFormatPCM = 1

	// WavSubchunk1Size is the size of the 'fmt ' chunk for PCM data (16 bytes).
	WavSubchunk1Size = 16

	// BitsPerByte is used for converting bit counts to byte counts.
	BitsPerByte = 8
)

// withWAVHeader prepends a standard 44-byte WAV header to the provided PCM byte slice.
//
// Why: Raw PCM data lacks metadata. This header tells media players the sample rate,
// channel count, and bit depth required to play the audio correctly.
func withWAVHeader(pcmData []byte, sampleRate int, channelCount int, bitsPerSample int) []byte {
	dataLength := len(pcmData)
	totalFileSize := uint32(dataLength + WavHeaderSize - 8)

	// Calculate data rates for the header.
	byteRate := uint32(sampleRate * channelCount * bitsPerSample / BitsPerByte)
	blockAlign := uint16(channelCount * bitsPerSample / BitsPerByte)

	header := make([]byte, WavHeaderSize)

	//

	// 1. RIFF Chunk Descriptor
	copy(header[0:4], []byte("RIFF"))
	binary.LittleEndian.PutUint32(header[4:8], totalFileSize)
	copy(header[8:12], []byte("WAVE"))

	// 2. fmt Sub-chunk (Describes the format of the sound information)
	copy(header[12:16], []byte("fmt "))
	binary.LittleEndian.PutUint32(header[16:20], WavSubchunk1Size)
	binary.LittleEndian.PutUint16(header[20:22], WavFormatPCM)
	binary.LittleEndian.PutUint16(header[22:24], uint16(channelCount))
	binary.LittleEndian.PutUint32(header[24:28], uint32(sampleRate))
	binary.LittleEndian.PutUint32(header[28:32], byteRate)
	binary.LittleEndian.PutUint16(header[32:34], blockAlign)
	binary.LittleEndian.PutUint16(header[34:36], uint16(bitsPerSample))

	// 3. data Sub-chunk (Indicates the size of the sound information)
	copy(header[36:40], []byte("data"))
	binary.LittleEndian.PutUint32(header[40:44], uint32(dataLength))

	// Append the raw PCM data after the header.
	return append(header, pcmData...)
}

// normalizeAudio applies peak normalization to the 16-bit PCM data.
// It scales the audio amplitude so the loudest peak reaches a near-maximum level (-0.5dB approx),
// ensuring consistent volume across different generations.
func normalizeAudio(pcmData []byte) []byte {
	if len(pcmData) == 0 {
		return pcmData
	}

	// 1. Convert bytes to int16 samples
	sampleCount := len(pcmData) / 2
	samples := make([]int16, sampleCount)
	for i := 0; i < sampleCount; i++ {
		samples[i] = int16(binary.LittleEndian.Uint16(pcmData[i*2:]))
	}

	// 2. Find the absolute peak value
	var maxVal int16
	for _, s := range samples {
		absS := s
		if s < 0 {
			absS = -s
		}
		if absS > maxVal {
			maxVal = absS
		}
	}

	// If silence, return original
	if maxVal == 0 {
		return pcmData
	}

	// 3. Calculate scaling factor
	// Target 32000 (out of 32767) to be safe (~97% or -0.2dB)
	const targetPeak = 32000
	scale := float64(targetPeak) / float64(maxVal)

	// If already loud enough (or louder/clipped), don't scale down aggressively unless clipped
	// But normalization usually implies bringing UP quiet audio.
	// If scale < 1.0 (audio is clipped), we should scale down.
	// If scale > 1.0 (audio is quiet), we scale up.

	// 4. Apply scaling
	normalized := make([]byte, len(pcmData))
	for i := 0; i < sampleCount; i++ {
		val := float64(samples[i]) * scale
		// Clamp just in case float math overshoots
		if val > 32767 {
			val = 32767
		} else if val < -32768 {
			val = -32768
		}
		binary.LittleEndian.PutUint16(normalized[i*2:], uint16(int16(val)))
	}

	return normalized
}

