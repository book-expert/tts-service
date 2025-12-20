/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
1.  LOVE AND CARE
    - Pure Go implementation to avoid external dependencies (ffmpeg).
    - Robust header parsing and size calculation.

2.  SIMPLE IS EFFICIENT
    - Concatenates identical WAV files by merging data chunks and updating headers.
*/

package audio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// WavHeader represents the standard WAV file header (44 bytes).
type WavHeader struct {
	ChunkID       [4]byte // "RIFF"
	ChunkSize     uint32  // Total file size - 8
	Format        [4]byte // "WAVE"
	Subchunk1ID   [4]byte // "fmt "
	Subchunk1Size uint32  // 16 for PCM
	AudioFormat   uint16  // 1 for PCM
	NumChannels   uint16
	SampleRate    uint32
	ByteRate      uint32
	BlockAlign    uint16
	BitsPerSample uint16
	Subchunk2ID   [4]byte // "data"
	Subchunk2Size uint32  // Size of audio data
}

// ConcatenateWavs merges multiple WAV files into a single WAV file.
// It assumes all files have the exact same format (SampleRate, Channels, Bits).
func ConcatenateWavs(filepaths []string) ([]byte, error) {
	if len(filepaths) == 0 {
		return nil, errors.New("no files to concatenate")
	}

	var totalDataSize uint32
	var firstHeader *WavHeader
	var audioData = new(bytes.Buffer)

	for i, path := range filepaths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", path, err)
		}

		if len(data) < 44 {
			return nil, fmt.Errorf("file %s is too small to be a WAV", path)
		}

		header, err := parseHeader(data[:44])
		if err != nil {
			return nil, fmt.Errorf("invalid header in %s: %w", path, err)
		}

		if i == 0 {
			firstHeader = header
		} else {
			// Validation: Ensure formats match
			if header.SampleRate != firstHeader.SampleRate ||
				header.NumChannels != firstHeader.NumChannels ||
				header.BitsPerSample != firstHeader.BitsPerSample {
				return nil, fmt.Errorf("format mismatch in %s", path)
			}
		}

		// Append data chunk (everything after 44 bytes)
		// Note: robust implementations might scan for 'data' chunk, but standard 44-byte header is our target constraint.
		audioData.Write(data[44:])
		totalDataSize += uint32(len(data) - 44)
	}

	// Update header with new sizes
	finalHeader := *firstHeader
	finalHeader.Subchunk2Size = totalDataSize
	finalHeader.ChunkSize = 36 + totalDataSize

	// Serialize
	var result bytes.Buffer
	if err := binary.Write(&result, binary.LittleEndian, finalHeader); err != nil {
		return nil, err
	}
	if _, err := result.Write(audioData.Bytes()); err != nil {
		return nil, err
	}

	return result.Bytes(), nil
}

func parseHeader(data []byte) (*WavHeader, error) {
	var h WavHeader
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, &h); err != nil {
		return nil, err
	}
	if string(h.ChunkID[:]) != "RIFF" || string(h.Format[:]) != "WAVE" {
		return nil, errors.New("invalid WAV signature")
	}
	return &h, nil
}
