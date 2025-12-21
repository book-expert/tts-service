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