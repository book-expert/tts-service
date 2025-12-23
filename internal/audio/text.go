/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS
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

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
--------------------------------------------------------------------------------
*/

package audio

import (
	"strings"
	"unicode"
)

// TextChunk represents a processed segment of text ready for audio generation.
type TextChunk struct {
	ID   int    // Sequence number
	Text string // The actual text content
}

// SplitText divides a large text block into manageable chunks for TTS processing.
// It respects paragraphs and sentence boundaries to maintain natural flow.
// Max chunk size is soft-capped around 300 characters to prevent VRAM OOM.
func SplitText(text string) []TextChunk {
	var chunks []TextChunk
	var currentID = 0

	// Normalize newlines
	text = strings.ReplaceAll(text, "\r\n", "\n")

	// 1. Split by Paragraphs (Double Newline)
	paragraphs := strings.Split(text, "\n\n")

	for _, p := range paragraphs {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		// If paragraph fits, add it.
		if len(p) < 300 {
			chunks = append(chunks, TextChunk{ID: currentID, Text: p})
			currentID++
			continue
		}

		// 2. Split by Sentences if paragraph is too long
		// We use a custom splitter to avoid breaking on "Dr.", "Mr.", etc.
		// For this implementation, we'll use a simplified robust approach.
		sentences := splitIntoSentences(p)

		currentChunk := ""
		for _, s := range sentences {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}

			// If adding this sentence exceeds limit, push current chunk
			if len(currentChunk)+len(s)+1 > 300 {
				if currentChunk != "" {
					chunks = append(chunks, TextChunk{ID: currentID, Text: currentChunk})
					currentID++
					currentChunk = ""
				}
			}

			if currentChunk != "" {
				currentChunk += " " + s
			} else {
				currentChunk = s
			}
		}

		// Push remaining
		if currentChunk != "" {
			chunks = append(chunks, TextChunk{ID: currentID, Text: currentChunk})
			currentID++
		}
	}

	return chunks
}

// splitIntoSentences roughly splits text by terminal punctuation.
// This is a heuristic approach (KISS) rather than a full NLP tokenizer.
func splitIntoSentences(text string) []string {
	var sentences []string
	var sb strings.Builder

	runes := []rune(text)
	for i, r := range runes {
		sb.WriteRune(r)

		// Check for terminal punctuation
		if r == '.' || r == '!' || r == '?' {
			// Look ahead to ensure it's a real sentence end (followed by space or end of string)
			// And NOT part of an acronym like "U.S." or "e.g." (Simplified check: if next is lower, probably not end)
			if i+1 < len(runes) && runes[i+1] != ' ' && runes[i+1] != '"' && runes[i+1] != '\n' {
				continue
			}

			// Simple check for titles (Dr., Mr.) - very basic
			lastWord := getLastWord(sb.String())
			if isAbbreviation(lastWord) {
				continue
			}

			sentences = append(sentences, sb.String())
			sb.Reset()
		}
	}

	if sb.Len() > 0 {
		sentences = append(sentences, sb.String())
	}

	return sentences
}

func getLastWord(s string) string {
	parts := strings.Fields(s)
	if len(parts) == 0 {
		return ""
	}

	// Remove trailing punctuation from the word before checking
	word := parts[len(parts)-1]
	return strings.TrimRightFunc(word, func(r rune) bool {
		return unicode.IsPunct(r) && r != '.' // Keep period for check
	})
}

func isAbbreviation(s string) bool {
	// Common abbreviations that end in period but aren't sentence ends
	lower := strings.ToLower(s)

	// Ensure it ends with a period
	if !strings.HasSuffix(lower, ".") {
		return false
	}

	abbrevs := []string{"dr.", "mr.", "mrs.", "ms.", "jr.", "sr.", "prof.", "vol.", "inc.", "ltd.", "co.", "vs.", "e.g.", "i.e."}
	for _, a := range abbrevs {
		if lower == a {
			return true
		}
	}
	return false
}
