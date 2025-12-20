/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
"Work is love made visible. And if you cannot work with love but only with
distaste, it is better that you should leave your work and sit at the gate of
the temple and take alms of those who work with joy." — Kahlil Gibran

1.  LOVE AND CARE (Primary Driver)
    - This is a craft. Build with pride, honesty, and kindness.
    - If you put love in your work, you build something deserving of love.
    - Be helpful: Code is read more than written; optimize for the reader.

2.  WRITE WHAT YOU MEAN (Explicit > Implicit)
    - Use WHOLE WORDS: `RequestIdentifier` not `ReqID`.
    - No magic numbers: Move application settings to `project.toml`.
    - Secure by design: Keep API keys and secrets strictly in `.env`.
    - No ambiguity: If you assume something, document it.

3.  SIMPLE IS EFFICIENT (Minimal Viable Elegance)
    - Avoid over-engineering. Small interfaces, clear structs.
    - If a design requires a hack, stop. Redesign it with elegance.
    - Lean, Clean, Mean: Delete dead code immediately.

4.  NO BASELESS ASSUMPTIONS (Scientific Rigor)
    - Do not guess. Base decisions on documentation and proven patterns.
    - If you do not know, ask or verify.

5.  NON-BLOCKING & ROBUST
    - Never block the main goroutine. Use Context for cancellation.
    - Handle errors explicitly: Don't just return them, wrap them with context.

--------------------------------------------------------------------------------
EXAMPLES OF "LOVE AND CARE" IN THIS CONTEXT:
--------------------------------------------------------------------------------
(A) NAMING
    Indifferent:  func Gen(t string, v string)
    With Love:    func GenerateSoundscape(ctx context.Context, textPrompt string, voiceID string)
    *Why: The Agent reading this next year will know exactly what it does and that it is cancellable.*

(B) CONFIGURATION
    Indifferent:  const Timeout = 30 // Hardcoded
    With Love:    config.App.TimeoutSeconds // Loaded from project.toml
    *Why: Allows behavior tuning without recompiling or touching the codebase.*

(C) ERROR HANDLING
    Indifferent:  if err != nil { return err }
    With Love:    if err != nil { return fmt.Errorf("failed to initialize vox engine: %w", err) }
    *Why: Wrapping the error gives the user the 'trace of breadcrumbs' they need to fix it. That is kindness.*
--------------------------------------------------------------------------------
*/

package tts

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
