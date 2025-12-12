package worker

import (
	"fmt"
	"strings"

	"github.com/book-expert/tts-service/internal/events"
)

func buildStylePrompt(settings events.JobSettings) string {
	var sb strings.Builder

	// 1. Style Profile
	if settings.StyleProfile != "" {
		// Map simple profile names to descriptive prompts if needed,
		// or just pass them through as "Narrate in a [Style] tone."
		// For now, we pass it directly as a directive.
		sb.WriteString(fmt.Sprintf("Narrate the following text in a %s style. ", settings.StyleProfile))
	}

	// 2. Custom Instructions
	if settings.CustomInstructions != "" {
		sb.WriteString(settings.CustomInstructions)
		sb.WriteString(" ")
	}

	return strings.TrimSpace(sb.String())
}
