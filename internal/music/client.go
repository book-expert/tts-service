/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
1.  LOVE AND CARE
    - Proper error handling and context propagation.
    - Clean interface for music generation.

2.  SIMPLE IS EFFICIENT
    - Use official SDK.
    - Handle Lyria 2 specifics (48kHz, 32.8s clips).
*/

package music

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/book-expert/logger"
	"google.golang.org/genai"
)

type Client struct {
	genaiClient *genai.Client
	modelID     string
	logger      *logger.Logger
}

func NewClient(ctx context.Context, apiKey string, log *logger.Logger) (*Client, error) {
	// Initialize GenAI Client
	// Note: For Vertex AI, we might need ProjectID/Location if not using API Key.
	// Assuming API Key works for now based on user context.
	
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey: apiKey,
		Backend: genai.BackendVertexAI, // Explicitly use Vertex AI for Lyria
		Project: os.Getenv("GOOGLE_CLOUD_PROJECT"), 
		Location: os.Getenv("GOOGLE_CLOUD_LOCATION"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create genai client: %w", err)
	}

	return &Client{
		genaiClient: client,
		modelID:     "lyria-002",
		logger:      log,
	}, nil
}

// GenerateMusic generates a music track from the prompt using Lyria 2.
// Returns the WAV audio data (48kHz, ~32s).
func (c *Client) GenerateMusic(ctx context.Context, prompt string) ([]byte, error) {
	c.logger.Infof("Generating music with %s. Prompt: %s", c.modelID, prompt)

	// Lyria 2 infers BPM and style directly from the text prompt.
	resp, err := c.genaiClient.Models.GenerateContent(ctx, c.modelID, genai.Text(prompt), nil)
	if err != nil {
		return nil, fmt.Errorf("genai request failed: %w", err)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return nil, fmt.Errorf("no content generated")
	}

	for _, part := range resp.Candidates[0].Content.Parts {
		if part.InlineData != nil {
			c.logger.Infof("Received audio data. MimeType: %s", part.InlineData.MimeType)
			return part.InlineData.Data, nil
		}
	}
	
	jsonData, _ := json.Marshal(resp)
	c.logger.Warnf("Full response (no InlineData found): %s", string(jsonData))
	
	return nil, fmt.Errorf("no audio data found in response")
}
