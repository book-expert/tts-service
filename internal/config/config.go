/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

package config

import (
	"os"
	"strconv"
)

// Config represents the top-level structure of the service configuration.
type Config struct {
	Service ServiceSettings
	TTS     TTSSettings
	NATS    NATSSettings
}

// ServiceSettings holds general service settings like logging and concurrency.
type ServiceSettings struct {
	LogDirectory string
	WorkerCount  int
}

// TTSSettings holds configuration specific to the Text-To-Speech provider.
type TTSSettings struct {
	APIKeyEnvironmentVariable string
	BaseURL                   string
	AudioServerURL            string
	SpeechConcurrency         int
}

// NATSSettings holds connection information for NATS.
type NATSSettings struct {
	URL                    string
	DeadLetterQueueSubject string
	Consumer               ConsumerSettings
}

// ConsumerSettings defines the JetStream consumer settings.
type ConsumerSettings struct {
	DurableName string
}

// Load retrieves the configuration from environment variables.
func Load(_ string) (*Config, error) {
	var configuration Config

	// Service Settings
	configuration.Service.LogDirectory = getEnv("TTS_LOG_DIR", "/home/niko/development/logs/tts-logs")
	configuration.Service.WorkerCount = getEnvInt("TTS_WORKERS", 3)

	// TTS Settings
	configuration.TTS.APIKeyEnvironmentVariable = getEnv("TTS_API_KEY_VARIABLE", "GEMINI_API_KEY")
	configuration.TTS.BaseURL = getEnv("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com")
	configuration.TTS.AudioServerURL = getEnv("TTS_AUDIO_SERVER_URL", "http://localhost:8001")
	configuration.TTS.SpeechConcurrency = getEnvInt("TTS_SPEECH_CONCURRENCY", 1)

	// NATS Settings
	configuration.NATS.URL = getEnv("NATS_ADDRESS", "nats://localhost:4222")
	configuration.NATS.DeadLetterQueueSubject = getEnv("TTS_DLQ_SUBJECT", "tts.dlq")
	configuration.NATS.Consumer.DurableName = getEnv("TTS_DURABLE_NAME", "tts-consumer")

	return &configuration, nil
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return fallback
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return fallback
	}
	return value
}
