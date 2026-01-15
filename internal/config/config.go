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
	configuration.Service.LogDirectory = getEnvironmentVariable("TTS_LOG_DIR", "/home/niko/development/logs/tts-logs")
	configuration.Service.WorkerCount = getEnvironmentVariableAsInteger("TTS_WORKERS", 3)

	// TTS Settings
	configuration.TTS.APIKeyEnvironmentVariable = "GEMINI_API_KEY"
	configuration.TTS.BaseURL = getEnvironmentVariable("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com")
	configuration.TTS.AudioServerURL = getEnvironmentVariable("TTS_AUDIO_SERVER_URL", "http://localhost:8001")
	configuration.TTS.SpeechConcurrency = getEnvironmentVariableAsInteger("TTS_SPEECH_CONCURRENCY", 1)

	// NATS Settings
	configuration.NATS.URL = getEnvironmentVariable("NATS_ADDRESS", "nats://localhost:4222")
	configuration.NATS.DeadLetterQueueSubject = getEnvironmentVariable("TTS_DLQ_SUBJECT", "tts.dlq")
	configuration.NATS.Consumer.DurableName = getEnvironmentVariable("TTS_DURABLE_NAME", "tts-consumer")

	return &configuration, nil
}

func getEnvironmentVariable(keyName, fallbackValue string) string {
	if value, exists := os.LookupEnv(keyName); exists {
		return value
	}
	return fallbackValue
}

func getEnvironmentVariableAsInteger(keyName string, fallbackValue int) int {
	valueString := getEnvironmentVariable(keyName, "")
	if valueString == "" {
		return fallbackValue
	}
	value, error := strconv.Atoi(valueString)
	if error != nil {
		return fallbackValue
	}
	return value
}
