# TTS Service

A NATS-based microservice that converts text into speech (audio) using Google's Gemini TTS model.

## Overview

This service consumes `texts.processed` events, extracts text from the NATS Object Store, generates audio using the Gemini API, and stores the resulting audio chunks (and aggregated final file) back into the Object Store.

## Key Features

-   **High Concurrency**: Supports parallel processing of multiple text pages.
-   **Rate Limiting**: Built-in 10 RPM rate limiter to respect API quotas (prevents 429 errors).
-   **Aggregation**: Automatically aggregates audio chunks into a single WAV file upon workflow completion.
-   **Robustness**: Handles long-running requests with configurable timeouts and retries.

## Configuration

Configuration is managed via `project.toml`:

```toml
[service]
workers = 10 # Concurrency level

[tts]
requests_per_minute = 10 # Rate limit
model = "gemini-2.5-flash-preview-tts"
```

## Running

```bash
make run
```

## Dependencies

-   Go 1.25+
-   NATS Server
-   Google GenAI SDK
