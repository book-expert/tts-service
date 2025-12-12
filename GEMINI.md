# GEMINI.md - TTS Service

## Service Overview
This service converts extracted text into speech (audio) using the Gemini TTS model.

## Architecture & Data Flow
1.  **Input**: Listens to NATS JetStream subject `texts.processed`.
    -   Payload: `TextProcessedEvent`.
2.  **Processing**:
    -   Downloads text from Object Store (`text_bucket`).
    -   Sends text to **Gemini TTS API**.
    -   Receives base64-encoded PCM/WAV audio.
    -   Stores audio chunk in Object Store (`audio_bucket`).
    -   **Aggregation**: When the last page is done, concatenates all chunks into a final `.wav` file.
3.  **Output**: Publishes `audio.created` (or similar) upon final aggregation.

## Configuration
-   **Config File**: `project.toml`
-   **Key Settings**:
    -   `workers`: **10** (Increased to handle high latency).
    -   `tts.requests_per_minute`: **10** (Rate limit).
    -   `tts.model`: `gemini-2.5-flash-preview-tts`.

## Current Status (Dec 12, 2025)
-   **Health**: ✅ Healthy
-   **Resolved Issues**:
    -   **Rate Limiting**: Implemented strict 10 RPM limiter to prevent 429 errors.
    -   **Timeouts**: Increased HTTP client timeout to 600s and processing timeout to 700s.
    -   **Throughput**: Scaled workers to 10 to utilize the full 10 RPM quota despite high generation latency.