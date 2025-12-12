# GEMINI.md - TTS Service

## Service Overview
This service converts extracted text into speech (audio) using Google's Gemini TTS model.

## Architecture & Data Flow
1.  **Input**: Listens to NATS JetStream subject `texts.processed`.
    -   Payload: `TextProcessedEvent`.
2.  **Processing**:
    -   Downloads text from Object Store (`text_bucket`).
    -   Sends text to **Gemini TTS API**.
        -   **Dynamic Style**: Prepends style instructions (e.g., "Narrate in a Storyteller style") to the text.
        -   **Voice/Language**: Uses `Voice` and `Language` from job settings if provided.
    -   Receives base64-encoded PCM/WAV audio.
    -   Stores audio chunk in Object Store (`audio_bucket`).
    -   **Aggregation**: When the last page is done:
        -   Concatenates all audio chunks.
        -   **Normalization**: Applies peak normalization to ensure consistent volume (-0.2dB peak).
        -   Uploads final `.wav` file.
3.  **Output**: Publishes `audio.created` (or similar) upon final aggregation.

## Configuration
-   **Config File**: `project.toml`
-   **Key Settings**:
    -   `workers`: **10** (High concurrency).
    -   `tts.requests_per_minute`: **10** (Rate limit).
    -   `tts.model`: `gemini-2.5-flash-preview-tts`.

## Current Status (Dec 12, 2025)
-   **Health**: ✅ Healthy
-   **New Features**:
    -   **Voice & Language**: Supports dynamic selection per job.
    -   **Audio Normalization**: Final output is peak-normalized for consistent loudness.
    -   **Style Steering**: Integrated with `JobSettings` to direct the AI narrator.
-   **Performance**: Stable at 10 RPM.
