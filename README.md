# TTS Service

A NATS-based microservice that orchestrates the generation of Cinematic Audiobooks by combining AI-generated speech and music.

## Overview

This service consumes `texts.processed` events and coordinates with the **Audio Server** to produce high-quality audio. It handles:

1.  **Speech Generation**: Sends text chunks to the `audio-server` (VoxCPM model) to generate voice clones.
2.  **Music Generation**: Requests ambient music loops from the `audio-server` (Lyria model) based on the "Music Prompt" derived from the content.
3.  **Mixing**: Uses FFmpeg to mix the speech with the generated music loop, applying auto-ducking (volume reduction) during speech.
4.  **Aggregation**: Stitches all page audio files into a final audiobook chapter.

## Architecture

-   **Sequential Page Processing**: Processes one page at a time to maintain order and resource sanity.
-   **Bounded Chunk Parallelism**: Processes text chunks (paragraphs) within a page in parallel (Max 2 concurrent requests) to optimize speed without overloading the GPU.
-   **Hybrid Audio**: Combines 48kHz speech (VoxCPM) with 48kHz stereo music (Lyria).

## Configuration

Configuration is managed via `project.toml`:

```toml
[service]
workers = 1 # Strictly 1 to process pages in order

[tts]
requests_per_minute = 60
reference_audio_path = "voices/niko.wav"
reference_text_path = "voices/niko.txt"
```

## Running

```bash
make run
```

## Development

To build the service:
```bash
make build
```

To run linting:
```bash
make lint
```

## Dependencies

-   Go 1.25+
-   NATS Server
-   FFmpeg (for mixing and concatenation)
-   **Audio Server** (running locally or reachable via NATS)