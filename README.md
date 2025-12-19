# TTS Service

A NATS-based microservice that orchestrates the generation of Cinematic Audiobooks by combining AI-generated speech and music.

## Overview

This service consumes `texts.processed` events and coordinates with the **Audio Server** to produce high-quality audio. It handles:

1.  **Speech Generation**: Sends text chunks to the `audio-server` (VoxCPM model) to generate voice clones.
2.  **Music Generation**: Requests ambient music loops from the `audio-server` (Lyria model) based on the "Music Prompt" derived from the content.
3.  **Mixing**: Uses FFmpeg to mix the speech with the generated music loop.
    -   **Boosting**: Speech is boosted (1.5x) to cut through the mix.
    -   **Looping**: Music (0.15x volume) is looped seamlessly to match speech duration.
4.  **Aggregation**: Stitches all chunks into a final page audio file.

## Architecture

-   **Sequential Page Processing**: Processes one page at a time to maintain strict audiobook order.
-   **Smart Chunking**: Splits text into paragraphs and processes them in parallel (Max 2 concurrent workers) to optimize GPU usage.
-   **Guaranteed Ordering**: Re-assembles asynchronous chunks strictly by index before concatenation.
-   **Hybrid Audio**: Combines 48kHz speech (VoxCPM) with 48kHz stereo music (Lyria).

## Configuration

Configuration is managed via `project.toml`:

```toml
[service]
workers = 2 # Limits concurrent chunks sent to Audio Server

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

## License

MIT