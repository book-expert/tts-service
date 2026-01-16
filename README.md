# TTS Service

The **TTS Service** is a Go-based microservice that orchestrates the synthesis of text into high-fidelity speech and coordinates the generation of background music for the book-to-audio ecosystem.

## Overview

Operating as the central coordinator in the processing pipeline, the TTS Service listens for processed text segments, manages state across multi-page documents, and interfaces with the specialized `audio-server` for neural speech synthesis. It also triggers music generation requests based on document analysis.

## Key Features

- **Distributed Synthesis**: Delegates compute-intensive speech synthesis to the GPU-accelerated `audio-server`.
- **Workflow State Management**: Utilizes NATS KeyValue store (`TTS_PROGRESS`) to track the status of individual pages and trigger aggregation.
- **Contextual Synthesis**: Propagates voice profiles and style directives to ensure consistent narration.
- **Background Music Coordination**: Triggers music requests to the `soundscape-service` for each job.
- **Sequential Chunking**: Intelligently splits text into chunks for optimal synthesis performance and inference stability.
- **Event-Driven Resilience**: Powered by the `common-worker` library, with explicit ACK/NAK handling and dead-letter queue (DLQ) support.

## 🛡️ Alignment with Project Standards

This service adheres to the **Manifesto of Truth** and project engineering standards:
- **Whole Words Only**: Naming conventions avoid abbreviations (e.g., `synthesis`, `configuration`, `context`).
- **Resilience**: Orchestration logic handles synthesis timeouts and hardware failures by re-queuing tasks.
- **Care**: Ensures consistent voice profile application across all pages of a document.

## Requirements

- Go 1.25.5+
- NATS Server with JetStream enabled
- **Audio Server**: A running instance of the Python-based `audio-server`.
- **Soundscape Service**: For background music generation.

## Configuration

The service is configured via `project.toml`. Key settings include:

- `[service]`: Worker count and log directory.
- `[tts]`: Connection details for the `audio-server` and speech concurrency limits.
- `[nats]`: Stream, subject, and object store bucket configurations.

## Getting Started

### Installation
```bash
make install
```

### Building
```bash
make build
```

### Running
```bash
make run
```

## Internal Architecture

- `cmd/tts-service`: Application initialization and NATS consumer orchestration.
- `internal/audio`: Speech client implementation and processing logic.
- `internal/worker`: Main event handlers for processing text and triggering synthesis.
- `internal/objectstore`: NATS Object Store client for fetching text and uploading audio chunks.

## Events

### Consumes
- `texts.processed`: Triggered when text is ready for synthesis.

### Produces
- `tts.started`: Triggered when synthesis begins for a page.
- `tts.result`: Triggered when a page's audio is ready.
- `audio.music.request`: Triggered to start background music generation.
- `tts.aggregation.started`: Triggered to signal the start of final audio mixing.

---
*Built with ❤️, Craftsmanship, and Discipline.*
