# TTS Service

The **TTS Service** is a Go-based microservice that orchestrates the synthesis of text into high-fidelity speech and coordinates the generation of background music for the book-to-audio ecosystem.

## Overview

Operating as a central coordinator in the processing pipeline, the TTS Service listens for processed text segments, manages state across multi-page documents, and interfaces with the specialized `audio-server` for speech synthesis. It also triggers music generation requests to the `soundscape-service` based on document analysis.

## Key Features

- **Distributed Synthesis**: Delegates high-compute speech synthesis tasks to the GPU-accelerated `audio-server` via a REST API.
- **Workflow State Management**: Utilizes NATS KeyValue store (`TTS_PROGRESS`) to track the status of individual pages and determine when to trigger document-wide aggregation.
- **Contextual Synthesis**: Propagates voice profiles and style directives (from document analysis) to ensure consistent narration.
- **Background Music Coordination**: Triggers music requests to the `soundscape-service` for each job, ensuring audio artifacts are ready for final mixing.
- **Sequential Chunking**: Intelligently splits text into chunks for optimal synthesis performance and stability on the inference engine.
- **Event-Driven Resilience**: Implements a robust NATS worker pattern powered by the `common-worker` library, with explicit ACK/NAK handling and dead-letter queue (DLQ) support.

## Requirements

- Go 1.25.5+
- NATS Server with JetStream enabled
- **Audio Server**: A running instance of the Python-based `audio-server` for speech synthesis.
- **Soundscape Service**: For background music generation.

## Configuration

The service is configured via `project.toml`. Key settings include:

- `[service]`: Worker count and log directory.
- `[tts]`: Connection details for the `audio-server` and speech concurrency limits.
- `[nats]`: Stream, subject, and object store bucket configurations for Text inputs and TTS outputs.
- `[nats.kv]`: Progress tracking bucket name.

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
- `internal/audio`: Speech client implementation and text-to-speech processing logic.
- `internal/worker`: Main event handlers for processing extracted text and triggering synthesis.
- `internal/objectstore`: NATS Object Store client for fetching text and uploading audio chunks.
- `internal/core`: Shared interfaces and domain models.

## Events

### Consumes
- `texts.processed`: Triggered when text has been extracted and is ready for synthesis.

### Produces
- `tts.started`: Triggered when synthesis begins for a page (for UI progress tracking).
- `tts.result`: Triggered when a page's audio has been successfully generated and stored.
- `audio.music.request`: Triggered to start background music generation for a workflow.
- `tts.aggregation.started`: Triggered when the last page is synthesized, signaling the start of final audio mixing.
