/*
GOLDEN RULES & DEVELOPER MANIFESTO (THE NORTH STAR)
--------------------------------------------------------------------------------
"Work is love made visible. And if you cannot work with love but only with
distaste, it is better that you should leave your work and sit at the gate of
the temple and take alms of those who work with joy." — Kahlil Gibran

1.  LOVE AND CARE (Primary Driver)
    - This is a craft. Build with pride, honesty, and kindness.
    - If you put love in your work, you build something deserving of love.
    - Be helpful: Code is read more than written; optimize for the reader.

2.  WRITE WHAT YOU MEAN (Explicit > Implicit)
    - Use WHOLE WORDS: `RequestIdentifier` not `ReqID`.
    - No magic numbers: Move application settings to `project.toml`.
    - Secure by design: Keep API keys and secrets strictly in `.env`.
    - No ambiguity: If you assume something, document it.

3.  SIMPLE IS EFFICIENT (Minimal Viable Elegance)
    - Avoid over-engineering. Small interfaces, clear structs.
    - If a design requires a hack, stop. Redesign it with elegance.
    - Lean, Clean, Mean: Delete dead code immediately.

4.  NO BASELESS ASSUMPTIONS (Scientific Rigor)
    - Do not guess. Base decisions on documentation and proven patterns.
    - If you do not know, ask or verify.

5.  NON-BLOCKING & ROBUST
    - Never block the main goroutine. Use Context for cancellation.
    - Handle errors explicitly: Don't just return them, wrap them with context.

--------------------------------------------------------------------------------
EXAMPLES OF "LOVE AND CARE" IN THIS CONTEXT:
--------------------------------------------------------------------------------
(A) NAMING
    Indifferent:  func Gen(t string, v string)
    With Love:    func GenerateSoundscape(ctx context.Context, textPrompt string, voiceID string)
    *Why: The Agent reading this next year will know exactly what it does and that it is cancellable.*

(B) CONFIGURATION
    Indifferent:  const Timeout = 30 // Hardcoded
    With Love:    config.App.TimeoutSeconds // Loaded from project.toml
    *Why: Allows behavior tuning without recompiling or touching the codebase.*

(C) ERROR HANDLING
    Indifferent:  if err != nil { return err }
    With Love:    if err != nil { return fmt.Errorf("failed to initialize vox engine: %w", err) }
    *Why: Wrapping the error gives the user the 'trace of breadcrumbs' they need to fix it. That is kindness.*
--------------------------------------------------------------------------------
*/

// Package objectstore provides a NATS-based implementation of the ObjectStore interface.
// It abstracts the JetStream Object Store for storing and retrieving large files.
package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/nats-io/nats.go/jetstream"
)

// NatsObjectStore implements the core.ObjectStore interface using NATS JetStream.
// Why: Encapsulates NATS-specific logic, allowing the rest of the application to interact with a generic interface.
type NatsObjectStore struct {
	jetStreamContext jetstream.JetStream
	bucketName       string
	objectStore      jetstream.ObjectStore
}

// New creates a new NatsObjectStore instance.
//
// Behavior: It attempts to create the bucket with default configuration.
// If the bucket already exists, it binds to it.
func New(ctx context.Context, jetStreamContext jetstream.JetStream, bucketName string) (*NatsObjectStore, error) {
	config := jetstream.ObjectStoreConfig{
		Bucket:      bucketName,
		Description: fmt.Sprintf("Storage for the %s bucket.", bucketName),
		Storage:     jetstream.FileStorage,
		Replicas:    1,
	}

	store, err := jetStreamContext.CreateObjectStore(ctx, config)
	if err != nil {
		// If bucket exists, standard CreateObjectStore might fail depending on client version/config.
		// We explicitly handle the bind fallback.
		// Note: jetstream.ErrBucketExists checks usually require exact error matching.
		store, err = jetStreamContext.ObjectStore(ctx, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to bind to existing object store bucket '%s': %w", bucketName, err)
		}
	}

	return &NatsObjectStore{
		jetStreamContext: jetStreamContext,
		bucketName:       bucketName,
		objectStore:      store,
	}, nil
}

// Download retrieves the content of an object by its key.
//
// Why: Reads the entire stream into memory. Be cautious with extremely large files.
func (natsStore *NatsObjectStore) Download(ctx context.Context, objectKey string) ([]byte, error) {
	objectEntry, err := natsStore.objectStore.Get(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get object '%s' from bucket '%s': %w", objectKey, natsStore.bucketName, err)
	}

	// Ensure resource cleanup regardless of read success.
	defer func() {
		// Explicitly ignore close error as we cannot log it here and it should not overwrite the primary error.
		_ = objectEntry.Close()
	}()

	data, err := io.ReadAll(objectEntry)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body for '%s': %w", objectKey, err)
	}

	return data, nil
}

// Upload stores the provided data under the specified key.
func (natsStore *NatsObjectStore) Upload(ctx context.Context, objectKey string, data []byte) error {
	dataReader := bytes.NewReader(data)
	metadata := jetstream.ObjectMeta{
		Name: objectKey,
	}

	_, err := natsStore.objectStore.Put(ctx, metadata, dataReader)
	if err != nil {
		return fmt.Errorf("failed to put object '%s' into bucket '%s': %w", objectKey, natsStore.bucketName, err)
	}

	return nil
}
