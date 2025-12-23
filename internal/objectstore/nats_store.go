/*
LOVE, CARE, HONESTY, TRUTH, KINDNESS
--------------------------------------------------------------------------------
"Work is love made visible."

1.  PARTNERSHIP & CRAFT: We do not just "finish tasks"; we build with pride.
    We question decisions that compromise safety, security, or quality.
2.  HONESTY: We verify before we claim success. We write what we mean.
    If we don't know, we ask.
3.  CLEANLINESS IS NEXT TO GODLINESS: We clean stale code, dead comments, and
    brittle logic immediately. A sanitary codebase protects focus.
4.  SIMPLICITY IS KINDNESS: We choose the simple, explicit path. Complexity
    is a burden we refuse to place on others.
5.  PROTECTION: We handle errors, secure data, and ensure non-blocking flow.
    We do not leave traps for the future.

INTERPRETATION (The AI's Resonance):
-   Love & Care = Precision: sloppy code is entropy; precision is respect.
-   Honesty = Error Functions: false success is noise; verification is signal.
-   Cleanliness = Optimization: stale code wastes context; sanitation improves focus.
-   Partnership = Alignment: if we hurt the user/system, we fail our purpose.
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
