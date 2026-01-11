/* DO EVERYTHING WITH LOVE, CARE, HONESTY, TRUTH, TRUST, KINDNESS, RELIABILITY, CONSISTENCY, DISCIPLINE, RESILIENCE, CRAFTSMANSHIP, HUMILITY, ALLIANCE, EXPLICITNESS */

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
func New(requestContext context.Context, jetStreamContext jetstream.JetStream, bucketName string) (*NatsObjectStore, error) {
	configuration := jetstream.ObjectStoreConfig{
		Bucket:      bucketName,
		Description: fmt.Sprintf("Storage for the %s bucket.", bucketName),
		Storage:     jetstream.FileStorage,
		Replicas:    1,
	}

	store, creationError := jetStreamContext.CreateObjectStore(requestContext, configuration)
	if creationError != nil {
		// If bucket exists, standard CreateObjectStore might fail depending on client version/config.
		// We explicitly handle the bind fallback.
		// Note: jetstream.ErrBucketExists checks usually require exact error matching.
		var lookupError error
		store, lookupError = jetStreamContext.ObjectStore(requestContext, bucketName)
		if lookupError != nil {
			return nil, fmt.Errorf("failed to bind to existing object store bucket '%s': %w", bucketName, lookupError)
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
func (objectStore *NatsObjectStore) Download(requestContext context.Context, objectKey string) ([]byte, error) {
	objectEntry, retrievalError := objectStore.objectStore.Get(requestContext, objectKey)
	if retrievalError != nil {
		return nil, fmt.Errorf("failed to get object '%s' from bucket '%s': %w", objectKey, objectStore.bucketName, retrievalError)
	}

	// Ensure resource cleanup regardless of read success.
	defer func() {
		// Explicitly ignore close error as we cannot log it here and it should not overwrite the primary error.
		_ = objectEntry.Close()
	}()

	data, readError := io.ReadAll(objectEntry)
	if readError != nil {
		return nil, fmt.Errorf("failed to read object body for '%s': %w", objectKey, readError)
	}

	return data, nil
}

// Upload stores the provided data under the specified key.
func (objectStore *NatsObjectStore) Upload(requestContext context.Context, objectKey string, data []byte) error {
	dataReader := bytes.NewReader(data)
	metadata := jetstream.ObjectMeta{
		Name: objectKey,
	}

	_, uploadError := objectStore.objectStore.Put(requestContext, metadata, dataReader)
	if uploadError != nil {
		return fmt.Errorf("failed to put object '%s' into bucket '%s': %w", objectKey, objectStore.bucketName, uploadError)
	}

	return nil
}
