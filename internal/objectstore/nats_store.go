// Package objectstore provides a NATS-based implementation of the ObjectStore interface.
package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/nats-io/nats.go/jetstream"
)

// NatsObjectStore implements the core.ObjectStore interface using NATS JetStream.
// This struct is responsible for all the interactions with the NATS object store.
type NatsObjectStore struct {
	jetstreamContext jetstream.JetStream
	bucket           string
	store            jetstream.ObjectStore
}

// New creates and initializes a new NatsObjectStore. This function is the
// designated constructor for the NatsObjectStore struct and ensures that the
// object store is initialized with a valid bucket.
func New(ctx context.Context, jetstreamContext jetstream.JetStream, bucketName string) (*NatsObjectStore, error) {
	// Use a "create-first" approach.
	store, err := jetstreamContext.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      bucketName,
		Description: fmt.Sprintf("Storage for the %s bucket.", bucketName),
		TTL:         0,
		MaxBytes:    0,
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		Placement:   nil,
		Metadata:    nil,
		Compression: false,
	})

	// If the bucket already exists, bind to it.
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketExists) {
			store, err = jetstreamContext.ObjectStore(ctx, bucketName)
			if err != nil {
				return nil, fmt.Errorf("failed to bind to existing object store bucket '%s': %w", bucketName, err)
			}
		} else {
			// For any other error, fail.
			return nil, fmt.Errorf("failed to create object store bucket '%s': %w", bucketName, err)
		}
	}

	return &NatsObjectStore{
		jetstreamContext: jetstreamContext,
		bucket:           bucketName,
		store:            store,
	}, nil
}

// Download retrieves an object from the NATS object store. This function is
// responsible for all the logic related to downloading an object from the NATS
// object store.
func (n *NatsObjectStore) Download(ctx context.Context, key string) ([]byte, error) {
	obj, err := n.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object '%s' from bucket '%s': %w", key, n.bucket, err)
	}

	data, readErr := io.ReadAll(obj)
	closeErr := obj.Close()

	if readErr != nil {
		return nil, fmt.Errorf("failed to read object '%s': %w", key, readErr)
	}

	if closeErr != nil {
		return data, fmt.Errorf("failed to close object '%s': %w", key, closeErr)
	}

	return data, nil
}

// Upload saves an object to the NATS object store. This function is responsible
// for all the logic related to uploading an object to the NATS object store.
func (n *NatsObjectStore) Upload(ctx context.Context, key string, data []byte) error {
	reader := bytes.NewReader(data)

	_, err := n.store.Put(ctx, jetstream.ObjectMeta{
		Name:        key,
		Description: "",
		Headers:     nil,
		Metadata:    nil,
		Opts:        nil,
	}, reader)
	if err != nil {
		return fmt.Errorf("failed to put object '%s' to bucket '%s': %w", key, n.bucket, err)
	}

	return nil
}
