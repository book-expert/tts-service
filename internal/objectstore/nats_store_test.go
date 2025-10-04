// Package objectstore_test tests the NATS object store implementation.
package objectstore_test

import (
    "context"
    "testing"

    "github.com/book-expert/tts-service/internal/objectstore"
    "github.com/nats-io/nats-server/v2/server"
    "github.com/nats-io/nats-server/v2/test"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"
    "github.com/stretchr/testify/require"
)

// StartTestServer starts an in-memory NATS server for testing purposes.
func StartTestServer(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()

	opts := test.DefaultTestOptions
	opts.Port = -1 // Use a random port
	opts.JetStream = true
	natsServer := test.RunServer(&opts)

	natsConnection, err := nats.Connect(natsServer.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to test NATS server: %v", err)
	}

	return natsServer, natsConnection
}

func TestNatsObjectStore_UploadDownload(t *testing.T) {
	t.Parallel()

	// 1. Setup
	natsServer, natsConnection := StartTestServer(t)
	defer natsServer.Shutdown()
	defer natsConnection.Close()

	jetstreamContext, err := jetstream.New(natsConnection)
	require.NoError(t, err)

	bucketName := "test-bucket"
	store, err := objectstore.New(context.Background(), jetstreamContext, bucketName)
	require.NoError(t, err)

	// 2. Test Data
	ctx := context.Background()
	key := "my-test-object"
	uploadData := []byte("hello world, this is a test")

	// 3. Upload
	err = store.Upload(ctx, key, uploadData)
	require.NoError(t, err)

	// 4. Download
	downloadData, err := store.Download(ctx, key)
	require.NoError(t, err)

	// 5. Assert
	require.Equal(t, uploadData, downloadData)
}
