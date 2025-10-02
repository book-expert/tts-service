package worker

import (
	"github.com/nats-io/nats.go"
)

// JetStreamContext defines the interface for interacting with NATS JetStream.
// This interface is a subset of the nats.JetStreamContext interface, containing
// only the methods used by the NatsWorker.
type JetStreamContext interface {
	Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
}
