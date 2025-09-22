// main package for the tts-service
package main

import (
	"github.com/nats-io/nats.go"
)

// jetstream is an interface that defines the methods used by the tts-service
// from the nats.JetStreamContext.
type jetstream interface {
	nats.JetStreamContext
}
