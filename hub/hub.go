package hub

import (
	"fmt"
	"time"

	"github.com/ankur-anand/pubhub/proto/gen/v1/hub"
	"google.golang.org/grpc"
)

const (
	publisherTimeout = 2 * time.Second
	publisherBuffer  = 10
)

// NoopHook implements Hooker interface and does nothing.
type NoopHook struct {
}

func (n NoopHook) Duration(metadata HookMetadata, d time.Duration) {
}

func (n NoopHook) PubHook(msg *hub.KV) {
}

func (n NoopHook) SubHook(ops SubscriberOPS) {
}

// Hub provides gRPC implementation of Publish/Subscribe messaging paradigm.
type Hub struct {
	pubTimeout time.Duration
	pubBuffer  int
	hooker     Hooker
	pubSub     *pubSub
}

// Options Configure the Hub with different parameters.
type Options func(*Hub)

// WithPublisherTimeout configures the hub with timeout duration
// that hub respect before timing out the publishing at server.
func WithPublisherTimeout(d time.Duration) Options {
	return func(h *Hub) {
		h.pubTimeout = d
	}
}

// WithSubscriberBufferCount configures the hub with the count
// which is then used to create a buffer channel for each subscriber
// that connect with hub server.
func WithSubscriberBufferCount(count int) Options {
	return func(h *Hub) {
		h.pubBuffer = count
	}
}

// WithHooker configures the hub to use the passed Hooker
// implementation.
func WithHooker(hooker Hooker) Options {
	return func(h *Hub) {
		h.hooker = hooker
	}
}

func (h *Hub) setDefaultIfNotSet() {
	if h.hooker == nil {
		h.hooker = NoopHook{}
	}

	if h.pubTimeout == 0 {
		h.pubTimeout = publisherTimeout
	}

	if h.pubBuffer == 0 {
		h.pubBuffer = publisherBuffer
	}
}

// NewHub returns an initialized hub server,
// and register the  gRPC implementation of Publish/Subscribe messaging paradigm
// over the provided grpcServer instance.
func NewHub(grpcServer *grpc.Server, opts ...Options) (*Hub, error) {
	if grpcServer == nil {
		return nil, fmt.Errorf("nil grpc server provided")
	}

	h := &Hub{}

	for _, opt := range opts {
		opt(h)
	}

	h.setDefaultIfNotSet()
	pubSub := &pubSub{
		pub:        newPublisher(h.pubTimeout, h.pubBuffer),
		shutdownCH: make(chan struct{}),
		hooker:     h.hooker,
	}

	h.pubSub = pubSub
	// register services to grpc servers.
	hub.RegisterPubSubServiceServer(grpcServer, pubSub)
	return h, nil
}

// Close closes the Hub
func (h *Hub) Close() error {
	h.pubSub.Close()
	return nil
}

// Publish publishes the given msg on this server for all subscriber.
// It's an async method.
func (h *Hub) Publish(msg *hub.KV) error {
	h.pubSub.broadcast(msg)
	return nil
}
