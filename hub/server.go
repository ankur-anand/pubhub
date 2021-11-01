package hub

import (
	"time"

	"github.com/ankur-anand/pubhub/proto/gen/v1/hub"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	publisherTimeout = 2 * time.Second
	publisherBuffer  = 10
)

// NoopHook implements Hooker interface and does nothing.
type NoopHook struct {
}

func (n NoopHook) PubHook(msg *hub.KV) {
}

func (n NoopHook) SubHook(ops SubscriberOPS) {
}

// Server Hub.
type Server struct {
	pubSub *pubSub
}

func NewServer(grpcServer *grpc.Server, log *zap.Logger, hooker Hooker) (*Server, error) {

	if hooker == nil {
		hooker = NoopHook{}
	}

	pubSub := &pubSub{
		log:        log,
		pub:        newPublisher(publisherTimeout, publisherBuffer),
		shutdownCH: make(chan struct{}),
		hooker:     hooker,
	}

	// register services to grpc servers.
	hub.RegisterPubSubServiceServer(grpcServer, pubSub)
	return &Server{pubSub: pubSub}, nil
}

// Close closes the Hub Server
func (s *Server) Close() error {
	s.pubSub.Close()
	return nil
}

// Publish publishes the given msg on this server for all subscriber.
// It's an async method.
func (s *Server) Publish(msg *hub.KV) error {
	s.pubSub.broadcast(msg)
	return nil
}
