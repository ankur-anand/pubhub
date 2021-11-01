package hub

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/pubhub/proto/gen/v1/hub"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	emptyPubResponse = &hub.PublishResponse{}
)

var startTime = time.Now()

// now returns time.Duration using stdlib time
func now() time.Duration { return time.Since(startTime) }

func nowNano() int64 { return time.Since(startTime).Nanoseconds() }

// SubscriberOPS represents what kind of Subscriber Action was performed.
type SubscriberOPS int

const (
	SubscriberCreate SubscriberOPS = 0
	SubscriberDelete SubscriberOPS = 1
)

// Hooker implementation provides hook methods that gets called during
// the pub and sub operations.
type Hooker interface {
	PubHook(msg *hub.KV)
	SubHook(ops SubscriberOPS)
}

// pubSub Server implements Publish and Subscriber model over gRPC Stream.
type pubSub struct {
	atomicClock int64

	log    *zap.Logger
	pub    *publisherChannel
	hooker Hooker
	// shutdown operations
	shutdownCH   chan struct{}
	shutdownLock sync.Mutex
	shutdownDone bool
	hub.UnimplementedPubSubServiceServer
}

func (p *pubSub) Subscribe(request *hub.SubscriptionRequest, server hub.PubSubService_SubscribeServer) error {
	p.log.Info("hub: new subscription request from downstream", zap.Strings("namespaces", request.Namespaces))

	if len(request.Namespaces) == 0 {
		return status.Errorf(codes.InvalidArgument, "cannot subscribe to empty namespace")
	}

	filterFunc := newNameSpaceFilterer(request)
	streamer := p.pub.Subscribe(filterFunc)
	p.hooker.SubHook(SubscriberCreate)
	// block until downstream cancel this request.
	defer p.pub.Evict(streamer)
	defer p.hooker.SubHook(SubscriberDelete)

	ctx := server.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.shutdownCH:
			// server shutting down
			return nil
		case msg := <-streamer:
			err := server.Send(msg)
			if err != nil {
				p.log.Error("hub: error sending to subscriber stream", zap.Error(err))
				return err
			}
		}
	}
}

func (p *pubSub) Publish(ctx context.Context, kv *hub.KV) (*hub.PublishResponse, error) {
	p.log.Debug("hub: new message published")
	p.broadcast(kv)
	return emptyPubResponse, nil
}

func (p *pubSub) PublishList(server hub.PubSubService_PublishListServer) error {
	p.log.Debug("hub: new stream publisherChannel")

	for {
		msg, err := server.Recv()
		if err == io.EOF {
			return nil

		}
		if err != nil {
			// we don't know what happened
			errStatus := status.Convert(err)
			if errStatus.Code() == codes.Unavailable {
				// client cancelled the request.
				return nil
			}

			p.log.Error("hub: publish error on receive stream", zap.Error(err))
			return err
		}

		if msg != nil {
			p.broadcast(msg)
		}
	}
}

func (p *pubSub) Close() {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()
	if p.shutdownDone {
		return
	}

	p.shutdownDone = true
	close(p.shutdownCH)
	p.pub.Close()
}

func (p *pubSub) broadcast(msg *hub.KV) {
	p.hooker.PubHook(msg)
	msg.Id = nowNano() + atomic.AddInt64(&p.atomicClock, 1)
	p.pub.Publish(msg)
}
