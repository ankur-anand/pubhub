package hub

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/pubhub/proto/gen/v1/hub"
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

// DurationKind represents what kind of duration has been sent.
type DurationKind int

// HookMetadata contains some embedded information for the hook.
type HookMetadata struct {
	DurationKind
	// if this was error
	Err error
}

const (
	// Broadcast duration
	Broadcast DurationKind = iota
	// Stream duration is overall stream duration
	Stream
)

// Hooker implementation provides hook methods that gets called during
// the pub and sub operations.
type Hooker interface {
	PubHook(msg *hub.KV)
	SubHook(ops SubscriberOPS)
	Duration(metadata HookMetadata, d time.Duration)
}

// pubSub Hub implements Publish and Subscriber model over gRPC Stream.
type pubSub struct {
	atomicClock int64
	pub         *publisherChannel
	hooker      Hooker
	// shutdown operations
	shutdownCH   chan struct{}
	shutdownLock sync.Mutex
	shutdownDone bool
	hub.UnimplementedPubSubServiceServer
}

func (p *pubSub) Subscribe(request *hub.SubscriptionRequest, server hub.PubSubService_SubscribeServer) error {

	if len(request.Namespaces) == 0 {
		return status.Errorf(codes.InvalidArgument, "cannot subscribe to empty namespace")
	}
	var cs Conditions
	if request.Conditions != nil {
		cs = make(Conditions)
		err := cs.UnmarshalJSON(request.Conditions)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid condition objects %v", err)
		}
	}

	filterFunc := newNameSpaceFilterer(cs, request)
	streamer := p.pub.Subscribe(filterFunc)
	p.hooker.SubHook(SubscriberCreate)
	// block until downstream cancel this request.
	defer p.pub.Evict(streamer)
	defer p.hooker.SubHook(SubscriberDelete)
	start := time.Now()
	defer p.hooker.Duration(HookMetadata{Err: nil, DurationKind: Stream}, time.Since(start))
	ctx := server.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.shutdownCH:
			// server shutting down
			return nil
		case msg := <-streamer:
			bs := time.Now()
			err := server.Send(msg)
			p.hooker.Duration(HookMetadata{DurationKind: Broadcast, Err: err}, time.Since(bs))
			if err != nil {
				return err
			}
		}
	}
}

func (p *pubSub) Publish(ctx context.Context, kv *hub.KV) (*hub.PublishResponse, error) {
	p.broadcast(kv)
	return emptyPubResponse, nil
}

func (p *pubSub) PublishList(server hub.PubSubService_PublishListServer) error {

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
