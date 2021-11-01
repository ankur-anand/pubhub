package hub

import (
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/pubhub/proto/gen/v1/hub"
)

var wgPool = sync.Pool{New: func() interface{} { return new(sync.WaitGroup) }}

type subscriber chan *hub.KV

// filter if the given namespace is subscribed by the client.
type namespaceFilterFunc func(msg *hub.KV) bool

// newNameSpaceFilterer returns a function that validates if the given msg should be
// published to provided subscriber or not.
func newNameSpaceFilterer(condition Conditions, request *hub.SubscriptionRequest) namespaceFilterFunc {
	return func(msg *hub.KV) bool {
		for _, namespace := range request.Namespaces {
			if condition != nil && !condition.Fulfills(&Request{
				Key:      msg.Key,
				Metadata: msg.Metadata,
			}) {
				return false
			}
			if strings.EqualFold(namespace, msg.Namespace) || namespace == "*" {
				return true
			}
		}

		return false
	}
}

// publisherChannel is basic pub/sub structure. Can be safely used from multiple goroutines.
type publisherChannel struct {
	m           sync.RWMutex
	buffer      int
	timeout     time.Duration
	subscribers map[subscriber]namespaceFilterFunc
}

// Subscribe adds a new subscriber to the publisherChannel.
func (p *publisherChannel) Subscribe(filterFunc namespaceFilterFunc) chan *hub.KV {
	ch := make(chan *hub.KV, p.buffer)
	p.m.Lock()
	p.subscribers[ch] = filterFunc
	p.m.Unlock()
	return ch
}

// newPublisher creates a new pub/sub publisherChannel to broadcast messages.
// The duration is used as send timeout as to not block the publisherChannel publishing
// messages to other clients if one client is slow or unresponsive.
// The buffer is used when creating new channels for subscribers.
func newPublisher(publishTimeout time.Duration, buffer int) *publisherChannel {
	return &publisherChannel{
		buffer:      buffer,
		timeout:     publishTimeout,
		subscribers: make(map[subscriber]namespaceFilterFunc),
	}
}

// Len returns the number of subscribers for the publisherChannel
func (p *publisherChannel) Len() int {
	p.m.RLock()
	i := len(p.subscribers)
	p.m.RUnlock()
	return i
}

// Evict removes the specified subscriber from receiving new messages.
func (p *publisherChannel) Evict(sub chan *hub.KV) {
	p.m.Lock()
	_, exists := p.subscribers[sub]
	if exists {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}

// Publish sends the data in v to all subscribers currently registered with the publisherChannel.
func (p *publisherChannel) Publish(msg *hub.KV) {
	p.m.RLock()
	if len(p.subscribers) == 0 {
		p.m.RUnlock()
		return
	}

	wg := wgPool.Get().(*sync.WaitGroup)
	for sub, filter := range p.subscribers {
		wg.Add(1)
		// TODO: Make this bounded
		// either using go pool?
		go p.sendToNameSpace(sub, filter, msg, wg)
	}
	wg.Wait()
	wgPool.Put(wg)
	p.m.RUnlock()
}

func (p *publisherChannel) sendToNameSpace(sub subscriber, filterFunc namespaceFilterFunc, msg *hub.KV, wg *sync.WaitGroup) {
	defer wg.Done()
	if filterFunc != nil && !filterFunc(msg) {
		return
	}

	// send under a select as to not block if the receiver is unavailable
	if p.timeout > 0 {
		timeout := time.NewTimer(p.timeout)
		defer timeout.Stop()

		select {
		case sub <- msg:
		case <-timeout.C:
		}
		return
	}

	select {
	case sub <- msg:
	default:
	}
}

// Close closes the channels to all subscribers registered with the publisherChannel.
func (p *publisherChannel) Close() {
	p.m.Lock()
	for sub := range p.subscribers {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}
