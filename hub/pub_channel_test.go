package hub

import (
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/pubhub/proto/gen/v1/hub"
)

func TestSendToOneSub(t *testing.T) {
	p := newPublisher(100*time.Millisecond, 10)
	c := p.Subscribe(func(msg *hub.KV) bool {
		return true
	})
	var heartBeatMSG = &hub.KV{
		Namespace: "test1",
		Key:       "hello",
	}
	p.Publish(heartBeatMSG)

	msg := <-c
	if msg.Namespace != "test1" && msg.Key != "hello" {
		t.Errorf("expected message not received")
	}
}

func TestSendToMultipleSubs(t *testing.T) {
	p := newPublisher(100*time.Millisecond, 10)
	var subs []chan *hub.KV
	subs = append(subs, p.Subscribe(func(msg *hub.KV) bool {
		return true
	}), p.Subscribe(func(msg *hub.KV) bool {
		return true
	}), p.Subscribe(func(msg *hub.KV) bool {
		return true
	}))

	var heartBeatMSG = &hub.KV{
		Namespace: "test1",
		Key:       "hello",
	}
	p.Publish(heartBeatMSG)

	for _, c := range subs {
		msg := <-c
		if msg.Namespace != "test1" && msg.Key != "hello" {
			t.Errorf("expected message not received")
		}
	}
}

func TestEvictOneSub(t *testing.T) {
	p := newPublisher(100*time.Millisecond, 10)
	s1 := p.Subscribe(func(msg *hub.KV) bool {
		return true
	})
	s2 := p.Subscribe(func(msg *hub.KV) bool {
		return true
	})

	p.Evict(s1)
	var heartBeatMSG = &hub.KV{
		Namespace: "test1",
		Key:       "hello",
	}
	p.Publish(heartBeatMSG)
	if _, ok := <-s1; ok {
		t.Errorf("expected s1 to not receive the published message")
	}

	msg := <-s2
	if msg.Namespace != "test1" && msg.Key != "hello" {
		t.Errorf("expected message not received")
	}
}

func TestClosePublisher(t *testing.T) {
	p := newPublisher(100*time.Millisecond, 10)
	var subs []chan *hub.KV
	subs = append(subs, p.Subscribe(func(msg *hub.KV) bool {
		return true
	}), p.Subscribe(func(msg *hub.KV) bool {
		return true
	}), p.Subscribe(func(msg *hub.KV) bool {
		return true
	}))
	p.Close()

	for _, c := range subs {
		if _, ok := <-c; ok {
			t.Fatal("expected all subscriber channels to be closed")
		}
	}
}

type testSubscriber struct {
	dataCh chan *hub.KV
	ch     chan error
}

func (s *testSubscriber) Wait() error {
	return <-s.ch
}

func newTestSubscriber(p *publisherChannel) *testSubscriber {
	ts := &testSubscriber{
		dataCh: p.Subscribe(func(msg *hub.KV) bool {
			return true
		}),
		ch: make(chan error),
	}
	go func() {
		for msg := range ts.dataCh {
			if msg.Namespace != "test1" && msg.Key != "hello" {
				ts.ch <- fmt.Errorf("expected message not received")
				break
			}
		}
		close(ts.ch)
	}()
	return ts
}

var beatMSG = &hub.KV{
	Namespace: "test1",
	Key:       "hello",
}

// for testing with -race
func TestPubSubRace(t *testing.T) {
	p := newPublisher(0, 1024)
	var subs []*testSubscriber
	for j := 0; j < 50; j++ {
		subs = append(subs, newTestSubscriber(p))
	}
	for j := 0; j < 1000; j++ {
		p.Publish(beatMSG)
	}
	time.AfterFunc(1*time.Second, func() {
		for _, s := range subs {
			p.Evict(s.dataCh)
		}
	})
	for _, s := range subs {
		s.Wait()
	}
}

func BenchmarkPubSub(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		p := newPublisher(0, 1024)
		var subs []*testSubscriber
		for j := 0; j < 50; j++ {
			subs = append(subs, newTestSubscriber(p))
		}
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			p.Publish(beatMSG)
		}
		time.AfterFunc(1*time.Second, func() {
			for _, s := range subs {
				p.Evict(s.dataCh)
			}
		})
		for _, s := range subs {
			if err := s.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
