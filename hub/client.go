package hub

import (
	"context"
	"io"

	"github.com/ankur-anand/pubhub/proto/gen/v1/hub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Client will subscribe to the upstream hub over the provided grpc conn.
type Client struct {
	// namespaces to subscribe to.
	namespaces []string
	grpcConn   *grpc.ClientConn
	cancelCtx  context.CancelFunc
	conditions []byte
}

// NewClient returns a new initialized client which connect to hub server,
// for Pub Sub Mechanism.
func NewClient(namespace []string, conditions []byte, conn *grpc.ClientConn) *Client {
	return &Client{
		namespaces: namespace,
		grpcConn:   conn,
		conditions: conditions,
	}
}

// Subscribe the given Client to upstream hub for receiving published messages.
// The channel on which message would be received should have enough buffer
// to hold for fast sender hub.
// It's a blocking API.
func (c *Client) Subscribe(ctx context.Context, msgRcv chan<- *hub.KV) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// make this stream clean exit
	c.cancelCtx = cancel

	client := hub.NewPubSubServiceClient(c.grpcConn)
	subscribed, err := client.Subscribe(ctx, &hub.SubscriptionRequest{
		Namespaces: c.namespaces,
		Conditions: c.conditions,
	})
	if err != nil {
		return err
	}
	for {
		msg, err := subscribed.Recv()
		if err == io.EOF {
			return nil

		}
		if err != nil {
			// we don't know what happened
			errStatus := status.Convert(err)
			if errStatus.Code() == codes.Unavailable || errStatus.Code() == codes.Canceled {
				// client cancelled the request.
				return nil
			}
			return err
		}

		if msg != nil {
			msgRcv <- msg
		}
	}
}

// Publish the provided kv to the hub server.
func (c *Client) Publish(ctx context.Context, kv *hub.KV) error {
	client := hub.NewPubSubServiceClient(c.grpcConn)
	_, err := client.Publish(ctx, kv)
	if err != nil {
		return err
	}
	return nil
}

// PublishList publishes received message over the sender channel to the hub server
// until the ctx is cancelled/done or msgSender is closed.
func (c *Client) PublishList(ctx context.Context, msgSender <-chan *hub.KV) error {
	client := hub.NewPubSubServiceClient(c.grpcConn)
	sender, err := client.PublishList(ctx)
	if err != nil {
		return err
	}

	defer sender.CloseSend()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-msgSender:
			// caller closed the channel
			if msg == nil {
				return nil
			}
			err := sender.Send(msg)
			if err != nil {
				return err
			}
		}
	}
}

// UnSubscribe the current client from the upstream hub.
func (c *Client) UnSubscribe() {
	if c.cancelCtx != nil {
		c.cancelCtx()
	}
}
