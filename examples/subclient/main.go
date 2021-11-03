package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ankur-anand/pubhub/hub"
	pbhub "github.com/ankur-anand/pubhub/proto/gen/v1/hub"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "127.0.0.1:9090", "address of hub server")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	conn, err := grpc.DialContext(ctx, *addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	client := hub.NewClient([]string{"*"}, nil, conn)
	msgCh := make(chan *pbhub.KV, 100)
	errCh := make(chan error)
	go func() {
		err := client.Subscribe(ctx, msgCh)
		errCh <- err
	}()

	for {
		select {
		case msg := <-msgCh:
			log.Println(msg.Key, msg.Id, msg.Value)
		case err := <-errCh:
			log.Fatal(err)
		case <-ctx.Done():
			stop()
			log.Println("closing")
			return
		}
	}
}
