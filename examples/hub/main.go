package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ankur-anand/pubhub/examples/hooker"
	"github.com/ankur-anand/pubhub/hub"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 9090, "port of the rpc server")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hs := &hubServer{}

	setupFn := []func(ctx context.Context) error{
		hs.init,
		hs.setupCMux,
		hs.setupServer,
		hs.startServers,
	}

	for _, fn := range setupFn {
		if err := fn(ctx); err != nil {
			if hs.log != nil {
				hs.log.Fatal("error starting server", zap.Error(err))
			}
			log.Fatal(err)
		}
	}
	hs.log.Info("hub: server started", zap.Int("port", *port))
	<-ctx.Done()
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := hs.StopServer(ctx)
	if err != nil {
		hs.log.Error("error stopping the server", zap.Error(err))
	}
	hs.log.Info("server stopped")
}

// An Example hub server for initialization
type hubServer struct {
	log       *zap.Logger
	rpcServer *grpc.Server
	hub       *hub.Hub
	cmux      cmux.CMux
	httpSrv   *http.Server
	// deferCallback are called when services are shutting down
	deferCallback []func(ctx context.Context)
}

func (h *hubServer) init(ctx context.Context) error {
	flag.Parse()
	l, err := zap.NewProduction()
	if err != nil {
		return err
	}
	h.log = l
	return nil
}

func (h *hubServer) setupCMux(ctx context.Context) error {
	rpcAddr := fmt.Sprintf(
		":%d",
		*port,
	)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	h.cmux = cmux.New(ln)
	return nil
}

func (h *hubServer) setupServer(ctx context.Context) error {
	h.rpcServer = grpc.NewServer()
	hub, err := hub.NewHub(h.rpcServer, hub.WithHooker(hooker.PromHook{}))
	if err != nil {
		return err
	}
	h.hub = hub
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	h.httpSrv = &http.Server{Handler: mux}
	return nil
}

func (h *hubServer) startServers(ctx context.Context) error {
	grpcLn := h.cmux.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := h.cmux.Match(cmux.HTTP1())

	go func() {
		if err := h.rpcServer.Serve(grpcLn); err != nil {
			if err == cmux.ErrListenerClosed || err == cmux.ErrServerClosed {
				h.log.Info("pubhub: gRPC Hub closed")
				return
			}
			h.log.Fatal("pubhub: gRPC server failed to serve", zap.Error(err))
		}
	}()

	go func() {
		err := h.httpSrv.Serve(httpL)
		if err != nil {
			if err == cmux.ErrListenerClosed || err == cmux.ErrServerClosed {
				h.log.Info("pubhub: HTTP API Hub stopped")
				return
			}
			h.log.Fatal("pubhub: HTTP API Hub cannot be started", zap.Error(err))
		}
	}()

	go func() {
		err := h.cmux.Serve()
		if err != nil {
			if err == cmux.ErrListenerClosed || err == cmux.ErrServerClosed {
				h.log.Info("pubhub: CMux stopped")
				return
			}
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			h.log.Fatal("alpha: errored serving cmux", zap.Error(err))
		}
	}()

	return nil
}

func (h *hubServer) StopServer(ctx context.Context) error {
	doneCh := make(chan struct{})
	go func() {
		h.cmux.Close()
		h.rpcServer.GracefulStop()
		err := h.httpSrv.Shutdown(ctx)
		if err != nil {
			h.log.Error("error closing down http Hub", zap.Error(err))
		}
		close(doneCh)
	}()
	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()

	}
}
