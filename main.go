// Command sse-relay runs the HTTP server: a publish endpoint for a single
// producer and a Server-Sent Events endpoint for any number of subscribers.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mallen1988/sse-relay/internal/hub"
	"github.com/mallen1988/sse-relay/internal/relay"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	buffer := flag.Int("buffer", hub.DefaultCapacity, "events kept per stream for replay")
	heartbeat := flag.Duration("heartbeat", 15*time.Second, "delay between heartbeat comment frames")
	retry := flag.Duration("retry", 2*time.Second, "reconnect delay advertised in the retry: field")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "grace period for in-flight requests on shutdown")
	flag.Parse()

	h := hub.New(*buffer)
	srv := relay.NewServer(h, relay.Config{
		Heartbeat: *heartbeat,
		RetryHint: *retry,
		Token:     os.Getenv("RELAY_TOKEN"),
	})

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: srv,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("sse-relay listening on %s", *addr)
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err, ok := <-serveErr:
		if ok {
			log.Fatalf("listen: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)

		// Finish every stream before touching the listener, so subscribers
		// still attached get event: done instead of a severed connection.
		h.CloseAll()

		ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
		<-serveErr
	}
}
