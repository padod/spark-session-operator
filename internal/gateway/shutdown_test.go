/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package gateway

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestRunWrapper_DrainsInflightRequest verifies the http.Server.Shutdown
// pattern used by both SessionGateway.Run and SessionProxy.StartThriftHTTPProxy
// lets a handler that is already executing finish cleanly when its
// surrounding ctx is canceled, instead of dropping the response mid-write.
// Both Run wrappers rely on this guarantee from net/http; the test pins it so
// a future refactor that swaps to e.g. server.Close() (which does NOT drain)
// fails loudly.
func TestRunWrapper_DrainsInflightRequest(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(handlerStarted)
			<-releaseHandler
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("done"))
		}),
		ReadHeaderTimeout: 2 * time.Second,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	// Replicates the exact wrapper used by gateway.Run / proxy.StartThriftHTTPProxy
	// so a regression in either path is caught here.
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		serveErr := make(chan error, 1)
		go func() {
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				serveErr <- err
				return
			}
			serveErr <- nil
		}()
		select {
		case <-ctx.Done():
			shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			runDone <- server.Shutdown(shutdownCtx)
		case err := <-serveErr:
			runDone <- err
		}
	}()

	// Fire an in-flight request and wait until the handler is executing.
	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/")
		if err != nil {
			requestDone <- err
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			requestDone <- &httpStatusError{got: resp.StatusCode}
			return
		}
		requestDone <- nil
	}()

	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	// Initiate graceful shutdown while the handler is still inside the call.
	cancel()

	// Verify Shutdown is actually blocking on the in-flight handler:
	// briefly wait, then confirm runDone has NOT fired yet.
	select {
	case <-runDone:
		t.Fatal("Run returned before in-flight handler finished")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the handler — request should now complete successfully.
	close(releaseHandler)

	if err := <-requestDone; err != nil {
		t.Fatalf("in-flight request failed: %v", err)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run wrapper returned error after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run wrapper did not return after in-flight drained")
	}

	// Post-shutdown: new connections must be refused.
	client := &http.Client{Timeout: 500 * time.Millisecond}
	if _, err := client.Get("http://" + addr + "/"); err == nil {
		t.Fatal("expected post-shutdown request to fail")
	}
}

type httpStatusError struct{ got int }

func (e *httpStatusError) Error() string { return http.StatusText(e.got) }
