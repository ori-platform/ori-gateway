// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"
)

// HealthHandler serves GET /health responses containing the current SiteHealth projection.
// It does not perform authentication; callers are responsible for network-layer access control.
type HealthHandler struct {
	viewer      Viewer
	gatewayView func() GatewayView
	listenAddr  string
	now         func() time.Time
}

// NewHealthHandler constructs a HealthHandler.
func NewHealthHandler(viewer Viewer, gatewayView func() GatewayView, listenAddr string, now func() time.Time) *HealthHandler {
	if now == nil {
		now = time.Now
	}
	return &HealthHandler{viewer: viewer, gatewayView: gatewayView, listenAddr: listenAddr, now: now}
}

// ServeHTTP responds to GET /health with the current SiteHealth projection as JSON.
// Requests for any other path receive 404; all non-GET methods receive 405.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/health" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	projection := h.viewer.Project(h.now(), h.gatewayView())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(projection)
}

// Run starts the HTTP server on h.listenAddr and blocks until ctx is cancelled or the server fails.
func (h *HealthHandler) Run(ctx context.Context) error {
	l, err := net.Listen("tcp", h.listenAddr)
	if err != nil {
		return err
	}
	return h.runOnListener(ctx, l)
}

// runOnListener serves on an externally-created listener. Used by tests to bind on :0.
func (h *HealthHandler) runOnListener(ctx context.Context, l net.Listener) error {
	mux := http.NewServeMux()
	mux.Handle("/health", h)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}
