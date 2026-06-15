// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestHandler(t *testing.T, status string) *HealthHandler {
	t.Helper()
	reg := NewRegistry()
	viewer := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{}, NodeTTLMS: 5000})
	return NewHealthHandler(viewer, func() GatewayView {
		return GatewayView{Status: status, ProviderName: "test"}
	}, "127.0.0.1:0", time.Now)
}

func TestHealthHandlerGETReturnsJSON(t *testing.T) {
	h := newTestHandler(t, SiteStatusHealthy)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json Content-Type, got %q", ct)
	}
	var out SiteHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not valid JSON: %v\nbody: %s", err, rec.Body.String())
	}
}

func TestHealthHandlerMethodNotAllowed(t *testing.T) {
	h := newTestHandler(t, SiteStatusHealthy)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/health", nil)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: expected 405, got %d", method, rec.Code)
		}
	}
}

func TestHealthHandlerProjectionStatusInBody(t *testing.T) {
	for _, tc := range []struct {
		gatewayStatus string
		wantSite      string
	}{
		{SiteStatusHealthy, SiteStatusHealthy},
		{SiteStatusDegraded, SiteStatusDegraded},
		{"starting", SiteStatusDegraded},
	} {
		h := newTestHandler(t, tc.gatewayStatus)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

		var out SiteHealth
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("gateway status %q: bad JSON: %v", tc.gatewayStatus, err)
		}
		if out.Status != tc.wantSite {
			t.Errorf("gateway status %q: want site %q, got %q", tc.gatewayStatus, tc.wantSite, out.Status)
		}
	}
}

func TestHealthHandlerNoSecretsInResponse(t *testing.T) {
	h := newTestHandler(t, SiteStatusHealthy)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	body := strings.ToLower(rec.Body.String())
	for _, forbidden := range []string{"mqtt://", "mqtts://", "password", "secret", "token", "hmac"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("response contains forbidden pattern %q: %s", forbidden, rec.Body.String())
		}
	}
}

func TestHealthHandlerGeneratedAtMSIsPopulated(t *testing.T) {
	h := newTestHandler(t, SiteStatusHealthy)
	before := time.Now().UnixMilli()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	after := time.Now().UnixMilli()

	var out SiteHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.GeneratedAtMS < before || out.GeneratedAtMS > after {
		t.Errorf("generated_at_ms %d not in [%d, %d]", out.GeneratedAtMS, before, after)
	}
}

func TestHealthHandlerRunStartsAndStopsCleanly(t *testing.T) {
	reg := NewRegistry()
	viewer := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{}, NodeTTLMS: 5000})
	h := NewHealthHandler(viewer, func() GatewayView {
		return GatewayView{Status: SiteStatusHealthy, ProviderName: "test"}
	}, "127.0.0.1:0", time.Now)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.runOnListener(ctx, l) }()

	// poll until server is ready
	deadline := time.Now().Add(2 * time.Second)
	var resp *http.Response
	for time.Now().Before(deadline) {
		resp, err = http.Get(fmt.Sprintf("http://%s/health", addr)) //nolint:noctx
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatalf("server did not become ready: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	var out SiteHealth
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("invalid JSON from live server: %v\nbody: %s", err, body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error on clean shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestHealthHandlerRunBindFailure(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	defer l.Close() // keep the port occupied

	reg := NewRegistry()
	viewer := NewProjector(reg, ProjectOptions{})
	h := NewHealthHandler(viewer, func() GatewayView { return GatewayView{} }, addr, time.Now)

	err = h.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when port is already in use")
	}
}
