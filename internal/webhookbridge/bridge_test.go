// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package webhookbridge

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
)

func testConfig(targetURL string) config.WebhookBridgeConfig {
	return config.WebhookBridgeConfig{
		Enabled:             true,
		ListenAddr:          "127.0.0.1:8090",
		Path:                "/webhooks/sms/africastalking",
		TargetURL:           targetURL,
		ProviderSourceCIDRs: []string{"127.0.0.1/32"},
		RuntimeTokenEnv:     "RUNTIME_TOKEN",
		HMACSecretEnv:       "WEBHOOK_HMAC_SECRET",
		RequestTimeoutMS:    1000,
		MaxBodyBytes:        64,
	}
}

func newTestServer(t *testing.T, targetURL string) *Server {
	t.Helper()
	s, err := New(testConfig(targetURL), Options{
		Now: func() time.Time { return time.UnixMilli(1_700_000_000_123) },
		Nonce: func() (string, error) {
			return "nonce-1", nil
		},
		Getenv: func(name string) string {
			switch name {
			case "RUNTIME_TOKEN":
				return "runtime-token"
			case "WEBHOOK_HMAC_SECRET":
				return "bridge-secret"
			default:
				return ""
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBridgeSignsAndForwardsRawBody(t *testing.T) {
	body := []byte("from=%2B2348000000000&text=YES-AB12CD34")
	var gotBody string
	var gotSignature string
	var gotToken string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		gotBody = string(payload)
		gotSignature = r.Header.Get("X-Ori-Webhook-Signature")
		gotToken = r.Header.Get("X-Ori-Webhook-Token")
		if r.Header.Get("X-Ori-Webhook-Timestamp") != "1700000000123" {
			t.Fatalf("unexpected timestamp: %q", r.Header.Get("X-Ori-Webhook-Timestamp"))
		}
		if r.Header.Get("X-Ori-Webhook-Nonce") != "nonce-1" {
			t.Fatalf("unexpected nonce: %q", r.Header.Get("X-Ori-Webhook-Nonce"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	bridge := newTestServer(t, target.URL)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", strings.NewReader(string(body)))
	req.RemoteAddr = "127.0.0.1:55123"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	bridge.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q", rr.Code, rr.Body.String())
	}
	if gotBody != string(body) {
		t.Fatalf("forwarded body changed: %q", gotBody)
	}
	if gotToken != "runtime-token" {
		t.Fatalf("runtime token = %q", gotToken)
	}
	expected := signBody(body, "bridge-secret", 1_700_000_000_123, "nonce-1")
	if gotSignature != expected {
		t.Fatalf("signature = %q want %q", gotSignature, expected)
	}
}

func TestBridgeRejectsDisallowedSource(t *testing.T) {
	called := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer target.Close()
	bridge := newTestServer(t, target.URL)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", strings.NewReader("from=x"))
	req.RemoteAddr = "10.0.0.1:55123"
	rr := httptest.NewRecorder()

	bridge.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rr.Code)
	}
	if called {
		t.Fatal("target should not be called for disallowed source")
	}
}

func TestBridgeRejectsOversizedBody(t *testing.T) {
	called := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer target.Close()
	bridge := newTestServer(t, target.URL)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", strings.NewReader(strings.Repeat("x", 65)))
	req.RemoteAddr = "127.0.0.1:55123"
	rr := httptest.NewRecorder()

	bridge.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rr.Code)
	}
	if called {
		t.Fatal("target should not be called for oversized body")
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("tcp reset") }
func (failingReader) Close() error             { return nil }

func TestBridgeReturnsBadRequestForBodyReadFailure(t *testing.T) {
	called := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer target.Close()
	bridge := newTestServer(t, target.URL)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", failingReader{})
	req.RemoteAddr = "127.0.0.1:55123"
	rr := httptest.NewRecorder()

	bridge.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rr.Code)
	}
	if called {
		t.Fatal("target should not be called after body read failure")
	}
}

func TestBridgeMapsRuntimeFailureToBadGateway(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer target.Close()
	bridge := newTestServer(t, target.URL)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", strings.NewReader("from=x"))
	req.RemoteAddr = "127.0.0.1:55123"
	rr := httptest.NewRecorder()

	bridge.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestBridgeRejectsWrongMethodAndPath(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer target.Close()
	bridge := newTestServer(t, target.URL)

	wrongPath := httptest.NewRequest(http.MethodPost, "/wrong", strings.NewReader("from=x"))
	wrongPath.RemoteAddr = "127.0.0.1:55123"
	wrongPathResp := httptest.NewRecorder()
	bridge.ServeHTTP(wrongPathResp, wrongPath)
	if wrongPathResp.Code != http.StatusNotFound {
		t.Fatalf("wrong path status = %d", wrongPathResp.Code)
	}

	wrongMethod := httptest.NewRequest(http.MethodGet, "/webhooks/sms/africastalking", nil)
	wrongMethod.RemoteAddr = "127.0.0.1:55123"
	wrongMethodResp := httptest.NewRecorder()
	bridge.ServeHTTP(wrongMethodResp, wrongMethod)
	if wrongMethodResp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method status = %d", wrongMethodResp.Code)
	}
}

func TestNewFailsWhenSecretsMissing(t *testing.T) {
	_, err := New(testConfig("http://127.0.0.1:8080/webhook"), Options{Getenv: func(string) string { return "" }})
	if err == nil || !strings.Contains(err.Error(), "runtime_token_env") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	cfg := testConfig("http://127.0.0.1:1/webhook")
	cfg.ListenAddr = "127.0.0.1:0"
	bridge := newTestServer(t, cfg.TargetURL)
	bridge.listenAddr = cfg.ListenAddr
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bridge.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}
