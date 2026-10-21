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
		SenderPerMinute:     config.DefaultWebhookBridgeSenderPerMinute,
		SenderBurst:         config.DefaultWebhookBridgeSenderBurst,
		GlobalPerMinute:     config.DefaultWebhookBridgeGlobalPerMinute,
		GlobalBurst:         config.DefaultWebhookBridgeGlobalBurst,
		MaxSenders:          config.DefaultWebhookBridgeMaxSenders,
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

// readCounter is a request body that counts its reads.
type readCounter struct {
	*strings.Reader
	reads int
}

func (r *readCounter) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

// TestARequestTheGatewayNoLongerAdmitsIsNeverReadOrForwarded refuses
// admission, as once the gateway has decided to stop: the request is
// answered 503, its body is never read, and nothing is signed or forwarded.
// An admitted request is released once it is answered, and not before: it is
// still held while it is being forwarded.
func TestARequestTheGatewayNoLongerAdmitsIsNeverReadOrForwarded(t *testing.T) {
	called, admitting, released, releasedWhileForwarding := false, false, 0, -1
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
		releasedWhileForwarding = released
	}))
	defer target.Close()
	bridge := newTestServer(t, target.URL)
	bridge.admit = func() (func(), bool) {
		if !admitting {
			return nil, false
		}
		return func() { released++ }, true
	}
	body := &readCounter{Reader: strings.NewReader("from=%2B2348000000000&text=YES")}
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", body)
	req.RemoteAddr = "127.0.0.1:55123"
	rr := httptest.NewRecorder()
	bridge.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if body.reads != 0 || called {
		t.Fatalf("a refused request was read %d times and forwarded %v, want neither", body.reads, called)
	}

	admitting = true
	req = httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", strings.NewReader("from=%2B2348000000000&text=YES"))
	req.RemoteAddr = "127.0.0.1:55123"
	rr = httptest.NewRecorder()
	bridge.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !called {
		t.Fatalf("an admitted request = %d, forwarded %v; want 200 and forwarded", rr.Code, called)
	}
	if released != 1 || releasedWhileForwarding != 0 {
		t.Fatalf("an admitted request was released %d times, %d of them while it was forwarded; want once, after it was answered", released, releasedWhileForwarding)
	}
}
