// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func validReasoningRequest() contracts.ReasoningRequest {
	return contracts.ReasoningRequest{
		RequestID:      "req-1",
		DeviceID:       "site-a",
		SensorType:     "current_clamp",
		TriggerName:    "overcurrent",
		Prompt:         "Explain this reading in plain English.",
		ActionTierHint: contracts.ActionTierC,
		TimeoutMS:      10000,
	}
}

func testClock() func() time.Time {
	calls := int64(0)
	base := time.UnixMilli(1_800_000_000_000)
	return func() time.Time {
		call := atomic.AddInt64(&calls, 1)
		return base.Add(time.Duration(call-1) * 123 * time.Millisecond)
	}
}

func TestLlamaCppProviderCompletion(t *testing.T) {
	var gotPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"model":"site-model.gguf"}`))
		case "/completion":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			gotPrompt = body["prompt"].(string)
			if strings.Contains(gotPrompt, "?stream=1") {
				t.Fatalf("prompt unexpectedly contained URL query: %q", gotPrompt)
			}
			if body["stream"] != false {
				t.Fatalf("stream = %#v, want false", body["stream"])
			}
			_, _ = w.Write([]byte(`{"content":"Reduce generator runtime by checking grid return first.","tokens_predicted":12,"tokens_evaluated":8}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL, ModelFallback: "fallback.gguf", Now: testClock()})
	if err != nil {
		t.Fatal(err)
	}
	req := validReasoningRequest()
	req.Prompt = "Unicode test: ẹrọ ⚡"
	req.Context = contracts.ReasoningContext{
		Value:     247.3,
		Unit:      "A",
		Timestamp: 1_800_000_000_000,
		History: []contracts.HistoryPoint{
			{Value: 220.1, Timestamp: 1_799_999_940_000},
		},
	}
	resp, err := p.Reason(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotPrompt, req.Prompt) {
		t.Fatalf("prompt = %q, want it to contain %q", gotPrompt, req.Prompt)
	}
	for _, want := range []string{"Structured sensor context", "value: 247.3", "unit: A", "timestamp_ms: 1800000000000", "value: 220.1"} {
		if !strings.Contains(gotPrompt, want) {
			t.Fatalf("prompt missing %q: %q", want, gotPrompt)
		}
	}
	if resp.RequestID != req.RequestID || resp.ActionTier != contracts.ActionTierC {
		t.Fatalf("correlation/tier not preserved: %#v", resp)
	}
	if resp.Text != "Reduce generator runtime by checking grid return first." {
		t.Fatalf("text = %q", resp.Text)
	}
	if resp.Model != "site-model.gguf" {
		t.Fatalf("model = %q", resp.Model)
	}
	if resp.TokensUsed != 20 {
		t.Fatalf("tokens = %d, want 20", resp.TokensUsed)
	}
	if resp.LatencyMS != 123 {
		t.Fatalf("latency = %d, want 123", resp.LatencyMS)
	}
	if resp.Confidence != defaultLlamaCppConfidence {
		t.Fatalf("confidence = %f", resp.Confidence)
	}
}

func TestLlamaCppProviderHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			_, _ = w.Write([]byte(`{"model":"m"}`))
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Reason(context.Background(), validReasoningRequest())
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("got %v, want HTTP 500 error", err)
	}
}

func TestLlamaCppProviderPropsFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			http.Error(w, "props down", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"content":"ok","timings":{"predicted_n":3,"prompt_n":4}}`))
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL + "/completion", ModelFallback: "fallback-model", Now: testClock()})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Reason(context.Background(), validReasoningRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "fallback-model" {
		t.Fatalf("model = %q, want fallback", resp.Model)
	}
	if resp.TokensUsed != 7 {
		t.Fatalf("tokens = %d, want timings total", resp.TokensUsed)
	}
}

func TestLlamaCppProviderMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			_, _ = w.Write([]byte(`{"model":"m"}`))
			return
		}
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Reason(context.Background(), validReasoningRequest())
	if err == nil || !strings.Contains(err.Error(), "decode completion response") {
		t.Fatalf("got %v, want decode error", err)
	}
}

func TestLlamaCppProviderMissingContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			_, _ = w.Write([]byte(`{"model":"m"}`))
			return
		}
		_, _ = w.Write([]byte(`{"content":"   "}`))
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Reason(context.Background(), validReasoningRequest())
	if err == nil || !strings.Contains(err.Error(), "missing content") {
		t.Fatalf("got %v, want missing content error", err)
	}
}

func TestLlamaCppProviderContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			_, _ = w.Write([]byte(`{"model":"m"}`))
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.Reason(ctx, validReasoningRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestLlamaCppProviderTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			_, _ = w.Write([]byte(`{"model":"m"}`))
			return
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"content":"late"}`))
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = p.Reason(ctx, validReasoningRequest())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context deadline", err)
	}
}

func TestLlamaCppProviderUnreachableServer(t *testing.T) {
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: "http://127.0.0.1:1", ModelFallback: "fallback"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = p.Reason(ctx, validReasoningRequest())
	if err == nil {
		t.Fatal("expected unreachable server error")
	}
}

func TestLlamaCppProviderRequiresURL(t *testing.T) {
	cases := []string{"", "localhost:8080", "ftp://localhost:8080", "http:///completion"}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, err := NewLlamaCppProvider(LlamaCppOptions{URL: raw}); err == nil {
				t.Fatal("expected URL validation error")
			}
		})
	}
}

func TestLlamaCppProviderDoesNotCallNetworkOnConstruction(t *testing.T) {
	called := atomic.Bool{}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		called.Store(true)
		return nil, errors.New("unexpected network call")
	})}
	if _, err := NewLlamaCppProvider(LlamaCppOptions{URL: "http://localhost:8080", HTTPClient: client}); err != nil {
		t.Fatal(err)
	}
	if called.Load() {
		t.Fatal("constructor must not call network")
	}
}

func TestLlamaCppProviderStripsQueryAndFragmentFromEndpointURLs(t *testing.T) {
	var paths []string
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		queries = append(queries, r.URL.RawQuery)
		switch r.URL.Path {
		case "/completion":
			_, _ = w.Write([]byte(`{"content":"ok"}`))
		case "/props":
			_, _ = w.Write([]byte(`{"model":"m"}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL + "/completion?stream=1#frag"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reason(context.Background(), validReasoningRequest()); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != "/props" || paths[1] != "/completion" {
		t.Fatalf("unexpected paths: %#v", paths)
	}
	for _, query := range queries {
		if query != "" {
			t.Fatalf("expected stripped query strings, got %#v", queries)
		}
	}
}

func TestLlamaCppProviderRequestTimeoutMSAppliesDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			_, _ = w.Write([]byte(`{"model":"m"}`))
			return
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"content":"late"}`))
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	req := validReasoningRequest()
	req.TimeoutMS = 1
	_, err = p.Reason(context.Background(), req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context deadline", err)
	}
}

func TestLlamaCppProviderCachesResolvedModelName(t *testing.T) {
	propsCalls := atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			propsCalls.Add(1)
			_, _ = w.Write([]byte(`{"model":"cached.gguf"}`))
		case "/completion":
			_, _ = w.Write([]byte(`{"content":"ok"}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	p, err := NewLlamaCppProvider(LlamaCppOptions{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		resp, err := p.Reason(context.Background(), validReasoningRequest())
		if err != nil {
			t.Fatal(err)
		}
		if resp.Model != "cached.gguf" {
			t.Fatalf("model = %q", resp.Model)
		}
	}
	if propsCalls.Load() != 1 {
		t.Fatalf("props calls = %d, want 1", propsCalls.Load())
	}
}

func TestLlamaCppTokensUsedPrefersTokenFieldsWhenEitherPresent(t *testing.T) {
	got := llamaCppCompletionResponse{
		TokensPredicted: 10,
		TokensEvaluated: -10,
		Timings: llamaTiming{
			PredictedN: 99,
			PromptN:    99,
		},
	}.tokensUsed()
	if got != 0 {
		t.Fatalf("tokens = %d, want direct token-field sum", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
