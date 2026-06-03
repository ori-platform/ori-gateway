// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func TestNewFromConfigEcho(t *testing.T) {
	p, err := NewFromConfig(config.ProviderConfig{Name: config.ProviderEcho})
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != config.ProviderEcho {
		t.Fatalf("provider name = %q", p.Name())
	}
	resp, err := p.Reason(context.Background(), contracts.ReasoningRequest{
		RequestID:      "req-1",
		Prompt:         "ping",
		ActionTierHint: contracts.ActionTierA,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.RequestID != "req-1" || resp.ActionTier != contracts.ActionTierA {
		t.Fatalf("echo provider did not preserve correlation/tier: %#v", resp)
	}
}

func TestNewFromConfigLlama(t *testing.T) {
	p, err := NewFromConfig(config.ProviderConfig{
		Name:      config.ProviderLlamaCpp,
		TimeoutMS: 2500,
		LlamaCpp: config.LlamaCppConfig{
			URL:   "http://localhost:8080/completion",
			Model: "fallback.gguf",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	llama, ok := p.(*LlamaCppProvider)
	if !ok {
		t.Fatalf("provider type = %T, want *LlamaCppProvider", p)
	}
	if llama.Name() != config.ProviderLlamaCpp {
		t.Fatalf("provider name = %q", llama.Name())
	}
	if llama.modelFallback != "fallback.gguf" {
		t.Fatalf("model fallback = %q", llama.modelFallback)
	}
	if llama.client == nil || llama.client.Timeout != 2500*time.Millisecond {
		t.Fatalf("client timeout = %v", llama.client)
	}
}

func TestNewFromConfigLlamaDoesNotCallNetwork(t *testing.T) {
	calls := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Fatalf("factory must not call network, got %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	_, err := NewFromConfig(config.ProviderConfig{
		Name:      config.ProviderLlamaCpp,
		TimeoutMS: 1000,
		LlamaCpp:  config.LlamaCppConfig{URL: server.URL, Model: "fallback.gguf"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("factory made %d network calls", calls.Load())
	}
}

func TestNewFromConfigLlamaReasonRoundTrip(t *testing.T) {
	var gotPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"model":"factory-model.gguf"}`))
		case "/completion":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			gotPrompt = body["prompt"].(string)
			_, _ = w.Write([]byte(`{"content":"factory wired response","tokens_predicted":3,"tokens_evaluated":2}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	p, err := NewFromConfig(config.ProviderConfig{
		Name:      config.ProviderLlamaCpp,
		TimeoutMS: 2500,
		LlamaCpp:  config.LlamaCppConfig{URL: server.URL, Model: "fallback.gguf"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Reason(context.Background(), contracts.ReasoningRequest{
		RequestID:      "req-factory",
		Prompt:         "Explain generator overrun.",
		ActionTierHint: contracts.ActionTierC,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPrompt != "Explain generator overrun." {
		t.Fatalf("prompt = %q", gotPrompt)
	}
	if resp.RequestID != "req-factory" || resp.Text != "factory wired response" || resp.Model != "factory-model.gguf" || resp.TokensUsed != 5 || resp.ActionTier != contracts.ActionTierC {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestNewFromConfigUnknown(t *testing.T) {
	_, err := NewFromConfig(config.ProviderConfig{Name: "bogus"})
	if err == nil {
		t.Fatal("expected unknown provider error")
	}
	if !strings.Contains(err.Error(), "bogus") || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewFromConfigRejectsUndocumentedAlias(t *testing.T) {
	_, err := NewFromConfig(config.ProviderConfig{Name: "local"})
	if err == nil {
		t.Fatal("expected alias rejection")
	}
	if !strings.Contains(err.Error(), "local") || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewFromConfigUnsupportedConfiguredProvider(t *testing.T) {
	cfg := config.ProviderConfig{
		Name: config.ProviderCloudLLM,
		CloudLLM: config.CloudLLMConfig{
			Vendor:    config.CloudVendorClaude,
			APIKeyEnv: "SECRET_ENV_SHOULD_NOT_LEAK",
			Model:     "claude-sonnet-4-5",
			BaseURL:   "SECRET_BASE_URL_SHOULD_NOT_LEAK",
		},
	}
	_, err := NewFromConfig(cfg)
	if err == nil {
		t.Fatal("expected cloud_llm unsupported error")
	}
	if !strings.Contains(err.Error(), config.ProviderCloudLLM) || !strings.Contains(err.Error(), "not wired") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), cfg.CloudLLM.APIKeyEnv) || strings.Contains(err.Error(), cfg.CloudLLM.Model) || strings.Contains(err.Error(), cfg.CloudLLM.Vendor) || strings.Contains(err.Error(), cfg.CloudLLM.BaseURL) {
		t.Fatalf("factory error leaked cloud config details: %v", err)
	}
}

func TestNewFromConfigZeroTimeoutUsesDefault(t *testing.T) {
	p, err := NewFromConfig(config.ProviderConfig{
		Name:      config.ProviderLlamaCpp,
		TimeoutMS: 0,
		LlamaCpp:  config.LlamaCppConfig{URL: "http://localhost:8080", Model: "fallback.gguf"},
	})
	if err != nil {
		t.Fatal(err)
	}
	llama, ok := p.(*LlamaCppProvider)
	if !ok {
		t.Fatalf("provider type = %T", p)
	}
	if llama.client == nil || llama.client.Timeout != time.Duration(config.DefaultProviderTimeoutMS)*time.Millisecond {
		t.Fatalf("timeout = %v, want default", llama.client)
	}
}

func TestNewFromConfigNegativeTimeoutFailsClosed(t *testing.T) {
	_, err := NewFromConfig(config.ProviderConfig{Name: config.ProviderEcho, TimeoutMS: -1})
	if err == nil {
		t.Fatal("expected negative timeout to fail")
	}
	if !strings.Contains(err.Error(), "provider.timeout_ms") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewFromConfigEmptyProviderFailsClosed(t *testing.T) {
	_, err := NewFromConfig(config.ProviderConfig{})
	if err == nil {
		t.Fatal("expected empty provider name to fail")
	}
	if !strings.Contains(err.Error(), "provider name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewFromConfigLlamaValidationErrorIncludesProviderName(t *testing.T) {
	_, err := NewFromConfig(config.ProviderConfig{Name: config.ProviderLlamaCpp})
	if err == nil {
		t.Fatal("expected llama_cpp URL validation error")
	}
	if !strings.Contains(err.Error(), config.ProviderLlamaCpp) || !strings.Contains(err.Error(), "url") {
		t.Fatalf("unexpected error: %v", err)
	}
}
