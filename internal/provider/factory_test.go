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

func TestNewFromConfigCloudLLMClaude(t *testing.T) {
	t.Setenv("CLAUDE_API_KEY", "secret-api-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-api-key"); got != "secret-api-key" {
			t.Fatalf("x-api-key = %q", got)
		}
		_, _ = w.Write([]byte(`{"model":"claude-sonnet-4-5","content":[{"type":"text","text":"factory cloud response"}],"usage":{"input_tokens":2,"output_tokens":3}}`))
	}))
	defer server.Close()

	p, err := NewFromConfig(config.ProviderConfig{
		Name:      config.ProviderCloudLLM,
		TimeoutMS: 2500,
		CloudLLM: config.CloudLLMConfig{
			Vendor:    config.CloudVendorClaude,
			APIKeyEnv: "CLAUDE_API_KEY",
			Model:     "claude-sonnet-4-5",
			BaseURL:   server.URL,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cloud, ok := p.(*CloudLLMProvider)
	if !ok {
		t.Fatalf("provider type = %T, want *CloudLLMProvider", p)
	}
	if cloud.client == nil || cloud.client.Timeout != 2500*time.Millisecond {
		t.Fatalf("client timeout = %v", cloud.client)
	}
	resp, err := p.Reason(context.Background(), contracts.ReasoningRequest{RequestID: "req-cloud", Prompt: "p", ActionTierHint: contracts.ActionTierC})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "factory cloud response" || resp.TokensUsed != 5 || resp.RequestID != "req-cloud" || resp.ActionTier != contracts.ActionTierC {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestNewFromConfigCloudLLMGemini(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "secret-gemini-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(geminiAPIKeyHeader); got != "secret-gemini-key" {
			t.Fatalf("%s = %q", geminiAPIKeyHeader, got)
		}
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"factory gemini response"}]}}],"usageMetadata":{"totalTokenCount":6}}`))
	}))
	defer server.Close()

	p, err := NewFromConfig(config.ProviderConfig{
		Name:      config.ProviderCloudLLM,
		TimeoutMS: 2500,
		CloudLLM: config.CloudLLMConfig{
			Vendor:    config.CloudVendorGemini,
			APIKeyEnv: "GEMINI_API_KEY",
			Model:     "gemini-2.5-flash",
			BaseURL:   server.URL,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Reason(context.Background(), contracts.ReasoningRequest{RequestID: "req-gemini", Prompt: "p", ActionTierHint: contracts.ActionTierA})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "factory gemini response" || resp.TokensUsed != 6 || resp.RequestID != "req-gemini" || resp.ActionTier != contracts.ActionTierA {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestNewFromConfigCloudLLMMissingKey(t *testing.T) {
	t.Setenv("MISSING_CLAUDE_KEY", "")
	_, err := NewFromConfig(config.ProviderConfig{
		Name: config.ProviderCloudLLM,
		CloudLLM: config.CloudLLMConfig{
			Vendor:    config.CloudVendorClaude,
			APIKeyEnv: "MISSING_CLAUDE_KEY",
			Model:     "claude-sonnet-4-5",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "MISSING_CLAUDE_KEY") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewFromConfigCloudLLMUnsupportedVendor(t *testing.T) {
	t.Setenv("OPENAI_KEY", "secret-openai-key")
	_, err := NewFromConfig(config.ProviderConfig{
		Name: config.ProviderCloudLLM,
		CloudLLM: config.CloudLLMConfig{
			Vendor:    config.CloudVendorOpenAI,
			APIKeyEnv: "OPENAI_KEY",
			Model:     "gpt-5",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), "secret-openai-key") {
		t.Fatalf("factory error leaked API key: %v", err)
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
