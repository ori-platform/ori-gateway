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

	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func TestCloudLLMProviderClaudeRequest(t *testing.T) {
	var gotAPIKey string
	var gotVersion string
	var gotModel string
	var gotPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.URL.Path != "/v1/messages" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		gotAPIKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		var body claudeMessagesRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		gotModel = body.Model
		if len(body.Messages) != 1 {
			t.Fatalf("messages = %d", len(body.Messages))
		}
		gotPrompt = body.Messages[0].Content
		if body.Messages[0].Role != "user" {
			t.Fatalf("role = %q", body.Messages[0].Role)
		}
		if body.MaxTokens != defaultCloudMaxTokens {
			t.Fatalf("max_tokens = %d", body.MaxTokens)
		}
		_, _ = w.Write([]byte(`{"model":"claude-sonnet-4-5","content":[{"type":"text","text":"Approve only if staff are clear."}],"usage":{"input_tokens":9,"output_tokens":6}}`))
	}))
	defer server.Close()

	p, err := NewCloudLLMProvider(CloudLLMOptions{
		Vendor:  config.CloudVendorClaude,
		APIKey:  "secret-api-key",
		Model:   "claude-sonnet-4-5",
		BaseURL: server.URL,
		Now:     testClock(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := validReasoningRequest()
	req.Context = contracts.ReasoningContext{Value: 18.4, Unit: "A", Timestamp: 1_800_000_000_000}
	resp, err := p.Reason(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if gotAPIKey != "secret-api-key" {
		t.Fatalf("x-api-key = %q", gotAPIKey)
	}
	if gotVersion != anthropicVersion {
		t.Fatalf("anthropic-version = %q", gotVersion)
	}
	if gotModel != "claude-sonnet-4-5" {
		t.Fatalf("model = %q", gotModel)
	}
	for _, want := range []string{"Explain this reading", "Structured sensor context", "value: 18.4", "unit: A"} {
		if !strings.Contains(gotPrompt, want) {
			t.Fatalf("prompt missing %q: %q", want, gotPrompt)
		}
	}
	if resp.RequestID != req.RequestID || resp.ActionTier != req.ActionTierHint {
		t.Fatalf("correlation/tier changed: %#v", resp)
	}
	if resp.Text != "Approve only if staff are clear." || resp.Model != "claude-sonnet-4-5" || resp.TokensUsed != 15 || resp.LatencyMS != 123 {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if resp.Confidence != defaultCloudConfidence {
		t.Fatalf("confidence = %f", resp.Confidence)
	}
}

func TestCloudLLMProviderGeminiRequest(t *testing.T) {
	var gotAPIKey string
	var gotModelPath string
	var gotPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		gotModelPath = r.URL.Path
		gotAPIKey = r.Header.Get(geminiAPIKeyHeader)
		var body geminiGenerateContentRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Contents) != 1 || len(body.Contents[0].Parts) != 1 {
			t.Fatalf("unexpected contents: %#v", body.Contents)
		}
		if body.Contents[0].Role != "user" {
			t.Fatalf("role = %q", body.Contents[0].Role)
		}
		if body.GenerationConfig.MaxOutputTokens != defaultCloudMaxTokens {
			t.Fatalf("maxOutputTokens = %d", body.GenerationConfig.MaxOutputTokens)
		}
		gotPrompt = body.Contents[0].Parts[0].Text
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"Use grid charging tonight before the rain."}]}}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":8,"totalTokenCount":15}}`))
	}))
	defer server.Close()

	p, err := NewCloudLLMProvider(CloudLLMOptions{
		Vendor:  config.CloudVendorGemini,
		APIKey:  "gemini-api-key",
		Model:   "gemini-2.5-flash",
		BaseURL: server.URL,
		Now:     testClock(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := validReasoningRequest()
	req.Prompt = "Explain tomorrow rain risk."
	req.Context = contracts.ReasoningContext{Value: 42, Unit: "%", Timestamp: 1_800_000_000_000}
	resp, err := p.Reason(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if gotAPIKey != "gemini-api-key" {
		t.Fatalf("%s = %q", geminiAPIKeyHeader, gotAPIKey)
	}
	if gotModelPath != "/models/gemini-2.5-flash:generateContent" {
		t.Fatalf("path = %q", gotModelPath)
	}
	for _, want := range []string{"Explain tomorrow rain risk", "Structured sensor context", "value: 42", "unit: %"} {
		if !strings.Contains(gotPrompt, want) {
			t.Fatalf("prompt missing %q: %q", want, gotPrompt)
		}
	}
	if resp.RequestID != req.RequestID || resp.ActionTier != req.ActionTierHint {
		t.Fatalf("correlation/tier changed: %#v", resp)
	}
	if resp.Text != "Use grid charging tonight before the rain." || resp.Model != "gemini-2.5-flash" || resp.TokensUsed != 15 || resp.LatencyMS != 123 {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestCloudLLMProviderGeminiResponseUsesTokenFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"First."},{"text":"Second."}]}}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3}}`))
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorGemini, APIKey: "key", Model: "gemini-model", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Reason(context.Background(), validReasoningRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "First.\n\nSecond." {
		t.Fatalf("text = %q", resp.Text)
	}
	if resp.TokensUsed != 5 {
		t.Fatalf("tokens = %d", resp.TokensUsed)
	}
}

func TestCloudLLMProviderGeminiHTTPErrorDoesNotLeakSecret(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad gemini-api-key", http.StatusForbidden)
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorGemini, APIKey: "gemini-api-key", Model: "m", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Reason(context.Background(), validReasoningRequest())
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), "gemini-api-key") {
		t.Fatalf("error leaked API key: %v", err)
	}
}

func TestCloudLLMProviderGeminiMalformedAndMissingContent(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "malformed", body: `not-json`, want: "decode gemini response"},
		{name: "missing content", body: `{"candidates":[{"content":{"parts":[{"text":"   "}]}}]}`, want: "missing text"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorGemini, APIKey: "key", Model: "m", BaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.Reason(context.Background(), validReasoningRequest())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCloudLLMProviderClaudeAppendsMessagesSuffixToProxyPath(t *testing.T) {
	p, err := NewCloudLLMProvider(CloudLLMOptions{
		Vendor:  config.CloudVendorClaude,
		APIKey:  "key",
		Model:   "claude-sonnet-4-5",
		BaseURL: "https://proxy.example/claude",
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.endpoint != "https://proxy.example/claude/v1/messages" {
		t.Fatalf("endpoint = %q", p.endpoint)
	}
}

func TestCloudLLMProviderHealthyIsConfigurationOnly(t *testing.T) {
	p, err := NewCloudLLMProvider(CloudLLMOptions{
		Vendor:  config.CloudVendorGemini,
		APIKey:  "key",
		Model:   "gemini-2.5-flash",
		BaseURL: "https://unreachable.invalid",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !p.Healthy(context.Background()) {
		t.Fatal("configured cloud provider should report configuration-ready")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.Healthy(ctx) {
		t.Fatal("canceled context should report unhealthy")
	}
}

func TestCloudLLMProviderClaudeResponseConcatenatesTextBlocks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"First."},{"type":"tool_use","text":"ignore"},{"type":"text","text":"Second."}],"usage":{"input_tokens":1,"output_tokens":2}}`))
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorClaude, APIKey: "key", Model: "configured-model", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Reason(context.Background(), validReasoningRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "First.\n\nSecond." {
		t.Fatalf("text = %q", resp.Text)
	}
	if resp.Model != "configured-model" {
		t.Fatalf("model fallback = %q", resp.Model)
	}
}

func TestCloudLLMProviderClaudeHTTPErrorDoesNotLeakSecret(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad secret-api-key", http.StatusUnauthorized)
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorClaude, APIKey: "secret-api-key", Model: "m", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Reason(context.Background(), validReasoningRequest())
	if err == nil || !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), "secret-api-key") {
		t.Fatalf("error leaked API key: %v", err)
	}
}

func TestCloudLLMProviderMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorClaude, APIKey: "key", Model: "m", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Reason(context.Background(), validReasoningRequest())
	if err == nil || !strings.Contains(err.Error(), "decode claude response") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloudLLMProviderMissingContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"   "}]}`))
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorClaude, APIKey: "key", Model: "m", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Reason(context.Background(), validReasoningRequest())
	if err == nil || !strings.Contains(err.Error(), "missing text") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloudLLMProviderTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"late"}]}`))
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorClaude, APIKey: "key", Model: "m", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = p.Reason(ctx, validReasoningRequest())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline", err)
	}
}

func TestCloudLLMProviderRequestTimeoutMSAppliesDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"late"}]}`))
	}))
	defer server.Close()
	p, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorClaude, APIKey: "key", Model: "m", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	req := validReasoningRequest()
	req.TimeoutMS = 1
	_, err = p.Reason(context.Background(), req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline", err)
	}
}

func TestCloudLLMProviderUnsupportedVendor(t *testing.T) {
	for _, vendor := range []string{config.CloudVendorOpenAI, config.CloudVendorDeepSeek, config.CloudVendorOpenAICompatible} {
		t.Run(vendor, func(t *testing.T) {
			_, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: vendor, APIKey: "key", Model: "model"})
			if err == nil || !strings.Contains(err.Error(), "not implemented") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCloudLLMProviderRequiresConfig(t *testing.T) {
	cases := []CloudLLMOptions{
		{Vendor: "", APIKey: "key", Model: "m"},
		{Vendor: config.CloudVendorClaude, APIKey: "", Model: "m"},
		{Vendor: config.CloudVendorClaude, APIKey: "key", Model: ""},
		{Vendor: config.CloudVendorClaude, APIKey: "key", Model: "m", BaseURL: "localhost:8080"},
	}
	for _, tc := range cases {
		if _, err := NewCloudLLMProvider(tc); err == nil {
			t.Fatalf("expected validation error for %#v", tc)
		}
	}
}

func TestCloudLLMProviderDoesNotCallNetworkOnConstruction(t *testing.T) {
	called := atomic.Bool{}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		called.Store(true)
		return nil, errors.New("unexpected network call")
	})}
	if _, err := NewCloudLLMProvider(CloudLLMOptions{Vendor: config.CloudVendorClaude, APIKey: "key", Model: "model", HTTPClient: client}); err != nil {
		t.Fatal(err)
	}
	if called.Load() {
		t.Fatal("constructor must not call network")
	}
}
