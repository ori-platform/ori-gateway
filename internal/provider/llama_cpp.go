// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

const (
	defaultLlamaCppConfidence = 0.7
	maxLlamaCppResponseBytes  = 1 << 20
)

// LlamaCppProvider calls a llama.cpp server completion endpoint for LAN reasoning.
type LlamaCppProvider struct {
	completionURL string
	propsURL      string
	modelFallback string
	client        *http.Client
	now           func() time.Time
	modelMu       sync.RWMutex
	cachedModel   string
}

// LlamaCppOptions configures LlamaCppProvider.
type LlamaCppOptions struct {
	// URL may be either the server base URL or the /completion endpoint URL.
	URL string
	// ModelFallback is used when /props is unavailable or has no model metadata.
	ModelFallback string
	HTTPClient    *http.Client
	Now           func() time.Time
}

// NewLlamaCppProvider constructs a provider without calling the network.
func NewLlamaCppProvider(opts LlamaCppOptions) (*LlamaCppProvider, error) {
	completionURL, propsURL, err := normalizeLlamaCppURLs(opts.URL)
	if err != nil {
		return nil, err
	}
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &LlamaCppProvider{
		completionURL: completionURL,
		propsURL:      propsURL,
		modelFallback: strings.TrimSpace(opts.ModelFallback),
		client:        client,
		now:           now,
	}, nil
}

func (p *LlamaCppProvider) Name() string {
	return "llama_cpp"
}

func (p *LlamaCppProvider) Healthy(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, ok := p.fetchModel(ctx)
	return ok
}

func (p *LlamaCppProvider) Reason(ctx context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	if req.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}

	model := p.model(ctx)
	started := p.now()

	payload := llamaCppCompletionRequest{
		Prompt:      buildLlamaCppPrompt(req),
		Stream:      false,
		CachePrompt: true,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return contracts.ReasoningResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.completionURL, bytes.NewReader(encoded))
	if err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("llama_cpp: build completion request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("llama_cpp: completion request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		drainResponseBody(resp.Body)
		return contracts.ReasoningResponse{}, fmt.Errorf("llama_cpp: completion returned HTTP %d", resp.StatusCode)
	}

	var decoded llamaCppCompletionResponse
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxLlamaCppResponseBytes))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("llama_cpp: decode completion response: %w", err)
	}
	text := strings.TrimSpace(decoded.Content)
	if text == "" {
		return contracts.ReasoningResponse{}, fmt.Errorf("llama_cpp: completion response missing content")
	}

	return contracts.ReasoningResponse{
		RequestID:  req.RequestID,
		Text:       text,
		Model:      model,
		TokensUsed: decoded.tokensUsed(),
		LatencyMS:  p.now().Sub(started).Milliseconds(),
		Confidence: defaultLlamaCppConfidence,
		ActionTier: req.ActionTierHint,
	}, nil
}

func (p *LlamaCppProvider) model(ctx context.Context) string {
	fallback := p.modelFallback
	if fallback == "" {
		fallback = "llama.cpp"
	}

	p.modelMu.RLock()
	cached := p.cachedModel
	p.modelMu.RUnlock()
	if cached != "" {
		return cached
	}

	model, ok := p.fetchModel(ctx)
	if !ok {
		return fallback
	}
	p.modelMu.Lock()
	if p.cachedModel == "" {
		p.cachedModel = model
	}
	cached = p.cachedModel
	p.modelMu.Unlock()
	return cached
}

func (p *LlamaCppProvider) fetchModel(ctx context.Context) (string, bool) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, p.propsURL, nil)
	if err != nil {
		return "", false
	}
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		drainResponseBody(resp.Body)
		return "", false
	}
	var props llamaCppPropsResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxLlamaCppResponseBytes)).Decode(&props); err != nil {
		return "", false
	}
	if props.Model != "" {
		return props.Model, true
	}
	if props.DefaultGenerationSettings.Model != "" {
		return props.DefaultGenerationSettings.Model, true
	}
	return "", false
}

type llamaCppCompletionRequest struct {
	Prompt      string `json:"prompt"`
	Stream      bool   `json:"stream"`
	CachePrompt bool   `json:"cache_prompt"`
}

type llamaCppCompletionResponse struct {
	Content         string      `json:"content"`
	TokensPredicted int         `json:"tokens_predicted"`
	TokensEvaluated int         `json:"tokens_evaluated"`
	Timings         llamaTiming `json:"timings"`
}

type llamaTiming struct {
	PredictedN int `json:"predicted_n"`
	PromptN    int `json:"prompt_n"`
}

func (r llamaCppCompletionResponse) tokensUsed() int {
	if r.TokensPredicted > 0 || r.TokensEvaluated > 0 {
		return r.TokensPredicted + r.TokensEvaluated
	}
	return r.Timings.PredictedN + r.Timings.PromptN
}

type llamaCppPropsResponse struct {
	Model                     string `json:"model"`
	DefaultGenerationSettings struct {
		Model string `json:"model"`
	} `json:"default_generation_settings"`
}

func normalizeLlamaCppURLs(raw string) (completionURL string, propsURL string, err error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", fmt.Errorf("llama_cpp: url must not be empty")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", "", fmt.Errorf("llama_cpp: parse url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", "", fmt.Errorf("llama_cpp: url must use http or https")
	}
	if parsed.Host == "" {
		return "", "", fmt.Errorf("llama_cpp: url must include host")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""

	completion := *parsed
	base := *parsed
	if strings.HasSuffix(strings.TrimRight(completion.Path, "/"), "/completion") {
		base.Path = strings.TrimSuffix(strings.TrimRight(base.Path, "/"), "/completion")
	} else {
		completion.Path = strings.TrimRight(completion.Path, "/") + "/completion"
	}
	props := base
	props.Path = strings.TrimRight(props.Path, "/") + "/props"
	return completion.String(), props.String(), nil
}

func buildLlamaCppPrompt(req contracts.ReasoningRequest) string {
	prompt := strings.TrimSpace(req.Prompt)
	if !hasReasoningContext(req.Context) {
		return prompt
	}

	var b strings.Builder
	b.WriteString(prompt)
	b.WriteString("\n\nStructured sensor context:\n")
	b.WriteString(fmt.Sprintf("value: %g\n", req.Context.Value))
	if req.Context.Unit != "" {
		b.WriteString(fmt.Sprintf("unit: %s\n", req.Context.Unit))
	}
	if req.Context.Timestamp > 0 {
		b.WriteString(fmt.Sprintf("timestamp_ms: %d\n", req.Context.Timestamp))
	}
	if len(req.Context.History) > 0 {
		b.WriteString("history:\n")
		for _, point := range req.Context.History {
			b.WriteString(fmt.Sprintf("- value: %g, timestamp_ms: %d\n", point.Value, point.Timestamp))
		}
	}
	return strings.TrimSpace(b.String())
}

func hasReasoningContext(ctx contracts.ReasoningContext) bool {
	return ctx.Value != 0 || ctx.Unit != "" || ctx.Timestamp != 0 || len(ctx.History) > 0
}

func drainResponseBody(body io.Reader) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxLlamaCppResponseBytes))
}
