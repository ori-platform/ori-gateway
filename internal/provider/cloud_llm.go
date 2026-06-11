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
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

const (
	anthropicVersion         = "2023-06-01"
	defaultClaudeEndpoint    = "https://api.anthropic.com/v1/messages"
	defaultGeminiEndpoint    = "https://generativelanguage.googleapis.com/v1beta"
	defaultCloudConfidence   = 0.75
	maxCloudResponseBytes    = 1 << 20
	defaultCloudMaxTokens    = 512
	geminiGenerateSuffix     = ":generateContent"
	geminiAPIKeyHeader       = "x-goog-api-key"
	anthropicAPIKeyHeader    = "x-api-key"
	anthropicVersionHTTPName = "anthropic-version"
)

// CloudLLMProvider calls a cloud-backed Tier 3 reasoning provider through the gateway.
type CloudLLMProvider struct {
	vendor   string
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
	now      func() time.Time
}

// CloudLLMOptions configures CloudLLMProvider.
type CloudLLMOptions struct {
	Vendor     string
	APIKey     string
	Model      string
	BaseURL    string
	HTTPClient *http.Client
	Now        func() time.Time
}

// NewCloudLLMProvider constructs a provider without calling the network.
func NewCloudLLMProvider(opts CloudLLMOptions) (*CloudLLMProvider, error) {
	vendor := strings.TrimSpace(opts.Vendor)
	if vendor == "" {
		return nil, fmt.Errorf("cloud_llm: vendor must not be empty")
	}
	apiKey := strings.TrimSpace(opts.APIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("cloud_llm: API key must not be empty")
	}
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		return nil, fmt.Errorf("cloud_llm: model must not be empty")
	}
	endpoint, err := normalizeCloudEndpoint(vendor, opts.BaseURL, model)
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
	return &CloudLLMProvider{
		vendor:   vendor,
		endpoint: endpoint,
		apiKey:   apiKey,
		model:    model,
		client:   client,
		now:      now,
	}, nil
}

func (p *CloudLLMProvider) Name() string {
	return config.ProviderCloudLLM
}

// Healthy reports configuration readiness only. It deliberately does not probe
// remote cloud APIs because health checks must not create paid provider traffic.
func (p *CloudLLMProvider) Healthy(ctx context.Context) bool {
	return ctx.Err() == nil && p.apiKey != "" && p.endpoint != "" && p.model != ""
}

func (p *CloudLLMProvider) Reason(ctx context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	if req.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}
	started := p.now()
	switch p.vendor {
	case config.CloudVendorClaude:
		return p.reasonClaude(ctx, req, started)
	case config.CloudVendorGemini:
		return p.reasonGemini(ctx, req, started)
	default:
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: vendor %q is not implemented", p.vendor)
	}
}

func (p *CloudLLMProvider) reasonClaude(ctx context.Context, req contracts.ReasoningRequest, started time.Time) (contracts.ReasoningResponse, error) {
	payload := claudeMessagesRequest{
		Model:     p.model,
		MaxTokens: defaultCloudMaxTokens,
		Messages:  []claudeMessage{{Role: "user", Content: buildCloudPrompt(req)}},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return contracts.ReasoningResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: build claude request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(anthropicVersionHTTPName, anthropicVersion)
	httpReq.Header.Set(anthropicAPIKeyHeader, p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: claude request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		drainCloudResponseBody(resp.Body)
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: claude returned HTTP %d", resp.StatusCode)
	}
	var decoded claudeMessagesResponse
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxCloudResponseBytes))
	if err := decoder.Decode(&decoded); err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: decode claude response: %w", err)
	}
	text := strings.TrimSpace(decoded.text())
	if text == "" {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: claude response missing text content")
	}
	model := strings.TrimSpace(decoded.Model)
	if model == "" {
		model = p.model
	}
	return p.reasoningResponse(req, text, model, decoded.Usage.InputTokens+decoded.Usage.OutputTokens, started), nil
}

func (p *CloudLLMProvider) reasonGemini(ctx context.Context, req contracts.ReasoningRequest, started time.Time) (contracts.ReasoningResponse, error) {
	payload := geminiGenerateContentRequest{
		Contents: []geminiContent{{
			Role:  "user",
			Parts: []geminiPart{{Text: buildCloudPrompt(req)}},
		}},
		GenerationConfig: geminiGenerationConfig{MaxOutputTokens: defaultCloudMaxTokens},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return contracts.ReasoningResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: build gemini request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(geminiAPIKeyHeader, p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: gemini request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		drainCloudResponseBody(resp.Body)
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: gemini returned HTTP %d", resp.StatusCode)
	}
	var decoded geminiGenerateContentResponse
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxCloudResponseBytes))
	if err := decoder.Decode(&decoded); err != nil {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: decode gemini response: %w", err)
	}
	text := strings.TrimSpace(decoded.text())
	if text == "" {
		return contracts.ReasoningResponse{}, fmt.Errorf("cloud_llm: gemini response missing text content")
	}
	return p.reasoningResponse(req, text, p.model, decoded.UsageMetadata.tokensUsed(), started), nil
}

func (p *CloudLLMProvider) reasoningResponse(req contracts.ReasoningRequest, text string, model string, tokens int, started time.Time) contracts.ReasoningResponse {
	return contracts.ReasoningResponse{
		RequestID:  req.RequestID,
		Text:       text,
		Model:      model,
		TokensUsed: tokens,
		LatencyMS:  p.now().Sub(started).Milliseconds(),
		Confidence: defaultCloudConfidence,
		ActionTier: req.ActionTierHint,
	}
}

type claudeMessagesRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	Messages  []claudeMessage `json:"messages"`
}

type claudeMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type claudeMessagesResponse struct {
	Model   string                `json:"model"`
	Content []claudeContentBlock  `json:"content"`
	Usage   claudeUsageAccounting `json:"usage"`
}

type claudeContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type claudeUsageAccounting struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func (r claudeMessagesResponse) text() string {
	parts := make([]string, 0, len(r.Content))
	for _, block := range r.Content {
		if block.Type == "text" && strings.TrimSpace(block.Text) != "" {
			parts = append(parts, strings.TrimSpace(block.Text))
		}
	}
	return strings.Join(parts, "\n\n")
}

type geminiGenerateContentRequest struct {
	Contents         []geminiContent        `json:"contents"`
	GenerationConfig geminiGenerationConfig `json:"generationConfig"`
}

type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiGenerationConfig struct {
	MaxOutputTokens int `json:"maxOutputTokens"`
}

type geminiGenerateContentResponse struct {
	Candidates    []geminiCandidate   `json:"candidates"`
	UsageMetadata geminiUsageMetadata `json:"usageMetadata"`
}

type geminiCandidate struct {
	Content geminiContent `json:"content"`
}

type geminiUsageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
	TotalTokenCount      int `json:"totalTokenCount"`
}

func (r geminiGenerateContentResponse) text() string {
	parts := []string{}
	for _, candidate := range r.Candidates {
		for _, part := range candidate.Content.Parts {
			if strings.TrimSpace(part.Text) != "" {
				parts = append(parts, strings.TrimSpace(part.Text))
			}
		}
	}
	return strings.Join(parts, "\n\n")
}

func (m geminiUsageMetadata) tokensUsed() int {
	if m.TotalTokenCount > 0 {
		return m.TotalTokenCount
	}
	return m.PromptTokenCount + m.CandidatesTokenCount
}

func normalizeCloudEndpoint(vendor string, raw string, model string) (string, error) {
	switch vendor {
	case config.CloudVendorClaude:
		return normalizeClaudeEndpoint(raw)
	case config.CloudVendorGemini:
		return normalizeGeminiEndpoint(raw, model)
	case config.CloudVendorOpenAI, config.CloudVendorDeepSeek, config.CloudVendorOpenAICompatible:
		return "", fmt.Errorf("cloud_llm: vendor %q is not implemented", vendor)
	default:
		return "", fmt.Errorf("cloud_llm: vendor %q is unknown", vendor)
	}
}

func normalizeClaudeEndpoint(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = defaultClaudeEndpoint
	}
	parsed, err := parseCloudHTTPURL(trimmed, "base_url")
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(parsed.Path, "/v1/messages") {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/v1/messages"
	}
	return parsed.String(), nil
}

func normalizeGeminiEndpoint(raw string, model string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = defaultGeminiEndpoint
	}
	parsed, err := parseCloudHTTPURL(trimmed, "base_url")
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(parsed.Path, geminiGenerateSuffix) {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/models/" + url.PathEscape(model) + geminiGenerateSuffix
	}
	return parsed.String(), nil
}

func parseCloudHTTPURL(raw string, field string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("cloud_llm: parse %s: %w", field, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("cloud_llm: %s must use http or https", field)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("cloud_llm: %s must include host", field)
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed, nil
}

func buildCloudPrompt(req contracts.ReasoningRequest) string {
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

func drainCloudResponseBody(body io.Reader) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxCloudResponseBytes))
}
