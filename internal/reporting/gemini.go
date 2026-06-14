// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
)

const (
	defaultGeminiReportEndpoint = "https://generativelanguage.googleapis.com/v1beta"
	geminiReportSuffix          = ":generateContent"
	geminiReportAPIKeyHeader    = "x-goog-api-key"
	defaultReportMaxTokens      = 1400
	maxReportResponseBytes      = 1 << 20
	defaultReportHTTPTimeout    = 30 * time.Second
)

// ProviderOptions configures connected reporting provider construction.
type ProviderOptions struct {
	HTTPClient *http.Client
	Now        func() time.Time
}

// NewProviderFromConfig constructs the selected customer-reporting provider.
func NewProviderFromConfig(cfg config.ReportingConfig, opts ProviderOptions) (Provider, error) {
	switch cfg.Provider {
	case config.ReportingProviderGemini:
		apiKeyEnv := strings.TrimSpace(cfg.Gemini.APIKeyEnv)
		apiKey := strings.TrimSpace(os.Getenv(apiKeyEnv))
		if apiKey == "" {
			return nil, fmt.Errorf("reporting.gemini.api_key_env %q is empty", apiKeyEnv)
		}
		return NewGeminiProvider(GeminiOptions{
			APIKey:     apiKey,
			Model:      cfg.Gemini.Model,
			BaseURL:    cfg.Gemini.BaseURL,
			HTTPClient: opts.HTTPClient,
			Now:        opts.Now,
		})
	case "":
		return nil, fmt.Errorf("reporting.provider must not be empty")
	default:
		return nil, fmt.Errorf("reporting.provider %q is unknown", cfg.Provider)
	}
}

// GeminiOptions configures GeminiProvider.
type GeminiOptions struct {
	APIKey     string
	Model      string
	BaseURL    string
	HTTPClient *http.Client
	Now        func() time.Time
}

// GeminiProvider generates customer-facing reports with Gemini.
type GeminiProvider struct {
	apiKey   string
	model    string
	endpoint string
	client   *http.Client
	now      func() time.Time
}

// NewGeminiProvider constructs a Gemini reporting provider without calling the network.
func NewGeminiProvider(opts GeminiOptions) (*GeminiProvider, error) {
	apiKey := strings.TrimSpace(opts.APIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("reporting gemini: API key must not be empty")
	}
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		return nil, fmt.Errorf("reporting gemini: model must not be empty")
	}
	endpoint, err := normalizeGeminiReportEndpoint(opts.BaseURL, model)
	if err != nil {
		return nil, err
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultReportHTTPTimeout}
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &GeminiProvider{apiKey: apiKey, model: model, endpoint: endpoint, client: client, now: now}, nil
}

func (p *GeminiProvider) GenerateWeeklyReport(ctx context.Context, input WeeklyReportInput) (ProviderReport, error) {
	started := p.now()
	prompt, err := buildGeminiWeeklyPrompt(input)
	if err != nil {
		return ProviderReport{}, err
	}
	payload := geminiReportRequest{
		Contents: []geminiReportContent{{
			Role:  "user",
			Parts: []geminiReportPart{{Text: prompt}},
		}},
		GenerationConfig: geminiReportGenerationConfig{MaxOutputTokens: defaultReportMaxTokens},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ProviderReport{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return ProviderReport{}, fmt.Errorf("reporting gemini: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(geminiReportAPIKeyHeader, p.apiKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return ProviderReport{}, fmt.Errorf("reporting gemini: request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		drainReportResponseBody(resp.Body)
		return ProviderReport{}, fmt.Errorf("reporting gemini: returned HTTP %d", resp.StatusCode)
	}
	var decoded geminiReportResponse
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxReportResponseBytes))
	if err := decoder.Decode(&decoded); err != nil {
		return ProviderReport{}, fmt.Errorf("reporting gemini: decode response: %w", err)
	}
	text := strings.TrimSpace(decoded.text())
	if text == "" {
		return ProviderReport{}, fmt.Errorf("reporting gemini: response missing text content")
	}
	return ProviderReport{
		Text:      text,
		Provider:  config.ReportingProviderGemini,
		Model:     p.model,
		Tokens:    decoded.UsageMetadata.tokensUsed(),
		LatencyMS: p.now().Sub(started).Milliseconds(),
		Metadata:  map[string]any{"prompt_version": "weekly-report-v1"},
	}, nil
}

func buildGeminiWeeklyPrompt(input WeeklyReportInput) (string, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("reporting gemini: encode weekly input: %w", err)
	}
	return "You are Ori, an operational intelligence assistant for Nigerian SMEs. " +
		"Write a concise weekly energy report for the business owner. Focus on cost, reliability, likely causes, and practical next actions. " +
		"If runtime posture warnings are present, explain them as operational reliability risks without exposing secrets or internal implementation details. " +
		"Do not claim that physical actions were taken unless they appear in the action log. " +
		"Use plain language and keep it under 350 words.\n\n" +
		"Structured weekly input JSON:\n" + string(encoded), nil
}

func normalizeGeminiReportEndpoint(raw string, model string) (string, error) {
	base := strings.TrimSpace(raw)
	if base == "" {
		base = defaultGeminiReportEndpoint
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("reporting gemini: invalid base_url %q", raw)
	}
	if parsed.Scheme != "https" {
		return "", fmt.Errorf("reporting gemini: base_url must use https")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/models/" + url.PathEscape(model) + geminiReportSuffix
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func drainReportResponseBody(body io.Reader) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxReportResponseBytes))
}

type geminiReportRequest struct {
	Contents         []geminiReportContent        `json:"contents"`
	GenerationConfig geminiReportGenerationConfig `json:"generationConfig"`
}

type geminiReportGenerationConfig struct {
	MaxOutputTokens int `json:"maxOutputTokens"`
}

type geminiReportContent struct {
	Role  string             `json:"role,omitempty"`
	Parts []geminiReportPart `json:"parts"`
}

type geminiReportPart struct {
	Text string `json:"text"`
}

type geminiReportResponse struct {
	Candidates    []geminiReportCandidate   `json:"candidates"`
	UsageMetadata geminiReportUsageMetadata `json:"usageMetadata"`
}

type geminiReportCandidate struct {
	Content geminiReportContent `json:"content"`
}

type geminiReportUsageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
	TotalTokenCount      int `json:"totalTokenCount"`
}

func (r geminiReportResponse) text() string {
	parts := make([]string, 0)
	for _, candidate := range r.Candidates {
		for _, part := range candidate.Content.Parts {
			if strings.TrimSpace(part.Text) != "" {
				parts = append(parts, strings.TrimSpace(part.Text))
			}
		}
	}
	return strings.Join(parts, "\n\n")
}

func (m geminiReportUsageMetadata) tokensUsed() int {
	if m.TotalTokenCount > 0 {
		return m.TotalTokenCount
	}
	return m.PromptTokenCount + m.CandidatesTokenCount
}
