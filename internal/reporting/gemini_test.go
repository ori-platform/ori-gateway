// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
)

func TestGeminiProviderGeneratesWeeklyReport(t *testing.T) {
	var gotPath string
	var gotAPIKey string
	var prompt string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAPIKey = r.Header.Get(geminiReportAPIKeyHeader)
		var req geminiReportRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		prompt = req.Contents[0].Parts[0].Text
		_, _ = w.Write([]byte(`{
			"candidates":[{"content":{"parts":[{"text":"Weekly report text"}]}}],
			"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5}
		}`))
	}))
	defer server.Close()

	provider, err := NewGeminiProvider(GeminiOptions{
		APIKey:     "secret-key",
		Model:      "gemini-test",
		BaseURL:    server.URL + "/gateway",
		HTTPClient: server.Client(),
		Now: func() time.Time {
			return time.UnixMilli(1_800_000_000_000)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := provider.GenerateWeeklyReport(context.Background(), WeeklyReportInput{
		CustomerName: "Customer",
		SiteName:     "Site A",
		SensorSeries: []SensorSeries{{SensorID: "current-main"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/gateway/models/gemini-test:generateContent" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotAPIKey != "secret-key" {
		t.Fatal("API key header not sent")
	}
	if !strings.Contains(prompt, "Structured weekly input JSON") || !strings.Contains(prompt, "Site A") {
		t.Fatalf("prompt missing structured input: %s", prompt)
	}
	if report.Text != "Weekly report text" || report.Provider != "gemini" || report.Model != "gemini-test" || report.Tokens != 15 {
		t.Fatalf("unexpected report: %#v", report)
	}
	if report.Metadata["prompt_version"] != "weekly-report-v1" {
		t.Fatalf("metadata missing prompt version: %#v", report.Metadata)
	}
}

func TestGeminiProviderHTTPErrorDoesNotLeakSecret(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "secret-key should not appear", http.StatusUnauthorized)
	}))
	defer server.Close()
	provider, err := NewGeminiProvider(GeminiOptions{APIKey: "secret-key", Model: "m", BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.GenerateWeeklyReport(context.Background(), WeeklyReportInput{SiteName: "Site A"})
	if err == nil {
		t.Fatal("expected HTTP error")
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("error leaked API key: %v", err)
	}
}

func TestNewProviderFromConfigGeminiUsesEnvSecret(t *testing.T) {
	t.Setenv("REPORTING_GEMINI_KEY", "secret-key")
	provider, err := NewProviderFromConfig(config.ReportingConfig{
		Provider: config.ReportingProviderGemini,
		Gemini:   config.ReportingGeminiConfig{APIKeyEnv: "REPORTING_GEMINI_KEY", Model: "gemini-test"},
	}, ProviderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := provider.(*GeminiProvider); !ok {
		t.Fatalf("unexpected provider type: %T", provider)
	}
}

func TestNewProviderFromConfigGeminiRejectsEmptyEnvSecret(t *testing.T) {
	_, err := NewProviderFromConfig(config.ReportingConfig{
		Provider: config.ReportingProviderGemini,
		Gemini:   config.ReportingGeminiConfig{APIKeyEnv: "MISSING_REPORTING_KEY", Model: "gemini-test"},
	}, ProviderOptions{})
	if err == nil || !strings.Contains(err.Error(), "MISSING_REPORTING_KEY") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGeminiProviderDefaultHTTPClientHasTimeout(t *testing.T) {
	provider, err := NewGeminiProvider(GeminiOptions{APIKey: "secret-key", Model: "gemini-test"})
	if err != nil {
		t.Fatal(err)
	}
	if provider.client == nil || provider.client.Timeout != defaultReportHTTPTimeout {
		t.Fatalf("unexpected timeout: %#v", provider.client)
	}
}

func TestGeminiProviderRejectsHTTPBaseURL(t *testing.T) {
	_, err := NewGeminiProvider(GeminiOptions{APIKey: "secret-key", Model: "gemini-test", BaseURL: "http://internal-proxy:8080"})
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("unexpected error: %v", err)
	}
}
