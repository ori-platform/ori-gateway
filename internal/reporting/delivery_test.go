// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLogDelivererLogsArtifact(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	d := &LogDeliverer{Logger: logger}

	artifact := WeeklyReportArtifact{
		DeviceID:    "edge-1",
		SiteName:    "Test Site",
		Provider:    "gemini",
		Model:       "gemini-2.0-flash",
		Tokens:      1500,
		LatencyMS:   800,
		Warnings:    []string{"one warning"},
		WindowEndMS: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC).UnixMilli(),
	}
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatalf("LogDeliverer.Deliver returned error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "weekly report generated") {
		t.Errorf("expected log message, got: %s", out)
	}
	if !strings.Contains(out, "edge-1") {
		t.Errorf("expected device_id in log, got: %s", out)
	}
}

func TestLogDelivererNilLoggerFallsBack(t *testing.T) {
	d := &LogDeliverer{Logger: nil}
	artifact := WeeklyReportArtifact{DeviceID: "edge-1", SiteName: "Test"}
	// Must not panic.
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatalf("LogDeliverer with nil Logger returned error: %v", err)
	}
}

func TestNewFileDelivererRejectsRelativePath(t *testing.T) {
	_, err := NewFileDeliverer("relative/path")
	if err == nil {
		t.Fatal("expected error for relative directory path")
	}
	if !strings.Contains(err.Error(), "absolute path") {
		t.Errorf("error should mention absolute path, got: %v", err)
	}
}

func TestNewFileDelivererRejectsMissingDir(t *testing.T) {
	_, err := NewFileDeliverer("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Fatal("expected error for nonexistent directory")
	}
}

func TestNewFileDelivererAcceptsExistingDir(t *testing.T) {
	dir := t.TempDir()
	fd, err := NewFileDeliverer(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fd.Dir != dir {
		t.Errorf("Dir = %q, want %q", fd.Dir, dir)
	}
}

func TestFileDelivererWritesJSON(t *testing.T) {
	dir := t.TempDir()
	d := &FileDeliverer{Dir: dir}

	windowEnd := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	artifact := WeeklyReportArtifact{
		DeviceID:      "edge-1",
		CustomerName:  "Acme Corp",
		SiteName:      "Lagos Office",
		WindowEndMS:   windowEnd.UnixMilli(),
		GeneratedAtMS: windowEnd.UnixMilli(),
		Text:          "Weekly summary text.",
		Provider:      "gemini",
		Model:         "gemini-2.0-flash",
		Tokens:        1200,
		Warnings:      []string{},
	}
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatalf("FileDeliverer.Deliver returned error: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}

	wantName := "weekly-lagos_office-2026-06-15.json"
	if entries[0].Name() != wantName {
		t.Errorf("expected filename %q, got %q", wantName, entries[0].Name())
	}

	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}

	var out weeklyReportFilePayload
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("written file is not valid JSON: %v\ncontent: %s", err, data)
	}
	if out.SiteName != artifact.SiteName {
		t.Errorf("SiteName mismatch: got %q", out.SiteName)
	}
	if out.Text != artifact.Text {
		t.Errorf("Text mismatch: got %q", out.Text)
	}
	if out.CustomerName != artifact.CustomerName {
		t.Errorf("CustomerName mismatch: got %q", out.CustomerName)
	}
}

func TestFileDelivererExcludesDeviceID(t *testing.T) {
	dir := t.TempDir()
	d := &FileDeliverer{Dir: dir}

	artifact := WeeklyReportArtifact{
		DeviceID:    "edge-secret-001",
		SiteName:    "Test Site",
		WindowEndMS: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC).UnixMilli(),
		Text:        "All systems nominal.",
		RuntimeHealth: CustomerHealthSummary{
			Status: "healthy",
		},
		Metadata: map[string]any{"internal_key": "internal_value"},
	}
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	data, _ := os.ReadFile(filepath.Join(dir, entries[0].Name()))

	// DeviceID and Metadata must not appear in the file output.
	body := string(data)
	if strings.Contains(body, "edge-secret-001") {
		t.Errorf("file output must not contain DeviceID, got: %s", body)
	}
	if strings.Contains(body, "internal_key") || strings.Contains(body, "internal_value") {
		t.Errorf("file output must not contain Metadata, got: %s", body)
	}
}

func TestFileDelivererNoSecretsInOutput(t *testing.T) {
	dir := t.TempDir()
	d := &FileDeliverer{Dir: dir}

	artifact := WeeklyReportArtifact{
		DeviceID:    "edge-1",
		SiteName:    "Test Site",
		WindowEndMS: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC).UnixMilli(),
		Text:        "All systems nominal.",
	}
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	data, _ := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	body := strings.ToLower(string(data))

	for _, forbidden := range []string{"mqtt://", "mqtts://", "password", "secret", "hmac", "lockout"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("output contains forbidden pattern %q", forbidden)
		}
	}
}

func TestFileDelivererFailsWhenDirMissing(t *testing.T) {
	d := &FileDeliverer{Dir: "/nonexistent/path/that/does/not/exist"}
	artifact := WeeklyReportArtifact{
		SiteName:    "Test",
		WindowEndMS: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC).UnixMilli(),
	}
	err := d.Deliver(context.Background(), artifact)
	if err == nil {
		t.Fatal("expected error when directory does not exist")
	}
}

func TestFileDelivererRejectsRelativePath(t *testing.T) {
	d := &FileDeliverer{Dir: "relative/path"}
	err := d.Deliver(context.Background(), WeeklyReportArtifact{
		SiteName:    "Test",
		WindowEndMS: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC).UnixMilli(),
	})
	if err == nil {
		t.Fatal("expected error for relative directory path")
	}
	if !strings.Contains(err.Error(), "absolute path") {
		t.Errorf("error should mention absolute path, got: %v", err)
	}
}

func TestSiteSlug(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"Lagos Office", "lagos_office"},
		{"Acme Corp — Site #2", "acme_corp_site_2"},
		{"", "unknown"},
		{"   ", "unknown"},
		{"already_good", "already_good"},
		{"UPPER CASE", "upper_case"},
		{"multiple   spaces", "multiple_spaces"},
	}
	for _, tc := range cases {
		got := siteSlug(tc.in)
		if got != tc.want {
			t.Errorf("siteSlug(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNewCloudDelivererRejectsHTTPEndpoint(t *testing.T) {
	_, err := NewCloudDeliverer("http://example.com/reports", "key", CloudDelivererOptions{})
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("expected https error, got: %v", err)
	}
}

func TestNewCloudDelivererRejectsRelativeEndpoint(t *testing.T) {
	_, err := NewCloudDeliverer("/relative/path", "key", CloudDelivererOptions{})
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("expected https error, got: %v", err)
	}
}

func TestNewCloudDelivererRejectsURLWithCredentials(t *testing.T) {
	_, err := NewCloudDeliverer("https://user:pass@api.example.com/reports", "key", CloudDelivererOptions{})
	if err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("expected credentials error, got: %v", err)
	}
}

func TestNewCloudDelivererRejectsURLWithFragment(t *testing.T) {
	_, err := NewCloudDeliverer("https://api.example.com/reports#section", "key", CloudDelivererOptions{})
	if err == nil || !strings.Contains(err.Error(), "fragment") {
		t.Fatalf("expected fragment error, got: %v", err)
	}
}

func TestNewCloudDelivererRejectsEmptyAPIKey(t *testing.T) {
	_, err := NewCloudDeliverer("https://example.com/reports", "", CloudDelivererOptions{})
	if err == nil || !strings.Contains(err.Error(), "API key") {
		t.Fatalf("expected API key error, got: %v", err)
	}
}

func TestCloudDelivererPostsJSONWithBearerAuth(t *testing.T) {
	var gotMethod, gotContentType, gotAuth string
	var gotBody weeklyReportFilePayload

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("could not decode request body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	d, err := NewCloudDeliverer(server.URL, "test-api-key", CloudDelivererOptions{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}

	artifact := WeeklyReportArtifact{
		CustomerName: "Acme Corp",
		SiteName:     "Lagos Office",
		WindowEndMS:  time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC).UnixMilli(),
		Text:         "Weekly summary.",
		Provider:     "gemini",
		Warnings:     []string{},
	}
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatalf("Deliver returned error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if gotAuth != "Bearer test-api-key" {
		t.Errorf("Authorization = %q, want Bearer test-api-key", gotAuth)
	}
	if gotBody.SiteName != "Lagos Office" || gotBody.CustomerName != "Acme Corp" {
		t.Errorf("unexpected body: %+v", gotBody)
	}
}

func TestCloudDelivererNon2xxReturnsError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	d, err := NewCloudDeliverer(server.URL, "key", CloudDelivererOptions{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	err = d.Deliver(context.Background(), WeeklyReportArtifact{SiteName: "S", WindowEndMS: 1})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401 error, got: %v", err)
	}
}

func TestCloudDelivererRequestErrorDoesNotLeakAPIKey(t *testing.T) {
	// Unreachable endpoint — the HTTP client will fail, and the error must not
	// contain the API key.
	d, err := NewCloudDeliverer("https://203.0.113.0/reports", "super-secret-key", CloudDelivererOptions{
		HTTPClient: &http.Client{Timeout: 10 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = d.Deliver(context.Background(), WeeklyReportArtifact{SiteName: "S", WindowEndMS: 1})
	if err == nil {
		t.Fatal("expected error for unreachable endpoint")
	}
	if strings.Contains(err.Error(), "super-secret-key") {
		t.Fatalf("API key leaked in error: %v", err)
	}
}

func TestCloudDelivererExcludesDeviceID(t *testing.T) {
	var gotBody map[string]json.RawMessage
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d, err := NewCloudDeliverer(server.URL, "key", CloudDelivererOptions{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	artifact := WeeklyReportArtifact{
		DeviceID:    "edge-secret-001",
		SiteName:    "Test Site",
		WindowEndMS: 1,
		Metadata:    map[string]any{"internal": "value"},
	}
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}

	if _, ok := gotBody["device_id"]; ok {
		t.Error("cloud payload must not contain device_id")
	}
	if _, ok := gotBody["metadata"]; ok {
		t.Error("cloud payload must not contain metadata")
	}
	raw, _ := json.Marshal(gotBody)
	if strings.Contains(string(raw), "edge-secret-001") {
		t.Errorf("cloud payload must not contain DeviceID value: %s", raw)
	}
}

func TestCloudDelivererContextCancellationReturnsError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d, err := NewCloudDeliverer(server.URL, "key", CloudDelivererOptions{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = d.Deliver(ctx, WeeklyReportArtifact{SiteName: "S", WindowEndMS: 1})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestFileDelivererUsesWindowEndDateInFilename(t *testing.T) {
	dir := t.TempDir()
	d := &FileDeliverer{Dir: dir}

	windowEnd := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	artifact := WeeklyReportArtifact{
		SiteName:    "My Site",
		WindowEndMS: windowEnd.UnixMilli(),
	}
	if err := d.Deliver(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if !strings.Contains(entries[0].Name(), "2026-01-05") {
		t.Errorf("filename should contain window end date, got %q", entries[0].Name())
	}
}
