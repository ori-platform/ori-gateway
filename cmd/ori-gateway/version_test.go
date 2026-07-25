// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"runtime/debug"
	"strings"
	"testing"
)

func TestGatewayProvenanceFallsBackToEmbeddedVCS(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "abc123"},
			{Key: "vcs.time", Value: "2026-07-24T00:00:00Z"},
			{Key: "vcs.modified", Value: "true"},
		},
	}
	p := gatewayProvenance(info, true)
	if p.Commit != "abc123" {
		t.Fatalf("commit: got %q, want abc123", p.Commit)
	}
	if p.BuildDate != "2026-07-24T00:00:00Z" {
		t.Fatalf("build date: got %q", p.BuildDate)
	}
	if !p.Modified {
		t.Fatal("expected modified working tree to be reported")
	}
	if p.GoVersion == "" {
		t.Fatal("go version must be populated")
	}
}

func TestGatewayProvenanceFailsClosedToUnknown(t *testing.T) {
	p := gatewayProvenance(nil, false)
	if p.Commit != "unknown" || p.BuildDate != "unknown" {
		t.Fatalf("expected unknown provenance without build info, got %+v", p)
	}
}

func TestWriteVersionMarksModifiedTree(t *testing.T) {
	var out bytes.Buffer
	writeVersion(&out, buildProvenance{
		Version:   "v2.1.0",
		Commit:    "deadbeef",
		BuildDate: "2026-07-24T00:00:00Z",
		Modified:  true,
		GoVersion: "go1.24",
	})
	got := out.String()
	for _, want := range []string{"ori-gateway v2.1.0", "deadbeef (modified)", "2026-07-24T00:00:00Z", "go1.24"} {
		if !strings.Contains(got, want) {
			t.Fatalf("version output missing %q:\n%s", want, got)
		}
	}
}

func TestRunCLIVersionFlagExitsZeroWithoutStartingGateway(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"--version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code: got %d, want 0 (stderr=%q)", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "ori-gateway ") {
		t.Fatalf("unexpected version output: %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("version path must not write to stderr: %q", stderr.String())
	}
}
