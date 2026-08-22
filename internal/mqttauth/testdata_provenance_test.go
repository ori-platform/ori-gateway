// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type vectorManifest struct {
	SourceRepository string            `json:"source_repository"`
	SourcePath       string            `json:"source_path"`
	SourceCommit     string            `json:"source_commit"`
	Files            map[string]string `json:"files"`
}

// TestVendoredVectorsMatchManifest keeps the vendored corpus honest. These vectors
// are normative in ori-specs; the copy here is a conformance fixture, not a source
// of truth. Editing it locally to make a test pass would silently fork the
// contract, so the digests are checked and the specs commit is recorded as the
// provenance trail.
func TestVendoredVectorsMatchManifest(t *testing.T) {
	raw, err := os.ReadFile("testdata/MANIFEST.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m vectorManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if m.SourceCommit == "" || m.SourceRepository == "" {
		t.Fatal("manifest must record the upstream repository and commit")
	}
	if len(m.Files) == 0 {
		t.Fatal("manifest lists no files; the provenance check is not testing anything")
	}
	for name, want := range m.Files {
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got := hex.EncodeToString(sha256Sum(body)); got != want {
			t.Errorf("%s has been edited locally\n got %s\nwant %s\nre-vendor from %s@%s instead",
				name, got, want, m.SourceRepository, m.SourceCommit)
		}
	}

	// Every vector file present must be listed, so a file cannot be added here
	// without a provenance entry.
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == "MANIFEST.json" {
			continue
		}
		if _, ok := m.Files[e.Name()]; !ok {
			t.Errorf("%s is not listed in the manifest and has no recorded provenance", e.Name())
		}
	}
}

func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}
