// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package specvectors serves the vendored ori-specs vectors to tests. Every
// file is embedded, so a clean checkout reads the same bytes in every package,
// and only files the manifest tracks can be read. Only tests import it.
package specvectors

import (
	"embed"
	"encoding/json"
	"fmt"
)

//go:embed vectors
var vendored embed.FS

// Manifest is the vendoring record: the source repository and commit, and the
// SHA-256 of each file at its path in that repository.
type Manifest struct {
	SourceRepository string            `json:"source_repository"`
	SourceCommit     string            `json:"source_commit"`
	Note             string            `json:"note"`
	Files            map[string]string `json:"files"`
}

// LoadManifest reads the vendoring record.
func LoadManifest() (Manifest, error) {
	var m Manifest
	raw, err := vendored.ReadFile("vectors/MANIFEST.json")
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(raw, &m)
	return m, err
}

// Read returns a vendored vector by its path in ori-specs, such as
// "evidence-transport/vectors/refusal-policy-v2.json". A path the manifest
// does not track is an error, so a test cannot read an unpinned file.
func Read(path string) ([]byte, error) {
	m, err := LoadManifest()
	if err != nil {
		return nil, err
	}
	if _, ok := m.Files[path]; !ok {
		return nil, fmt.Errorf("%s is not vendored; add it to the manifest from %s@%s", path, m.SourceRepository, m.SourceCommit)
	}
	return vendored.ReadFile("vectors/" + path)
}
