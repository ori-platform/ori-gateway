// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package agentsdoc keeps AGENTS.md, which every agent session loads in full,
// a summary of the invariants whose full text and tests live in
// docs/INVARIANTS.md.
package agentsdoc

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
)

const (
	root = "../.."
	// maxAgentsBytes caps AGENTS.md so detail moves to docs/INVARIANTS.md
	// rather than regrowing in the file every session loads.
	maxAgentsBytes = 24 * 1024
)

var (
	summaryPattern = regexp.MustCompile("(?m)^\\d+\\. `(GW-\\d+)`")
	sectionPattern = regexp.MustCompile("(?m)^## (GW-\\d+): ")
	testPattern    = regexp.MustCompile("`(Test[A-Za-z0-9_]+)`")
	funcPattern    = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)
)

func read(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func ids(pattern *regexp.Regexp, text string) []string {
	var out []string
	for _, m := range pattern.FindAllStringSubmatch(text, -1) {
		out = append(out, m[1])
	}
	return out
}

func TestAgentsStaysASummary(t *testing.T) {
	agents := read(t, "AGENTS.md")
	if len(agents) > maxAgentsBytes {
		t.Errorf("AGENTS.md is %d bytes, over %d: move detail to docs/INVARIANTS.md", len(agents), maxAgentsBytes)
	}
	if names := ids(testPattern, agents); len(names) > 0 {
		t.Errorf("AGENTS.md names tests %v: list them under the invariant in docs/INVARIANTS.md", names)
	}
}

// Every invariant summarised in AGENTS.md has its full text in
// docs/INVARIANTS.md, in the same order, and no full text lacks a summary.
func TestEveryInvariantHasItsFullText(t *testing.T) {
	summaries := ids(summaryPattern, read(t, "AGENTS.md"))
	sections := ids(sectionPattern, read(t, "docs/INVARIANTS.md"))
	if len(summaries) == 0 || !slices.Equal(summaries, sections) {
		t.Fatalf("AGENTS.md summarises %v, docs/INVARIANTS.md holds %v", summaries, sections)
	}
}

// A test named as verifying an invariant exists, so a rename or removal
// cannot leave the invariant citing evidence that is gone.
func TestEveryNamedTestExists(t *testing.T) {
	defined := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "vectors" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range funcPattern.FindAllStringSubmatch(string(body), -1) {
			defined[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	named := ids(testPattern, read(t, "docs/INVARIANTS.md"))
	if len(named) == 0 {
		t.Fatal("docs/INVARIANTS.md names no test")
	}
	for _, name := range named {
		if !defined[name] {
			t.Errorf("docs/INVARIANTS.md names %s, which no test defines", name)
		}
	}
}
