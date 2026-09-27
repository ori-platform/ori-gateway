// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package specvectors

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestEveryVendoredFileMatchesTheManifest refuses a local edit, a file the
// manifest does not track, and a tracked file that is missing.
func TestEveryVendoredFileMatchesTheManifest(t *testing.T) {
	m, err := LoadManifest()
	if err != nil {
		t.Fatal(err)
	}
	if m.SourceRepository != "ori-platform/ori-specs" || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(m.SourceCommit) {
		t.Fatalf("the manifest must name ori-platform/ori-specs and a full commit, got %q@%q", m.SourceRepository, m.SourceCommit)
	}
	seen := map[string]bool{}
	err = fs.WalkDir(vendored, "vectors", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || path == "vectors/MANIFEST.json" {
			return err
		}
		name := strings.TrimPrefix(path, "vectors/")
		seen[name] = true
		want, ok := m.Files[name]
		if !ok {
			t.Errorf("%s is vendored but not tracked by the manifest", name)
			return nil
		}
		body, err := vendored.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != want {
			t.Errorf("%s has been edited locally; re-vendor it from %s@%s", name, m.SourceRepository, m.SourceCommit)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range m.Files {
		if !seen[name] {
			t.Errorf("the manifest tracks %s but it is not vendored", name)
		}
	}
}

func TestAnUntrackedPathIsNotReadable(t *testing.T) {
	if _, err := Read("evidence-transport/vectors/refusal-policy.json"); err == nil {
		t.Fatal("a path the manifest does not track must not be readable")
	}
	if _, err := Read("evidence-transport/vectors/refusal-policy-v2.json"); err != nil {
		t.Fatal(err)
	}
}

// executableSurface reports whether path is code, a script, a build file or
// CI configuration: every surface that can read a sibling checkout when it
// runs. Prose is not one; its references are swept separately.
func executableSurface(path string) bool {
	base := filepath.Base(path)
	switch {
	case strings.HasSuffix(base, ".go"), strings.HasSuffix(base, ".sh"), strings.HasSuffix(base, ".py"),
		strings.HasSuffix(base, ".mk"), strings.HasPrefix(base, "Makefile"), base == ".pre-commit-config.yaml":
		return true
	}
	slashed := filepath.ToSlash(path)
	return strings.Contains(slashed, "/.github/workflows/") &&
		(strings.HasSuffix(base, ".yml") || strings.HasSuffix(base, ".yaml"))
}

// TestNoTestReadsASiblingCheckout keeps every contract vector on the vendored,
// pinned path: code, a script, a build file or a workflow that reads an
// ori-specs checkout beside this repository passes only where one happens to
// exist, at whatever revision it holds. It matches the listed forms only: a
// path assembled some other way passes it.
func TestNoTestReadsASiblingCheckout(t *testing.T) {
	root := filepath.Join("..", "..")
	forms := []*regexp.Regexp{
		regexp.MustCompile(`"` + `ori-specs` + `"\s*[,)]`),
		regexp.MustCompile(`ORI_` + `SPECS_DIR`),
		regexp.MustCompile(`\.\./` + `ori-specs`),
	}
	self, err := filepath.Abs("specvectors_test.go")
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "vectors" {
				return filepath.SkipDir
			}
			return nil
		}
		if !executableSurface(path) {
			return nil
		}
		if abs, _ := filepath.Abs(path); abs == self {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, form := range forms {
			if form.Match(body) {
				t.Errorf("%s reads an ori-specs checkout (%s); read the vendored copy through specvectors.Read", path, form)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
