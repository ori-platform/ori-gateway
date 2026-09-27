// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package specvectors

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// requirementIssues is the owned disposition of every missing requirement a
// pin may name: exactly one issue for each, and an issue serves only the
// requirements listed against it here.
var requirementIssues = []struct{ Requirement, Issue string }{
	{Requirement: "missing_requirement:retention-verification", Issue: "https://github.com/ori-platform/ori-gateway/issues/102"},
	{Requirement: "missing_requirement:refusal-return-set", Issue: "https://github.com/ori-platform/ori-gateway/issues/102"},
	{Requirement: "missing_requirement:retention-capacity", Issue: "https://github.com/ori-platform/ori-gateway/issues/102"},
	{Requirement: "missing_requirement:incidents", Issue: "https://github.com/ori-platform/ori-gateway/issues/103"},
	{Requirement: "missing_requirement:last-error-vocabulary", Issue: "https://github.com/ori-platform/ori-gateway/issues/103"},
	{Requirement: "missing_requirement:routing-anchor-epoch", Issue: "https://github.com/ori-platform/ori-gateway/issues/104"},
	{Requirement: "missing_requirement:disposition-carriage", Issue: "https://github.com/ori-platform/ori-gateway/issues/105"},
	{Requirement: "missing_requirement:stopped-custody-bounds", Issue: "https://github.com/ori-platform/ori-gateway/issues/105"},
	{Requirement: "missing_requirement:handoff-ordering", Issue: "https://github.com/ori-platform/ori-gateway/issues/106"},
}

var (
	requirementForm = regexp.MustCompile(`^missing_requirement:[a-z0-9]+(-[a-z0-9]+)*$`)
	issueForm       = regexp.MustCompile(`^https://github\.com/ori-platform/ori-gateway/issues/[1-9][0-9]*$`)
	// issueReference matches the forms an issue or pull request reference
	// takes in this repository. It matches the listed forms only.
	issueReference = regexp.MustCompile(`github\.com/ori-platform/[A-Za-z0-9_.-]+/(issues|pull)/[0-9]+|ori-[a-z]+#[0-9]+`)
)

// pinLiteral is a composite literal carrying Requirement and Issue keys: a
// pin, or a registry entry.
type pinLiteral struct {
	file               string
	requirement, issue string
}

// TestPinsLinkEachRequirementToOneIssue holds the one exception to the rule
// that no issue reference appears in this repository: the structured Issue
// field of an expected-failure pin in a test file. Offline, it requires every
// pin's requirement to be a stable missing_requirement:<id> whose registry
// entry names the pin's issue, the registry to name one issue per requirement,
// every registry entry to be in use, and every issue URL anywhere in the
// repository to be the value of such a field. It matches the listed reference
// forms only.
func TestPinsLinkEachRequirementToOneIssue(t *testing.T) {
	root := filepath.Join("..", "..")
	registry := map[string]string{}
	for _, entry := range requirementIssues {
		if _, dup := registry[entry.Requirement]; dup {
			t.Errorf("the registry names %s twice", entry.Requirement)
		}
		if !requirementForm.MatchString(entry.Requirement) || !issueForm.MatchString(entry.Issue) {
			t.Errorf("registry entry %s -> %s is not a requirement ID and a full issue URL", entry.Requirement, entry.Issue)
		}
		registry[entry.Requirement] = entry.Issue
	}

	used := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch filepath.ToSlash(path) {
			case filepath.ToSlash(filepath.Join(root, ".git")),
				filepath.ToSlash(filepath.Join(root, "tools")),
				filepath.ToSlash(filepath.Join(root, "internal", "specvectors", "vectors")):
				return filepath.SkipDir
			}
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		refs := issueReference.FindAllIndex(body, -1)
		allowed := [][2]int{}
		if strings.HasSuffix(path, "_test.go") {
			pins, spans := parsePins(t, path, body)
			allowed = spans
			for _, pin := range pins {
				if !requirementForm.MatchString(pin.requirement) || !issueForm.MatchString(pin.issue) {
					t.Errorf("%s: pin %s -> %s is not a requirement ID and a full issue URL", path, pin.requirement, pin.issue)
					continue
				}
				want, ok := registry[pin.requirement]
				if !ok {
					t.Errorf("%s: pin names %s, which the registry does not own", path, pin.requirement)
					continue
				}
				if pin.issue != want {
					t.Errorf("%s: pin names %s under %s; its issue is %s", path, pin.requirement, pin.issue, want)
				}
				if !strings.HasSuffix(path, "pins_test.go") || !strings.Contains(path, "specvectors") {
					used[pin.requirement] = true
				}
			}
		}
		for _, ref := range refs {
			inside := false
			for _, span := range allowed {
				if ref[0] >= span[0] && ref[1] <= span[1] {
					inside = true
					break
				}
			}
			if !inside {
				t.Errorf("%s: an issue reference outside a pin's Issue field: %s (this guard recognises full GitHub issue and pull request URLs and ori-<repo>#N; a bare #N it cannot tell from other text, and review must catch it)", path, body[ref[0]:ref[1]])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for requirement := range registry {
		if !used[requirement] {
			t.Errorf("no pin names %s; remove it from the registry", requirement)
		}
	}
}

// parsePins returns the Requirement/Issue composite literals in a Go file and
// the byte spans of their Issue values, the only place an issue URL may sit.
func parsePins(t *testing.T, path string, body []byte) ([]pinLiteral, [][2]int) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, body, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var pins []pinLiteral
	var spans [][2]int
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		var requirement, issue *ast.BasicLit
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			value, isString := kv.Value.(*ast.BasicLit)
			if !ok || !isString || value.Kind != token.STRING {
				continue
			}
			switch key.Name {
			case "Requirement":
				requirement = value
			case "Issue":
				issue = value
			}
		}
		if requirement == nil || issue == nil {
			return true
		}
		req, err1 := strconv.Unquote(requirement.Value)
		iss, err2 := strconv.Unquote(issue.Value)
		if err1 != nil || err2 != nil {
			t.Errorf("%s: a pin's Requirement and Issue are plain string literals", path)
			return true
		}
		pins = append(pins, pinLiteral{file: path, requirement: req, issue: iss})
		spans = append(spans, [2]int{fset.Position(issue.Pos()).Offset, fset.Position(issue.End()).Offset})
		return true
	})
	return pins, spans
}
