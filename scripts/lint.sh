#!/usr/bin/env bash
# Copyright 2026 Ori Nexus Systems LTD
# SPDX-License-Identifier: Apache-2.0
#
# Static analysis beyond go vet: staticcheck, the modernize analyzers, and
# gopls's own checks (the diagnostics an editor shows, fmtappendf among them).
# The tools are pinned in tools/go.mod, a module of their own so that no linter
# moves a dependency of the gateway; go.sum verifies them. Every Go file is
# checked, including test files and files behind the evidence_integration tag.

set -euo pipefail

cd "$(dirname "$0")/.."

TAGS="evidence_integration"
TOOL=(go tool -modfile=tools/go.mod)
status=0

echo "staticcheck"
"${TOOL[@]}" staticcheck -tags "$TAGS" ./... || status=1

echo "modernize"
"${TOOL[@]}" modernize -tags "$TAGS" -test ./... || status=1

echo "gopls check"
# Build gopls first: on a cold module cache the go command reports its
# downloads on stderr, which the capture below would read as findings.
"${TOOL[@]}" gopls version >/dev/null || status=1
files=()
while IFS= read -r file; do
  # The index can still name a file a pending rename or removal took away.
  [ -f "$file" ] && files+=("$file")
done < <(git ls-files --cached --others --exclude-standard -- '*.go' ':!tools/**')
findings="$(GOFLAGS="-tags=$TAGS" "${TOOL[@]}" gopls check "${files[@]}" 2>&1)" || status=1
if [ -n "$findings" ]; then
  printf '%s\n' "$findings"
  status=1
fi

exit "$status"
