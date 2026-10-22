#!/usr/bin/env python3
# Copyright 2026 Ori Nexus Systems LTD
# SPDX-License-Identifier: Apache-2.0
"""Guard CI and hook configuration against supply-chain footguns."""

from __future__ import annotations

import re
import sys
from pathlib import Path

WORKFLOW_DIR = Path(".github/workflows")
PRE_COMMIT_CONFIG = Path(".pre-commit-config.yaml")
SHA_RE = re.compile(r"uses:\s*[^\s#]+@[0-9a-f]{40}(?:\s*#.*)?$")
MUTABLE_ACTION_RE = re.compile(r"uses:\s*[^\s#]+@(?:v\d+(?:\.\d+\.\d+)?|main|master)\b")
PRE_COMMIT_REV_RE = re.compile(r"^\s+rev:\s*([^#\s]+)")
REMOTE_EXEC_RE = re.compile(
    r"(?:curl|wget)\b[^\n]*(?:\|\s*(?:bash|sh|python\d*)|&&\s*(?:bash|sh|python\d*)\b)",
    re.IGNORECASE,
)
JOB_HEADER_RE = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$", re.MULTILINE)
CGO_SETTING_RE = re.compile(r"CGO_ENABLED\s*[:=]\s*[\"']?([^\"'\s]*)")
GO_JOB_RE = re.compile(r"setup-go|\bgo\s+(?:test|build|vet|run|install|generate)\b")
# The race detector needs cgo, so one job, which only runs tests, may enable it.
RACE_JOB = "race"
RACE_JOB_FORBIDDEN_RE = re.compile(
    r"\bgo\s+(?:build|install|run|generate)\b|actions/upload-artifact|goreleaser"
)


def _workflow_files() -> list[Path]:
    if not WORKFLOW_DIR.exists():
        return []
    return sorted(p for p in WORKFLOW_DIR.rglob("*") if p.suffix in {".yml", ".yaml"})


def _line_number(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def _check_pre_commit_config(failures: list[str]) -> None:
    if not PRE_COMMIT_CONFIG.exists():
        return
    current_repo: str | None = None
    for line_number, line in enumerate(
        PRE_COMMIT_CONFIG.read_text().splitlines(), start=1
    ):
        stripped = line.strip()
        if stripped.startswith("- repo:"):
            current_repo = stripped.removeprefix("- repo:").strip()
            continue
        if current_repo in {None, "local"}:
            continue
        match = PRE_COMMIT_REV_RE.match(line)
        if not match:
            continue
        rev = match.group(1)
        if not re.fullmatch(r"[0-9a-f]{40}", rev):
            failures.append(
                f"{PRE_COMMIT_CONFIG}:{line_number}: remote pre-commit hook "
                f"{current_repo} must pin rev to a full commit SHA"
            )


def _jobs(text: str) -> list[tuple[str, str]]:
    """Split a workflow's jobs block into (job id, job text) pairs."""
    start = re.search(r"^jobs:\s*$", text, re.MULTILINE)
    if not start:
        return []
    body = text[start.end() :]
    headers = list(JOB_HEADER_RE.finditer(body))
    return [
        (h.group(1), body[h.start() : headers[i + 1].start() if i + 1 < len(headers) else len(body)])
        for i, h in enumerate(headers)
    ]


def _check_cgo(path: Path, text: str, failures: list[str]) -> None:
    """Every Go job sets CGO_ENABLED=0, except the test-only race job, which sets 1."""
    top = re.search(r"^jobs:\s*$", text, re.MULTILINE)
    if CGO_SETTING_RE.search(text[: top.start()] if top else text):
        failures.append(f"{path}: set CGO_ENABLED per job, not for the whole workflow")
    for job, body in _jobs(text):
        # Passing the job's own setting through, CGO_ENABLED=$CGO_ENABLED, sets nothing.
        values = [
            v for v in CGO_SETTING_RE.findall(body)
            if v not in {"$CGO_ENABLED", "${CGO_ENABLED}"}
        ]
        if job == RACE_JOB:
            if "go test -race" not in body:
                failures.append(f"{path}: job {job} must run go test -race")
            if values != ["1"]:
                failures.append(f"{path}: job {job} must set CGO_ENABLED to 1 exactly once")
            if RACE_JOB_FORBIDDEN_RE.search(body):
                failures.append(f"{path}: job {job} may only run tests")
            continue
        if not GO_JOB_RE.search(body) and not values:
            continue
        if not values or any(v != "0" for v in values):
            failures.append(f"{path}: job {job} must set CGO_ENABLED to 0")


def main() -> int:
    failures: list[str] = []
    for path in _workflow_files():
        text = path.read_text()
        if "pull_request_target" in text:
            failures.append(f"{path}: contains forbidden trigger pull_request_target")
        if path.name != "release.yml" and re.search(r"\bid-token:\s*write\b", text):
            failures.append(f"{path}: id-token: write is allowed only in release.yml")
        for match in MUTABLE_ACTION_RE.finditer(text):
            line = _line_number(text, match.start())
            # A full SHA pin with a version comment is allowed; mutable refs are not.
            source_line = text.splitlines()[line - 1]
            if not SHA_RE.search(source_line):
                failures.append(
                    f"{path}:{line}: mutable GitHub Action ref: {source_line.strip()}"
                )
        for match in REMOTE_EXEC_RE.finditer(text):
            line = _line_number(text, match.start())
            failures.append(
                f"{path}:{line}: remote script download/execution is forbidden"
            )
        if "permissions:" not in text:
            failures.append(f"{path}: missing explicit workflow permissions")
        _check_cgo(path, text, failures)
    _check_pre_commit_config(failures)
    if failures:
        print("Supply-chain guard failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("Supply-chain guard: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
