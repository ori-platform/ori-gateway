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
JOB_HEADER_RE = re.compile(r"^  ([A-Za-z0-9_-]+):$")
JOB_KEY_RE = re.compile(r"^    ([A-Za-z0-9_-]+):")
CGO_SETTING_RE = re.compile(r"CGO_ENABLED\s*[:=]\s*[\"']?([^\"'\s]*)")
STEP_RUN_RE = re.compile(r"^\s+(?:-\s+)?run:\s*(.*?)\s*$")
STEP_USES_RE = re.compile(r"^\s+(?:-\s+)?uses:\s*(\S+)")
# The race detector needs cgo, so one job in ci.yml, which only runs tests,
# may enable it. It is held to exactly what it needs.
RACE_WORKFLOW = "ci.yml"
RACE_JOB = "race"
RACE_JOB_KEYS = {"name", "runs-on", "env", "steps"}
RACE_JOB_RUNS = {"go mod verify", "go test -race ./..."}
RACE_JOB_ACTIONS = (
    "step-security/harden-runner@",
    "actions/checkout@",
    "actions/setup-go@",
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


def _jobs(path: Path, text: str, failures: list[str]) -> list[tuple[str, list[str]]]:
    """Split a workflow's jobs block into (job id, job lines) pairs.

    Full-line comments are dropped first. Anything in the block that is not a
    plain job header, or a jobs key written any other way, is refused rather
    than read past, so no job escapes the checks below.
    """
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("#")]
    starts = [i for i, line in enumerate(lines) if re.match(r"^jobs\b", line)]
    if not starts:
        return []
    if len(starts) > 1 or lines[starts[0]].rstrip() != "jobs:":
        failures.append(f"{path}: cannot parse the jobs block; write it as a plain 'jobs:' line")
        return []
    jobs: list[tuple[str, list[str]]] = []
    for line in lines[starts[0] + 1 :]:
        if not line.strip():
            continue
        if not line.startswith(" "):
            break
        if not line.startswith("   "):
            header = JOB_HEADER_RE.match(line.rstrip())
            if not header:
                failures.append(f"{path}: cannot parse job header {line.strip()!r}")
                return []
            jobs.append((header.group(1), []))
            continue
        if not jobs:
            failures.append(f"{path}: cannot parse the jobs block")
            return []
        jobs[-1][1].append(line)
    return jobs


def _check_cgo(path: Path, text: str, failures: list[str]) -> None:
    """Every job sets CGO_ENABLED=0, except ci.yml's test-only race job, which sets 1."""
    top = re.search(r"^jobs\b", text, re.MULTILINE)
    if CGO_SETTING_RE.search(text[: top.start()] if top else text):
        failures.append(f"{path}: set CGO_ENABLED per job, not for the whole workflow")
    for job, lines in _jobs(path, text, failures):
        keys = [m.group(1) for m in map(JOB_KEY_RE.match, lines) if m]
        if "uses" in keys:
            failures.append(f"{path}: job {job} calls a reusable workflow, which this guard cannot check")
            continue
        # Passing the job's own setting through, CGO_ENABLED=$CGO_ENABLED, sets nothing.
        values = [
            v
            for line in lines
            for v in CGO_SETTING_RE.findall(line)
            if v not in {"$CGO_ENABLED", "${CGO_ENABLED}"}
        ]
        if path.name != RACE_WORKFLOW or job != RACE_JOB:
            if not values or any(v != "0" for v in values):
                failures.append(f"{path}: job {job} must set CGO_ENABLED to 0")
            continue
        if values != ["1"]:
            failures.append(f"{path}: job {job} must set CGO_ENABLED to 1 exactly once")
        if extra := sorted(set(keys) - RACE_JOB_KEYS):
            failures.append(f"{path}: job {job} may set only {sorted(RACE_JOB_KEYS)}, not {extra}")
        runs = [m.group(1) for m in map(STEP_RUN_RE.match, lines) if m]
        if "go test -race ./..." not in runs or any(r not in RACE_JOB_RUNS for r in runs):
            failures.append(f"{path}: job {job} may only run {sorted(RACE_JOB_RUNS)}")
        actions = [m.group(1) for m in map(STEP_USES_RE.match, lines) if m]
        if any(not a.startswith(RACE_JOB_ACTIONS) for a in actions):
            failures.append(f"{path}: job {job} may only use {list(RACE_JOB_ACTIONS)}")


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
