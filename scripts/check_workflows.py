#!/usr/bin/env python3
# Copyright 2026 Ori Nexus Systems LTD
# SPDX-License-Identifier: Apache-2.0
"""Guard CI and hook configuration against supply-chain footguns."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

WORKFLOW_DIR = Path(".github/workflows")
PRE_COMMIT_CONFIG = Path(".pre-commit-config.yaml")
SHA_RE = re.compile(r"uses:\s*[^\s#]+@[0-9a-f]{40}(?:\s*#.*)?$")
MUTABLE_ACTION_RE = re.compile(r"uses:\s*[^\s#]+@(?:v\d+(?:\.\d+\.\d+)?|main|master)\b")
PRE_COMMIT_REV_RE = re.compile(r"^\s+rev:\s*([^#\s]+)")
REMOTE_EXEC_RE = re.compile(
    r"(?:curl|wget)\b[^\n]*(?:\|\s*(?:bash|sh|python\d*)|&&\s*(?:bash|sh|python\d*)\b)",
    re.IGNORECASE,
)
CGO_SETTING_RE = re.compile(r"CGO_ENABLED\s*=\s*[\"']?([^\"'\s;&|]*)")
# Passing the job's own setting through, CGO_ENABLED=$CGO_ENABLED, sets nothing.
CGO_PASS_THROUGH = {"$CGO_ENABLED", "${CGO_ENABLED}"}
# The race detector needs cgo, so one job in ci.yml, which only runs tests,
# may enable it. Every part of it is held to exactly what it needs.
RACE_WORKFLOW = "ci.yml"
RACE_JOB = "race"
RACE_JOB_KEYS = {"name", "runs-on", "env", "steps"}
RACE_JOB_ENV = {"CGO_ENABLED": "1", "GOFLAGS": "-mod=readonly", "GONOSUMCHECK": ""}
RACE_STEP_KEYS = {"name", "uses", "with", "run"}
RACE_JOB_RUNS = {"go mod verify", "go test -race ./..."}
RACE_JOB_RUNNER = "ubuntu-latest"
RACE_JOB_ACTIONS = {
    "step-security/harden-runner": {"egress-policy"},
    "actions/checkout": set(),
    "actions/setup-go": {"go-version-file", "cache"},
}
PINNED_ACTION_RE = re.compile(r"^([A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+)@[0-9a-f]{40}$")


class _StrictLoader(yaml.SafeLoader):
    """A SafeLoader that refuses what GitHub reads differently: duplicate keys and merge keys."""


def _construct_strict_mapping(loader: _StrictLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    seen: list[object] = []
    for key_node, _ in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            raise yaml.constructor.ConstructorError(None, None, "merge keys (<<) are not allowed", key_node.start_mark)
        key = loader.construct_object(key_node, deep=True)
        if key in seen:
            raise yaml.constructor.ConstructorError(None, None, f"duplicate key {key!r}", key_node.start_mark)
        seen.append(key)
    return loader.construct_mapping(node, deep=deep)


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_strict_mapping)


def _cgo_settings(value: object):
    """Every value CGO_ENABLED is declared with anywhere in value: as a key, or as an assignment in a string."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "CGO_ENABLED":
                yield item
            yield from _cgo_settings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _cgo_settings(item)
    elif isinstance(value, str):
        yield from CGO_SETTING_RE.findall(value)


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


def _check_cgo(path: Path, text: str, failures: list[str]) -> None:
    """Every job sets CGO_ENABLED=0, except ci.yml's test-only race job, which sets 1.

    The workflow is read as YAML, so comments, aliases and folded lines mean
    what they mean to GitHub. Anything that is not the expected shape is
    refused rather than read past.
    """
    try:
        workflow = yaml.load(text, Loader=_StrictLoader)
    except yaml.YAMLError as err:
        failures.append(f"{path}: cannot parse as YAML: {err}")
        return
    if not isinstance(workflow, dict):
        failures.append(f"{path}: cannot parse: not a mapping")
        return
    for key in ("env", "defaults"):
        if key in workflow:
            failures.append(f"{path}: set {key} per job, not for the whole workflow")
    jobs = workflow.get("jobs") or {}
    if not isinstance(jobs, dict):
        failures.append(f"{path}: cannot parse the jobs block")
        return
    for name, job in jobs.items():
        if not isinstance(job, dict):
            failures.append(f"{path}: cannot parse job {name!r}")
            continue
        if "uses" in job:
            failures.append(f"{path}: job {name} calls a reusable workflow, which this guard cannot check")
            continue
        for step in job.get("steps") or []:
            action = step.get("uses") if isinstance(step, dict) else None
            if action is not None and not (str(action).startswith("./") or PINNED_ACTION_RE.match(str(action))):
                failures.append(f"{path}: job {name} uses {action}, not pinned to a full commit SHA")
        if path.name == RACE_WORKFLOW and name == RACE_JOB:
            _check_race_job(path, name, job, failures)
        else:
            _check_cgo_off(path, name, job, failures)


def _check_cgo_off(path: Path, name: str, job: dict, failures: list[str]) -> None:
    env = job.get("env")
    settings = list(_cgo_settings(job))
    if (
        not isinstance(env, dict)
        or env.get("CGO_ENABLED") != "0"
        or any(v != "0" and v not in CGO_PASS_THROUGH for v in settings)
    ):
        failures.append(f"{path}: job {name} must set CGO_ENABLED to \"0\" in its env, and nowhere to anything else")


def _check_race_job(path: Path, name: str, job: dict, failures: list[str]) -> None:
    if extra := sorted(set(job) - RACE_JOB_KEYS):
        failures.append(f"{path}: job {name} may set only {sorted(RACE_JOB_KEYS)}, not {extra}")
    if job.get("runs-on") != RACE_JOB_RUNNER:
        failures.append(f"{path}: job {name} must run on {RACE_JOB_RUNNER}")
    if job.get("env") != RACE_JOB_ENV:
        failures.append(f"{path}: job {name} env must be exactly {RACE_JOB_ENV}")
    steps = job.get("steps")
    if not isinstance(steps, list) or not all(isinstance(step, dict) for step in steps):
        failures.append(f"{path}: job {name} has steps this guard cannot parse")
        return
    runs = []
    for step in steps:
        if extra := sorted(set(step) - RACE_STEP_KEYS):
            failures.append(f"{path}: job {name} steps may set only {sorted(RACE_STEP_KEYS)}, not {extra}")
        if "run" in step:
            runs.append(step["run"])
        if "uses" in step:
            action = str(step["uses"])
            pinned = PINNED_ACTION_RE.match(action)
            allowed = RACE_JOB_ACTIONS.get(pinned.group(1)) if pinned else None
            if allowed is None:
                failures.append(f"{path}: job {name} may only use {sorted(RACE_JOB_ACTIONS)}")
            elif not isinstance(step.get("with") or {}, dict) or set(step.get("with") or {}) - allowed:
                failures.append(f"{path}: job {name} passes {action} inputs outside {sorted(allowed)}")
        elif "with" in step:
            failures.append(f"{path}: job {name} has inputs on a step that uses no action")
    if "go test -race ./..." not in runs or any(r not in RACE_JOB_RUNS for r in runs):
        failures.append(f"{path}: job {name} may only run {sorted(RACE_JOB_RUNS)}")


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
