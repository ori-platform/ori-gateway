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
# The race detector needs cgo, so one job in ci.yml, which only runs tests,
# may enable it. Every part of it is held to exactly what it needs.
RACE_WORKFLOW = "ci.yml"
RACE_JOB = "race"
RACE_JOB_KEYS = {"name", "runs-on", "env", "steps"}
RACE_JOB_ENV = {"CGO_ENABLED": "1", "GOFLAGS": "-mod=readonly", "GONOSUMCHECK": ""}
RACE_STEP_KEYS = {"name", "uses", "with", "run"}
# Module integrity is verified before any test runs (supply-chain rule 8).
RACE_JOB_RUNS = ["go mod verify", "go test -race ./..."]
RACE_JOB_RUNNER = "ubuntu-latest"
RACE_JOB_ACTIONS = {
    "step-security/harden-runner": {"egress-policy"},
    "actions/checkout": set(),
    "actions/setup-go": {"go-version-file", "cache"},
}
# Every job keeps go.mod and go.sum as committed, and verifies the module
# cache against go.sum before it runs any other command.
GOFLAGS_SETTING = "-mod=readonly"
# The verify step runs exactly one of these commands, whose failure fails the
# job, and sets nothing that could skip or soften it. No other shell is read.
VERIFY_RUNS = {"go mod verify", "go mod verify && (cd tools && go mod verify)"}
VERIFY_STEP_KEYS = {"name", "run"}
# The guard reads no shell. GOFLAGS and CGO_ENABLED are set only as keys of a
# job's env, and any other mention of either, in any case, is refused unread,
# even a harmless one such as `go env GOFLAGS`. The one exception is the run
# text of the step below, pinned exactly: it gives the elevated process both
# settings as constants, since sudo does not pass the job env through.
ROOT_STEP_JOB = "test"
ROOT_STEP_RUN = (
    'out="$(sudo env "PATH=$PATH" CGO_ENABLED=0 GOFLAGS=-mod=readonly \\\n'
    '  "GOMODCACHE=$(go env GOMODCACHE)" "GOCACHE=$(mktemp -d)" \\\n'
    "  go test ./internal/evidence/courier -run '^TestADirectoryOwnedByAnotherUserIsRefused$' -count=1 -v)\"\n"
    'echo "$out"\n'
    "grep -q -- '--- PASS: TestADirectoryOwnedByAnotherUserIsRefused' <<<\"$out\"\n"
)
MENTION_REFUSAL = "the guard reads no shell, so any other mention is refused, even a harmless one"
# Every workflow declares contents: read and id-token: none; only a release
# workflow's jobs may declare otherwise (rule 3).
RELEASE_WORKFLOW = "release.yml"
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


def _leaves(value: object, at: tuple = ()):
    """Every mapping key and scalar in value, with the path that reaches it."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield at + (key,), key
            yield from _leaves(item, at + (key,))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _leaves(item, at + (index,))
    else:
        yield at, value


def _mentions(path: Path, job_name: str, job: dict, setting: str) -> bool:
    """Whether job mentions setting anywhere but as a key of its env.

    Only the job's and its steps' display names are skipped, and the pinned
    root step's run text in ci.yml's test job.
    """
    for at, leaf in _leaves(job):
        if not isinstance(leaf, str) or setting.lower() not in leaf.lower():
            continue
        if at == ("env", setting) and leaf == setting:
            continue
        if at == ("name",) or (len(at) == 3 and at[0] == "steps" and at[2] == "name"):
            continue
        if (
            path.name == RACE_WORKFLOW
            and job_name == ROOT_STEP_JOB
            and len(at) == 3 and at[0] == "steps" and at[2] == "run"
            and leaf == ROOT_STEP_RUN
        ):
            continue
        return True
    return False


def _check_literal_env(path: Path, job_name: str, job: dict, failures: list[str]) -> None:
    """Every env mapping in the job, and every mapping that holds one, is literal.

    No key anywhere in the job is an expression, which refuses mapping
    insertions; a container, the services, each service and each step are
    literal mappings (a container may also be a literal image name); every
    env is a mapping. So the guard sees every setting the job declares.
    """
    def computed(value: object) -> bool:
        return isinstance(value, str) and "${{" in value

    def keys(value: object):
        if isinstance(value, dict):
            for key, item in value.items():
                yield key
                yield from keys(item)
        elif isinstance(value, list):
            for item in value:
                yield from keys(item)

    refused = any(not isinstance(key, str) or computed(key) for key in keys(job))
    container = job.get("container")
    services = job.get("services")
    steps = job.get("steps") or []
    mappings = [job.get("env")]
    if container is not None and not isinstance(container, dict) and (not isinstance(container, str) or computed(container)):
        refused = True
    elif isinstance(container, dict):
        mappings.append(container.get("env"))
    if services is not None and (
        not isinstance(services, dict) or any(not isinstance(service, dict) for service in services.values())
    ):
        refused = True
    for service in services.values() if isinstance(services, dict) else ():
        if isinstance(service, dict):
            mappings.append(service.get("env"))
    if not isinstance(steps, list) or any(not isinstance(step, dict) for step in steps):
        refused = True
    for step in steps if isinstance(steps, list) else ():
        if isinstance(step, dict):
            mappings.append(step.get("env"))
    if refused or any(env is not None and not isinstance(env, dict) for env in mappings):
        failures.append(f"{path}: job {job_name} env, and every mapping that holds one, must be literal, not computed")


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

    Every job also sets GOFLAGS=-mod=readonly and runs go mod verify first, and
    every workflow but a release one grants contents: read and id-token: none.

    The workflow is read as YAML, so comments, aliases and folded lines mean
    what they mean to GitHub. Anything that is not the expected shape is
    refused rather than read past.
    """
    try:
        # An explicit tag (!!omap, !!pairs, ...) builds values this guard does
        # not walk, and GitHub may read it differently: refuse every one.
        if any(isinstance(token, yaml.TagToken) for token in yaml.scan(text)):
            failures.append(f"{path}: explicit YAML tags are not allowed")
            return
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
    _check_permissions(path, workflow.get("permissions"), jobs if path.name != RELEASE_WORKFLOW else {}, failures)
    if not isinstance(jobs, dict):
        failures.append(f"{path}: cannot parse the jobs block")
        return
    if path.name == RACE_WORKFLOW and RACE_JOB not in jobs:
        failures.append(f"{path}: must keep the {RACE_JOB} job, so CI runs the race detector")
    for name, job in jobs.items():
        if not isinstance(job, dict):
            failures.append(f"{path}: cannot parse job {name!r}")
            continue
        if "uses" in job:
            failures.append(f"{path}: job {name} calls a reusable workflow, which this guard cannot check")
            continue
        for step in job.get("steps") or []:
            action = step.get("uses") if isinstance(step, dict) else None
            # A local action's own steps are outside this guard, so none is used.
            if action is not None and not PINNED_ACTION_RE.match(str(action)):
                failures.append(f"{path}: job {name} uses {action}, not an action pinned to a full commit SHA")
        if path.name == RACE_WORKFLOW and name == RACE_JOB:
            _check_race_job(path, name, job, failures)
        else:
            _check_cgo_off(path, name, job, failures)
        _check_go_modules(path, name, job, failures)
        _check_literal_env(path, name, job, failures)


def _check_permissions(path: Path, permissions: object, jobs: object, failures: list[str]) -> None:
    def normal(value: object) -> bool:
        return isinstance(value, dict) and value.get("contents") == "read" and value.get("id-token") == "none"

    if not normal(permissions):
        failures.append(f"{path}: permissions must declare contents: read and id-token: none")
    for name, job in jobs.items() if isinstance(jobs, dict) else ():
        if isinstance(job, dict) and "permissions" in job and not normal(job["permissions"]):
            failures.append(f"{path}: job {name} permissions must declare contents: read and id-token: none")


def _check_go_modules(path: Path, name: str, job: dict, failures: list[str]) -> None:
    """GOFLAGS=-mod=readonly in the job env, and go mod verify before any other command.

    It is a fail-closed policy on how the workflow is written, not proof of
    what its commands do: an action run before the verify step, a setting
    changed at run time ($GITHUB_ENV, go env -w, a script, a command computed
    from an expression such as `run: ${{ vars.CMD }}`), a command-line -mod
    flag and the runner PATH the pinned root step carries into root are left
    to review.
    """
    env = job.get("env")
    if (
        not isinstance(env, dict)
        or env.get("GOFLAGS") != GOFLAGS_SETTING
        or _mentions(path, name, job, "GOFLAGS")
    ):
        failures.append(
            f"{path}: job {name} must set GOFLAGS to \"{GOFLAGS_SETTING}\" in its env and mention it nowhere else: {MENTION_REFUSAL}"
        )
    if "defaults" in job:
        failures.append(f"{path}: job {name} sets defaults, which reach the go mod verify step")
    steps = job.get("steps")
    runs = [step for step in steps if isinstance(step, dict) and "run" in step] if isinstance(steps, list) else []
    first = runs[0] if runs else {}
    if first.get("run") not in VERIFY_RUNS or set(first) - VERIFY_STEP_KEYS:
        failures.append(f"{path}: job {name} must first run exactly one of {sorted(VERIFY_RUNS)}, in a step with only a name")


def _check_cgo_off(path: Path, name: str, job: dict, failures: list[str]) -> None:
    env = job.get("env")
    if (
        not isinstance(env, dict)
        or env.get("CGO_ENABLED") != "0"
        or _mentions(path, name, job, "CGO_ENABLED")
    ):
        failures.append(f"{path}: job {name} must set CGO_ENABLED to \"0\" in its env and mention it nowhere else: {MENTION_REFUSAL}")


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
        if ("run" in step) == ("uses" in step):
            failures.append(f"{path}: job {name} steps must each either run a command or use an action")
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
    if runs != RACE_JOB_RUNS:
        failures.append(f"{path}: job {name} may only run, in order, {RACE_JOB_RUNS}")


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
