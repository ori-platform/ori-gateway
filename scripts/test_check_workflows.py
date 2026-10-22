#!/usr/bin/env python3
# Copyright 2026 Ori Nexus Systems LTD
# SPDX-License-Identifier: Apache-2.0
"""Drive the workflow guard through its command line against workflow trees."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().with_name("check_workflows.py")
WORKFLOWS = Path(__file__).resolve().parent.parent / ".github/workflows"
SHA = "a" * 40

TEST_JOB = """
  test:
    runs-on: ubuntu-latest
    env:
      CGO_ENABLED: "0"
    steps:
      - uses: actions/setup-go@{sha} # v7
      - run: {run}
"""

RACE_JOB = """
  race:
    name: Race detector
    runs-on: ubuntu-latest
    env:
      CGO_ENABLED: "1"
      GOFLAGS: "-mod=readonly"
      GONOSUMCHECK: ""
    steps:
      - uses: step-security/harden-runner@{sha} # v2
        with:
          egress-policy: audit
      - uses: actions/checkout@{sha} # v7
      - uses: actions/setup-go@{sha} # v7
        with:
          go-version-file: go.mod
          cache: false
      - run: go mod verify
      - run: {run}
"""


def workflow(*jobs: str, head: str = "") -> str:
    return "name: CI\npermissions:\n  contents: read\n" + head + "jobs:" + "".join(jobs)


def job(run: str = "go test ./...") -> str:
    return TEST_JOB.format(sha=SHA, run=run)


def race(run: str = "go test -race ./...") -> str:
    return RACE_JOB.format(sha=SHA, run=run)


def build(lines: str) -> str:
    """A job named build: runs-on, then the given lines at job indentation."""
    return "\n  build:\n    runs-on: ubuntu-latest\n" + lines


class CheckWorkflowsTest(unittest.TestCase):
    def guard(self, text: str, name: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root, ".github/workflows", name)
            path.parent.mkdir(parents=True)
            path.write_text(text)
            return subprocess.run(
                [sys.executable, str(SCRIPT)], cwd=root, capture_output=True, text=True
            )

    def assertPasses(self, text: str, name: str = "ci.yml") -> None:
        result = self.guard(text, name)
        self.assertEqual(result.returncode, 0, result.stderr)

    def assertRefused(self, text: str, reason: str, name: str = "ci.yml") -> None:
        result = self.guard(text, name)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(reason, result.stderr)

    # The repository's own workflows, and the shapes they use.

    def test_the_repository_workflows_pass(self) -> None:
        for path in sorted(WORKFLOWS.glob("*.yml")):
            with self.subTest(path.name):
                self.assertPasses(path.read_text(), path.name)

    def test_a_test_job_with_cgo_off_and_a_race_job_with_cgo_on_pass(self) -> None:
        self.assertPasses(workflow(job(), race()))

    def test_passing_the_jobs_own_setting_through_sets_nothing(self) -> None:
        self.assertPasses(workflow(job(run='sudo env "CGO_ENABLED=$CGO_ENABLED" go test ./...'), race()))

    def test_a_quoted_or_commented_job_header_is_read_as_yaml_reads_it(self) -> None:
        for header in ('"build":', "build: # ships"):
            with self.subTest(header):
                text = job() + f"\n  {header}\n    runs-on: ubuntu-latest\n    steps:\n      - run: go build ./...\n"
                self.assertRefused(workflow(text), "job build must set CGO_ENABLED")

    # Every job sets CGO_ENABLED=0 in its env.

    def test_a_job_that_does_not_look_like_go_still_needs_the_setting(self) -> None:
        self.assertRefused(workflow(build("    steps:\n      - run: make build\n")), "job build must set CGO_ENABLED")

    def test_a_job_that_enables_cgo_is_refused(self) -> None:
        text = job().replace('CGO_ENABLED: "0"', 'CGO_ENABLED: "1"')
        self.assertRefused(workflow(text), "job test must set CGO_ENABLED")

    def test_an_expression_is_not_the_setting(self) -> None:
        text = job().replace('CGO_ENABLED: "0"', "CGO_ENABLED: ${{ matrix.cgo }}")
        self.assertRefused(workflow(text), "job test must set CGO_ENABLED")

    def test_a_comment_is_not_the_setting(self) -> None:
        for lines in (
            "    # CGO_ENABLED: \"0\"\n    steps:\n      - run: go build ./...\n",
            "    steps: # CGO_ENABLED=0\n      - run: go build ./...\n",
        ):
            with self.subTest(lines):
                self.assertRefused(workflow(build(lines)), "job build must set CGO_ENABLED")

    def test_a_setting_on_one_step_only_is_refused(self) -> None:
        lines = '    steps:\n      - run: go vet ./...\n        env:\n          CGO_ENABLED: "0"\n      - run: go build ./...\n'
        self.assertRefused(workflow(build(lines)), "job build must set CGO_ENABLED")

    def test_a_step_that_enables_cgo_is_refused(self) -> None:
        for step in (
            '      - run: go build ./...\n        env:\n          CGO_ENABLED: "1"\n',
            "      - run: CGO_ENABLED=1 go build ./...\n",
        ):
            with self.subTest(step):
                self.assertRefused(workflow(job() + step), "job test must set CGO_ENABLED")

    def test_a_step_env_aliased_from_the_race_job_is_refused(self) -> None:
        text = race().replace("    env:\n", "    env: &raceenv\n", 1) + job() + "      - run: go build ./...\n        env: *raceenv\n"
        self.assertRefused(workflow(text), "job test must set CGO_ENABLED")

    def test_workflow_wide_env_or_defaults_are_refused(self) -> None:
        # Both reach every job, the race job included, unseen by its checks.
        for head, key in (
            ('env:\n  CGO_ENABLED: "0"\n', "env"),
            ("env:\n  CC: ./scripts/anything\n", "env"),
            ("env:\n  GOTOOLCHAIN: go1.99.0\n", "env"),
            ("defaults:\n  run:\n    shell: bash -c 'go build ./...; bash {0}'\n", "defaults"),
        ):
            with self.subTest(head):
                self.assertRefused(workflow(job(), race(), head=head), f"set {key} per job")

    def test_a_shell_container_or_defaults_that_enable_cgo_are_refused(self) -> None:
        for extra in (
            "      - run: go build ./...\n        shell: env CGO_ENABLED=1 bash -e {0}\n",
        ):
            with self.subTest(extra):
                self.assertRefused(workflow(job() + extra), "job test must set CGO_ENABLED")
        for key in (
            "defaults:\n      run:\n        shell: env CGO_ENABLED=1 bash -e {0}",
            'container:\n      image: golang:1\n      env:\n        CGO_ENABLED: "1"',
        ):
            with self.subTest(key):
                text = job().replace("    steps:", f"    {key}\n    steps:", 1)
                self.assertRefused(workflow(text), "job test must set CGO_ENABLED")

    def test_yaml_github_reads_differently_is_refused(self) -> None:
        duplicate_env = job().replace('CGO_ENABLED: "0"', 'CGO_ENABLED: "1"\n      CGO_ENABLED: "0"')
        duplicate_run = race().replace("      - run: go mod verify\n", "      - run: go build ./...\n        run: go mod verify\n")
        merged = job().replace('    env:\n      CGO_ENABLED: "0"', '    env:\n      <<: {CGO_ENABLED: "0"}')
        for text, reason in ((duplicate_env, "duplicate key"), (duplicate_run, "duplicate key"), (merged, "merge keys")):
            with self.subTest(reason):
                self.assertRefused(workflow(text), reason)

    def test_an_action_not_pinned_to_a_full_sha_is_refused(self) -> None:
        for ref in ("feature-x", "v4", "a" * 39):
            with self.subTest(ref):
                text = job().replace(f"actions/setup-go@{SHA}", f"actions/setup-go@{ref}")
                self.assertRefused(workflow(text), "not an action pinned to a full commit SHA")
                race_text = race().replace(f"actions/checkout@{SHA}", f"actions/checkout@{ref}")
                self.assertRefused(workflow(race_text), "may only use")

    def test_a_local_action_is_refused(self) -> None:
        # Its own steps, and any cgo setting they declare, are outside the guard.
        self.assertRefused(workflow(job() + "      - uses: ./.github/actions/build\n"), "not an action pinned")

    def test_an_explicit_tag_is_refused(self) -> None:
        for value in ('!!omap [CGO_ENABLED: "1"]', '!!str "0"'):
            with self.subTest(value):
                text = job() + f"      - run: go build ./...\n        env: {value}\n"
                self.assertRefused(workflow(text), "explicit YAML tags")

    def test_a_setting_of_another_type_is_refused_with_its_reason(self) -> None:
        text = job() + "      - run: go build ./...\n        env:\n          CGO_ENABLED: {a: b}\n"
        self.assertRefused(workflow(text), "job test must set CGO_ENABLED")
        listed = race().replace("      - run: go mod verify\n", "      - run: go mod verify\n      - run: [go build ./...]\n")
        self.assertRefused(workflow(listed), "may only run")

    def test_a_setting_spelled_in_another_case_is_refused(self) -> None:
        for step in ('      - run: go build ./...\n        env:\n          cgo_enabled: "1"\n', "      - run: cgo_enabled=1 go build ./...\n"):
            with self.subTest(step):
                self.assertRefused(workflow(job() + step), "job test must set CGO_ENABLED")

    def test_a_race_job_on_another_runner_is_refused(self) -> None:
        self.assertRefused(workflow(race().replace("runs-on: ubuntu-latest", "runs-on: self-hosted")), "must run on ubuntu-latest")

    def test_a_reusable_workflow_job_is_refused(self) -> None:
        for indent in ("    ", "      "):
            with self.subTest(len(indent)):
                text = f"\n  build:\n{indent}uses: org/repo/.github/workflows/b.yml@{SHA}\n{indent}with: {{CGO_ENABLED: \"0\"}}\n"
                self.assertRefused(workflow(text), "calls a reusable workflow")

    def test_a_workflow_that_is_not_yaml_is_refused(self) -> None:
        self.assertRefused(workflow(job()) + "  bad: [\n", "cannot parse as YAML")

    def test_jobs_that_are_not_a_mapping_are_refused(self) -> None:
        self.assertRefused(workflow() + " [build]\n", "cannot parse the jobs block")

    # The race job is held to exactly what it needs, and only in ci.yml.

    def test_a_race_job_outside_ci_yml_is_refused(self) -> None:
        self.assertRefused(workflow(race()), "job race must set CGO_ENABLED", name="release.yml")

    def test_a_race_job_with_another_env_is_refused(self) -> None:
        for old, new in (
            ('CGO_ENABLED: "1"', 'CGO_ENABLED: "0"'),
            ('GOFLAGS: "-mod=readonly"', 'GOFLAGS: "-mod=readonly -toolexec=./scripts/anything"'),
            ('GONOSUMCHECK: ""', 'GONOSUMCHECK: ""\n      GOFLAGS_EXTRA: "1"'),
        ):
            with self.subTest(new):
                self.assertRefused(workflow(race().replace(old, new)), "env must be exactly")

    def test_a_race_job_that_runs_anything_else_is_refused(self) -> None:
        for run in (
            "go test ./...",
            "go test -race ./... && go build ./cmd/ori-gateway",
            "go test -race -c -o gw.test ./cmd/ori-gateway && gh release upload v1 gw.test",
            "scripts/build.sh",
            # A plain scalar folded onto the next line is one command to YAML.
            "go test -race ./...\n          && go build ./cmd/ori-gateway",
        ):
            with self.subTest(run):
                self.assertRefused(workflow(race(run=run)), "may only run")

    def test_ci_yml_without_the_race_job_is_refused(self) -> None:
        self.assertRefused(workflow(job()), "must keep the race job")
        self.assertPasses(workflow(job()), name="pr-title.yml")

    def test_a_race_job_must_verify_modules_before_testing(self) -> None:
        verify = "      - run: go mod verify\n"
        test = "      - run: go test -race ./...\n"
        for label, text in (
            ("verification removed", race().replace(verify, "")),
            ("verification after the tests", race().replace(verify, "") + verify),
            ("tests twice", race() + test),
            ("verification twice", race().replace(verify, verify + verify)),
        ):
            with self.subTest(label):
                self.assertRefused(workflow(text), "may only run, in order")

    def test_a_race_job_naming_the_race_detector_only_in_a_comment_is_refused(self) -> None:
        text = race(run="go build ./...").replace("    steps:", "    # go test -race ./...\n    steps:")
        self.assertRefused(workflow(text), "may only run")

    def test_a_flow_style_or_aliased_step_in_the_race_job_is_refused(self) -> None:
        flow = race() + "      - {name: ship, run: go build ./...}\n"
        self.assertRefused(workflow(flow), "may only run")
        shipping = job().replace("      - run: go test ./...", "      - &ship\n        run: go build ./...")
        self.assertRefused(workflow(shipping, race() + "      - *ship\n"), "may only run")

    def test_a_race_job_with_other_job_keys_is_refused(self) -> None:
        for key in ("permissions:\n      contents: write", "container: golang:1", "defaults:\n      run:\n        shell: sh"):
            with self.subTest(key):
                text = race().replace("    env:", f"    {key}\n    env:", 1)
                self.assertRefused(workflow(text), "may set only")

    def test_a_race_step_with_other_keys_is_refused(self) -> None:
        for extra in ("shell: sh -c 'go build ./...; sh {0}'", 'env:\n          CGO_ENABLED: "1"', "working-directory: tools"):
            with self.subTest(extra):
                text = race().replace("      - run: go mod verify\n", f"      - run: go mod verify\n        {extra}\n")
                self.assertRefused(workflow(text), "steps may set only")

    def test_a_race_step_that_neither_runs_nor_uses_or_does_both_is_refused(self) -> None:
        for step in (
            "      - {}\n",
            "      - name: nothing\n",
            f"      - uses: actions/checkout@{SHA}\n        run: go build ./...\n",
        ):
            with self.subTest(step):
                self.assertRefused(workflow(race() + step), "either run a command or use an action")

    def test_a_race_job_that_uses_another_action_is_refused(self) -> None:
        for action in ("actions/upload-artifact@", "actions/cache/save@", "./local-action", "docker://alpine"):
            with self.subTest(action):
                text = race() + f"      - uses: {action}{SHA if action.endswith('@') else ''}\n"
                self.assertRefused(workflow(text), "may only use")

    def test_a_race_job_that_passes_other_inputs_is_refused(self) -> None:
        text = race().replace("          cache: false\n", "          cache: false\n          token: x\n")
        self.assertRefused(workflow(text), "inputs outside")


if __name__ == "__main__":
    unittest.main()
