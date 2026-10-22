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
REAL_CI = Path(__file__).resolve().parent.parent / ".github/workflows/ci.yml"

GO_JOB = """
  {name}:
    runs-on: ubuntu-latest
    env:
      CGO_ENABLED: "{cgo}"
    steps:
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
      - run: {run}
"""


def workflow(*jobs: str, head: str = "") -> str:
    return "name: CI\npermissions:\n  contents: read\n" + head + "jobs:" + "".join(jobs)


def job(name: str = "test", cgo: str = "0", run: str = "go test ./...") -> str:
    return GO_JOB.format(name=name, cgo=cgo, run=run)


def race(cgo: str = "1", run: str = "go test -race ./...") -> str:
    return job("race", cgo, run)


class CheckWorkflowsTest(unittest.TestCase):
    def guard(self, text: str, name: str = "ci.yml") -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root, ".github/workflows", name)
            path.parent.mkdir(parents=True)
            path.write_text(text)
            return subprocess.run(
                [sys.executable, str(SCRIPT)], cwd=root, capture_output=True, text=True
            )

    def assertPasses(self, text: str) -> None:
        result = self.guard(text)
        self.assertEqual(result.returncode, 0, result.stderr)

    def assertRefused(self, text: str, reason: str, name: str = "ci.yml") -> None:
        result = self.guard(text, name)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(reason, result.stderr)

    def test_the_repository_ci_passes(self) -> None:
        self.assertPasses(REAL_CI.read_text())

    def test_a_test_job_with_cgo_off_and_a_race_job_with_cgo_on_pass(self) -> None:
        self.assertPasses(workflow(job(), race()))

    def test_passing_the_jobs_own_setting_through_sets_nothing(self) -> None:
        self.assertPasses(workflow(job(run='sudo env "CGO_ENABLED=$CGO_ENABLED" go test ./...')))

    def test_a_job_that_does_not_look_like_go_still_needs_the_setting(self) -> None:
        text = "\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make build\n"
        self.assertRefused(workflow(text), "job build must set CGO_ENABLED to 0")

    def test_a_job_header_with_a_trailing_comment_is_refused(self) -> None:
        text = job() + "\n  build: # ships\n    runs-on: ubuntu-latest\n    steps:\n      - run: go build ./...\n"
        self.assertRefused(workflow(text), "cannot parse job header")

    def test_a_quoted_job_header_is_refused(self) -> None:
        text = job() + '\n  "build":\n    runs-on: ubuntu-latest\n    steps:\n      - run: go build ./...\n'
        self.assertRefused(workflow(text), "cannot parse job header")

    def test_a_jobs_key_with_a_comment_is_refused(self) -> None:
        self.assertRefused(workflow(job()).replace("jobs:", "jobs: # all"), "cannot parse the jobs block")

    def test_flow_style_jobs_are_refused(self) -> None:
        text = workflow().replace("jobs:", "jobs: {build: {runs-on: ubuntu-latest}}")
        self.assertRefused(text, "cannot parse the jobs block")

    def test_a_reusable_workflow_job_is_refused(self) -> None:
        text = "\n  build:\n    uses: org/repo/.github/workflows/b.yml@" + "a" * 40 + "\n"
        self.assertRefused(workflow(text), "calls a reusable workflow")

    def test_a_commented_out_setting_does_not_count(self) -> None:
        text = "\n  build:\n    runs-on: ubuntu-latest\n    # CGO_ENABLED: \"0\"\n    steps:\n      - run: go build ./...\n"
        self.assertRefused(workflow(text), "job build must set CGO_ENABLED to 0")

    def test_a_go_job_that_enables_cgo_is_refused(self) -> None:
        self.assertRefused(workflow(job(cgo="1")), "job test must set CGO_ENABLED to 0")

    def test_a_go_job_that_sets_nothing_is_refused(self) -> None:
        text = "\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: go build ./...\n"
        self.assertRefused(workflow(text), "job build must set CGO_ENABLED to 0")

    def test_a_step_that_enables_cgo_inside_a_go_job_is_refused(self) -> None:
        self.assertRefused(
            workflow(job(run="CGO_ENABLED=1 go build ./...")), "job test must set CGO_ENABLED to 0"
        )

    def test_a_workflow_wide_setting_is_refused(self) -> None:
        head = 'env:\n  CGO_ENABLED: "1"\n'
        self.assertRefused(workflow(job(), head=head), "set CGO_ENABLED per job")

    def test_a_race_job_without_cgo_is_refused(self) -> None:
        self.assertRefused(workflow(race(cgo="0")), "job race must set CGO_ENABLED to 1")

    def test_a_race_job_without_the_race_detector_is_refused(self) -> None:
        self.assertRefused(workflow(race(run="go test ./...")), "may only run")

    def test_a_race_job_that_builds_is_refused(self) -> None:
        self.assertRefused(
            workflow(race(run="go test -race ./... && go build ./cmd/ori-gateway")),
            "may only run",
        )

    def test_a_race_job_outside_ci_yml_is_refused(self) -> None:
        self.assertRefused(workflow(race()), "job race must set CGO_ENABLED to 0", name="release.yml")

    def test_a_race_job_that_names_the_race_detector_only_in_a_comment_is_refused(self) -> None:
        text = race(run="go build ./...").replace("    steps:", "    # go test -race ./...\n    steps:")
        self.assertRefused(workflow(text), "may only run")

    def test_a_race_job_that_builds_a_test_binary_and_releases_it_is_refused(self) -> None:
        self.assertRefused(
            workflow(race(run="go test -race -c -o gw.test ./cmd/ori-gateway && gh release upload v1 gw.test")),
            "may only run",
        )

    def test_a_race_job_running_a_script_is_refused(self) -> None:
        self.assertRefused(workflow(race(run="scripts/build.sh")), "may only run")

    def test_a_race_job_with_its_own_permissions_is_refused(self) -> None:
        text = race().replace("    env:", "    permissions:\n      contents: write\n    env:")
        self.assertRefused(workflow(text), "may set only")

    def test_a_race_job_that_saves_a_cache_is_refused(self) -> None:
        text = race() + "      - uses: actions/cache/save@" + "a" * 40 + " # v4\n"
        self.assertRefused(workflow(text), "may only use")

    def test_a_race_job_whose_step_enables_cgo_again_is_refused(self) -> None:
        text = race() + '      - run: go mod verify\n        env:\n          CGO_ENABLED: "1"\n'
        self.assertRefused(workflow(text), "must set CGO_ENABLED to 1 exactly once")

    def test_a_race_job_that_uploads_is_refused(self) -> None:
        text = race() + "      - uses: actions/upload-artifact@" + "a" * 40 + " # v4\n"
        self.assertRefused(workflow(text), "may only use")


if __name__ == "__main__":
    unittest.main()
