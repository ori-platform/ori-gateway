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
    def guard(self, text: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root, ".github/workflows/ci.yml")
            path.parent.mkdir(parents=True)
            path.write_text(text)
            return subprocess.run(
                [sys.executable, str(SCRIPT)], cwd=root, capture_output=True, text=True
            )

    def assertPasses(self, text: str) -> None:
        result = self.guard(text)
        self.assertEqual(result.returncode, 0, result.stderr)

    def assertRefused(self, text: str, reason: str) -> None:
        result = self.guard(text)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(reason, result.stderr)

    def test_the_repository_ci_passes(self) -> None:
        self.assertPasses(REAL_CI.read_text())

    def test_a_test_job_with_cgo_off_and_a_race_job_with_cgo_on_pass(self) -> None:
        self.assertPasses(workflow(job(), race()))

    def test_passing_the_jobs_own_setting_through_sets_nothing(self) -> None:
        self.assertPasses(workflow(job(run='sudo env "CGO_ENABLED=$CGO_ENABLED" go test ./...')))

    def test_a_job_without_go_needs_no_setting(self) -> None:
        self.assertPasses(workflow("\n  lint:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ok\n"))

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
        self.assertRefused(workflow(race(run="go test ./...")), "job race must run go test -race")

    def test_a_race_job_that_builds_is_refused(self) -> None:
        self.assertRefused(
            workflow(race(run="go test -race ./... && go build ./cmd/ori-gateway")),
            "job race may only run tests",
        )

    def test_a_race_job_that_uploads_is_refused(self) -> None:
        text = race() + "      - uses: actions/upload-artifact@" + "a" * 40 + " # v4\n"
        self.assertRefused(workflow(text), "job race may only run tests")


if __name__ == "__main__":
    unittest.main()
