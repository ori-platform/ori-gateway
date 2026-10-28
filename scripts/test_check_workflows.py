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
sys.dont_write_bytecode = True
sys.path.insert(0, str(SCRIPT.parent))
from check_workflows import ROOT_STEP_RUN  # noqa: E402

TEST_JOB = """
  test:
    runs-on: ubuntu-latest
    env:
      CGO_ENABLED: "0"
      GOFLAGS: "-mod=readonly"
    steps:
      - uses: actions/setup-go@{sha} # v7
      - run: go mod verify
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
    return "name: CI\npermissions:\n  contents: read\n  id-token: none\n" + head + "jobs:" + "".join(jobs)


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

    # Every workflow but a release one declares contents: read and id-token: none.

    def test_permissions_without_both_declarations_are_refused(self) -> None:
        for permissions in (
            "permissions:\n  contents: read\n",
            "permissions:\n  id-token: none\n",
            "permissions:\n  contents: write\n  id-token: none\n",
            "permissions:\n  contents: read\n  id-token: write\n",
            "permissions: read-all\n",
            "permissions: {}\n",
        ):
            with self.subTest(permissions):
                text = workflow(job()).replace("permissions:\n  contents: read\n  id-token: none\n", permissions)
                self.assertRefused(text, "permissions must declare contents: read and id-token: none", name="pr-title.yml")

    def test_another_read_scope_beside_both_declarations_passes(self) -> None:
        text = workflow(job()).replace("  id-token: none\n", "  pull-requests: read\n  id-token: none\n")
        self.assertPasses(text, name="signed-commits.yml")

    def test_a_job_whose_own_permissions_drop_a_declaration_is_refused(self) -> None:
        text = job().replace("    env:", "    permissions:\n      contents: write\n    env:", 1)
        self.assertRefused(workflow(text), "job test permissions must declare", name="pr-title.yml")
        kept = job().replace("    env:", "    permissions:\n      contents: read\n      id-token: none\n    env:", 1)
        self.assertPasses(workflow(kept), name="pr-title.yml")

    # Every job sets GOFLAGS=-mod=readonly and runs go mod verify before anything else.

    def test_a_job_without_readonly_modules_is_refused(self) -> None:
        for new in ("", '      GOFLAGS: "-mod=mod"\n', '      GOFLAGS: "-mod=readonly -toolexec=x"\n'):
            with self.subTest(new):
                text = job().replace('      GOFLAGS: "-mod=readonly"\n', new)
                self.assertRefused(workflow(text), 'job test must set GOFLAGS to "-mod=readonly"', name="pr-title.yml")

    def test_a_job_that_runs_anything_before_verifying_modules_is_refused(self) -> None:
        verify = "      - run: go mod verify\n"
        for label, text in (
            ("no verification", job().replace(verify, "")),
            ("verification after a command", job().replace(verify, "") + verify),
            ("a python command first", job().replace(verify, "      - run: python3 scripts/check.py\n" + verify)),
            ("a look-alike", job().replace(verify, "      - run: go mod verifyx\n")),
            ("a comment", job().replace(verify, "      - run: '# go mod verify'\n")),
            ("failure ignored", job().replace(verify, "      - run: go mod verify || true\n")),
            ("in the background", job().replace(verify, "      - run: go mod verify &\n")),
            ("piped", job().replace(verify, "      - run: go mod verify | cat\n")),
            ("then anything", job().replace(verify, "      - run: go mod verify; go build ./...\n")),
            ("failure masked after a list", job().replace(verify, "      - run: go mod verify && echo verified || true\n")),
            ("a list in the background", job().replace(verify, "      - run: go mod verify && echo verified &\n")),
            ("a script after it", job().replace(verify, "      - run: |\n          go mod verify && echo verified\n          go build ./...\n")),
        ):
            with self.subTest(label):
                self.assertRefused(workflow(text), "job test must first run exactly one of", name="pr-title.yml")

    def test_a_verify_step_that_can_be_skipped_or_softened_is_refused(self) -> None:
        for extra in ("if: false", "continue-on-error: true", "working-directory: /tmp", "shell: 'true {0}'", "env:\n          GOFLAGS: -mod=readonly"):
            with self.subTest(extra):
                text = job().replace("      - run: go mod verify\n", f"      - run: go mod verify\n        {extra}\n")
                self.assertRefused(workflow(text), "job test must first run exactly one of", name="pr-title.yml")
        named = job().replace("      - run: go mod verify\n", "      - name: Verify\n        run: go mod verify\n")
        self.assertPasses(workflow(named), name="pr-title.yml")

    # GOFLAGS and CGO_ENABLED are keys of a job's env and mentioned nowhere else.
    # The guard reads no shell, so every other mention is refused unread.

    def test_any_mention_of_a_setting_outside_the_job_env_is_refused(self) -> None:
        for setting, reason in (("GOFLAGS", "job test must set GOFLAGS"), ("CGO_ENABLED", "job test must set CGO_ENABLED")):
            for step in (
                f"      - run: go build ./...\n        env:\n          {setting}: x\n",
                f"      - run: go build ./...\n        env:\n          {setting.lower()}: x\n",
                f"      - run: {setting}=x go build ./...\n",
                f'      - run: sudo env "{setting}=${setting}" go test ./...\n',
                f"      - run: sudo env \"{setting}=${setting}\"' x' go test ./...\n",
                f'      - run: sudo env "{setting}=${setting.lower()}" go test ./...\n',
                f"      - run: {setting}='-mod=readonly -mod=mod' go build ./...\n",
                f"      - run: go build -ldflags=-X=main.{setting}=x ./...\n",
                f"      - run: go env {setting}\n",
                f"      - run: go build ./...\n        shell: env {setting}=x bash -e {{0}}\n",
            ):
                with self.subTest(setting=setting, step=step):
                    self.assertRefused(workflow(job() + step, race()), reason)
        self.assertRefused(workflow(job() + "      - run: CGO_ENABLED=0 go build ./...\n", race()), "mention it nowhere else")

    def test_a_setting_hidden_in_a_matrix_name_is_refused(self) -> None:
        text = job().replace(
            "    steps:",
            "    strategy:\n      matrix:\n        include:\n          - name: env GOFLAGS=-mod=mod go build ./...\n    steps:",
            1,
        ) + "      - run: ${{ matrix.name }}\n"
        self.assertRefused(workflow(text, race()), "job test must set GOFLAGS")

    def test_a_display_name_mentioning_a_setting_passes(self) -> None:
        for named in (
            job() + "      - name: Explain GOFLAGS=-mod=mod is forbidden\n        run: go test ./...\n",
            job().replace("  test:\n    runs-on", "  test:\n    name: Never CGO_ENABLED=1\n    runs-on", 1),
        ):
            with self.subTest(named):
                self.assertPasses(workflow(named, race()))

    # An env mapping has literal keys, so the guard can see what it sets.

    def test_a_computed_env_or_env_key_is_refused(self) -> None:
        for env in (
            "        env: ${{ fromJSON(vars.BUILD_ENV) }}\n",
            "        env:\n          ${{ vars.KEY }}: x\n",
            "        env:\n          ${{ insert }}: ${{ fromJSON(vars.BUILD_ENV) }}\n",
        ):
            with self.subTest(env):
                self.assertRefused(workflow(job() + "      - run: go build ./...\n" + env, race()), "env must be a mapping with literal keys")
        for key in (
            "container:\n      image: golang:1\n      env: ${{ fromJSON(vars.BUILD_ENV) }}",
            "services:\n      db:\n        image: postgres:16\n        env: ${{ fromJSON(vars.BUILD_ENV) }}",
            "env: ${{ fromJSON(vars.BUILD_ENV) }}",
        ):
            with self.subTest(key):
                text = job().replace("    env:\n      CGO_ENABLED", f"    {key}\n    env:\n      CGO_ENABLED", 1) if not key.startswith("env") else job()
                if key.startswith("env"):
                    text = job().replace('    env:\n      CGO_ENABLED: "0"\n      GOFLAGS: "-mod=readonly"\n', f"    {key}\n", 1)
                self.assertRefused(workflow(text, race()), "job test")
        self.assertRefused(workflow(job(), race(), head="env: ${{ fromJSON(vars.BUILD_ENV) }}\n"), "set env per job")
        literal = job() + "      - run: go build ./...\n        env:\n          VERSION: ${{ github.sha }}\n"
        self.assertPasses(workflow(literal, race()))

    # One exception: the root step's run text in ci.yml's test job, pinned exactly.

    def test_the_pinned_root_step_passes_only_where_it_is_designated(self) -> None:
        root = "      - name: As root\n        run: |\n" + "".join(
            f"          {line}\n" if line else "\n" for line in ROOT_STEP_RUN.rstrip("\n").split("\n")
        )
        self.assertPasses(workflow(job() + root, race()))
        self.assertRefused(workflow(job() + root, race()).replace("  test:\n", "  build:\n", 1), "job build must set")
        self.assertRefused(workflow(job() + root), "job test must set", name="pr-title.yml")
        for label, edit in (
            ("one space more", root.replace("sudo env", "sudo  env")),
            ("a trailing space", root.replace('<<<"$out"\n', '<<<"$out" \n')),
            ("a value changed", root.replace("GOFLAGS=-mod=readonly", "GOFLAGS=-mod=mod")),
        ):
            with self.subTest(label):
                self.assertRefused(workflow(job() + edit, race()), "mention it nowhere else")
        renamed = root.replace("      - name: As root\n", "      - name: As root\n        env:\n          GOFLAGS: -mod=mod\n")
        self.assertRefused(workflow(job() + renamed, race()), "job test must set GOFLAGS")

    def test_job_defaults_are_refused(self) -> None:
        text = job().replace("    steps:", "    defaults:\n      run:\n        shell: 'true {0}'\n    steps:", 1)
        self.assertRefused(workflow(text), "job test sets defaults", name="pr-title.yml")

    def test_a_release_workflow_declares_both_for_the_workflow_and_may_differ_per_job(self) -> None:
        signing = job().replace("    env:", "    permissions:\n      contents: read\n      id-token: write\n    env:", 1)
        self.assertPasses(workflow(signing), name="release.yml")
        self.assertRefused(workflow(signing), "job test permissions must declare", name="pr-title.yml")
        widened = workflow(job()).replace("  id-token: none\n", "  id-token: write\n")
        self.assertRefused(widened, "permissions must declare contents: read and id-token: none", name="release.yml")

    def test_verifying_every_module_in_one_command_passes(self) -> None:
        text = job().replace("      - run: go mod verify\n", "      - run: go mod verify && (cd tools && go mod verify)\n")
        self.assertPasses(workflow(text), name="pr-title.yml")

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
