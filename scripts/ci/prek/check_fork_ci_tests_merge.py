#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# /// script
# requires-python = ">=3.11,<3.12"
# dependencies = [
#   "pyyaml>=6.0.4",
#   "rich>=13.6.0",
# ]
# ///
"""
Check that every job of a fork CI run tests the pushed commit merged with the base branch.

A fork run checks out the pushed commit; the ``merge-with-base`` action then replaces it with its
merge into the base branch, which is what a pull_request run of project CI tests (AIP-120). The
check covers ``ci-amd.yml``, ``ci-arm.yml`` and every workflow they call, directly or not:

- every call of a reusable workflow passes ``merge-with``, and every reusable workflow takes it;
- in every job that checks out the pushed commit into the workspace root, the merge step follows
  the last such checkout, with an ``if:`` that runs it only when ``merge-with`` is set.

A checkout of another ref (a constraints branch, the gate configuration) or into a sub-directory is
not the pull request's sources and is not counted.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console

console = Console(color_system="standard", width=200)

WORKFLOWS_DIR = Path(__file__).resolve().parents[3] / ".github" / "workflows"
TOP_WORKFLOWS = ("ci-amd.yml", "ci-arm.yml")
MERGE_ACTION = "./.github/actions/merge-with-base"
# The same action, read from a sparse checkout of the workflow's own version where a job checks out
# another ref into the workspace, so that no code of that ref runs as an action.
WORKFLOW_VERSION_MERGE_ACTION = "./workflow-version/.github/actions/merge-with-base"
REUSABLE_WORKFLOW = re.compile(r"^\./\.github/workflows/([\w.-]+\.yml)$")
# Refs a checkout of the pushed commit is written with.
PUSHED_COMMIT_REFS = frozenset({"", "${{ github.sha }}", "${{ inputs.checkout-ref }}"})
# Jobs that read the pushed commit itself rather than test it.
EXEMPT_JOBS = {
    **{(top, "gate"): "reads the gate configuration" for top in TOP_WORKFLOWS},
    **{
        (top, "build-info"): "selects tests from the pushed commit's changes and resolves the merge base"
        for top in TOP_WORKFLOWS
    },
    **{(top, "notify-slack"): "only reports on the run" for top in TOP_WORKFLOWS},
    **{(top, "summarize-warnings"): "only reports on the run" for top in TOP_WORKFLOWS},
}


def load_workflow(name: str, workflows_dir: Path) -> dict[str, Any]:
    return yaml.safe_load((workflows_dir / name).read_text())


def find_called_workflows(workflow: dict[str, Any]) -> set[str]:
    called = set()
    for job in (workflow.get("jobs") or {}).values():
        match = REUSABLE_WORKFLOW.match(str(job.get("uses", "")))
        if match:
            called.add(match.group(1))
    return called


def find_fork_run_workflows(workflows_dir: Path) -> dict[str, dict[str, Any]]:
    """The workflows a fork run executes: the top ones and every workflow they call."""
    found: dict[str, dict[str, Any]] = {}
    pending = list(TOP_WORKFLOWS)
    while pending:
        name = pending.pop()
        if name in found:
            continue
        found[name] = load_workflow(name, workflows_dir)
        pending.extend(find_called_workflows(found[name]))
    return found


def is_pushed_commit_checkout(step: dict[str, Any]) -> bool:
    if not str(step.get("uses", "")).startswith("actions/checkout@"):
        return False
    options = step.get("with") or {}
    return options.get("path", ".") in (".", "") and str(options.get("ref", "")) in PUSHED_COMMIT_REFS


def check_job(workflow_name: str, job_id: str, job: dict[str, Any]) -> list[str]:
    if REUSABLE_WORKFLOW.match(str(job.get("uses", ""))):
        if "merge-with" not in (job.get("with") or {}):
            return [f"job [yellow]{job_id}[/] calls {job['uses']} without passing `merge-with`"]
        return []
    if (workflow_name, job_id) in EXEMPT_JOBS:
        return []
    steps = job.get("steps") or []
    checkouts = [index for index, step in enumerate(steps) if is_pushed_commit_checkout(step)]
    if not checkouts:
        return []
    merges = [
        index
        for index, step in enumerate(steps)
        if step.get("uses") in (MERGE_ACTION, WORKFLOW_VERSION_MERGE_ACTION)
    ]
    if not any(index > checkouts[-1] for index in merges):
        return [
            f"job [yellow]{job_id}[/] checks out the pushed commit without a `{MERGE_ACTION}` step "
            "after its last such checkout"
        ]
    # The action is read from the checkout, which an older ref or a skipped checkout lacks, so it may
    # only be loaded when there is something to merge.
    return [
        f"job [yellow]{job_id}[/] runs `{MERGE_ACTION}` without an `if:` that checks `merge-with`"
        for index in merges
        if "merge-with" not in str(steps[index].get("if", ""))
    ]


def check_workflow(name: str, workflow: dict[Any, Any]) -> list[str]:
    errors = []
    if name not in TOP_WORKFLOWS:
        # PyYAML reads the bare `on:` key as boolean True.
        triggers = workflow.get("on", workflow.get(True)) or {}
        call = triggers.get("workflow_call") if isinstance(triggers, dict) else None
        if "merge-with" not in ((call or {}).get("inputs") or {}):
            errors.append("does not take a `merge-with` input")
    for job_id, job in (workflow.get("jobs") or {}).items():
        errors.extend(check_job(name, job_id, job))
    return errors


def main(workflows_dir: Path = WORKFLOWS_DIR) -> int:
    errors = [
        f"[red].github/workflows/{name}[/]: {error}"
        for name, workflow in sorted(find_fork_run_workflows(workflows_dir).items())
        for error in check_workflow(name, workflow)
    ]
    if not errors:
        return 0
    for error in errors:
        console.print(error)
    console.print(
        "\nFork CI tests the pushed commit merged with the base branch, so every job of a fork run must "
        f"merge it after checking it out: add a `uses: {MERGE_ACTION}` step taking `merge-with`, pass "
        "`merge-with` to called workflows, or add the job to EXEMPT_JOBS in "
        "scripts/ci/prek/check_fork_ci_tests_merge.py with the reason it does not test the sources."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
