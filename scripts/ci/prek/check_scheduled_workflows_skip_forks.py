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
#   "pyyaml>=6.0.3",
#   "rich>=13.6.0",
# ]
# ///
"""Check that every job in a scheduled workflow skips its cron runs in forks.

Forks that enable Actions for fork CI also get every cron. A job passes when its ``if:`` has the
``apache/airflow`` guard, or when it needs a job that passes and does not override the skip with
``always()``, ``cancelled()`` or ``failure()``. ``ci-amd.yml`` and ``ci-arm.yml`` are exempt
because they must run in forks on push, and their ``check-run-needed`` step skips scheduled runs there.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console

console = Console(color_system="standard", width=200)

EXEMPTIONS = frozenset({"ci-amd.yml", "ci-arm.yml"})
GUARD = "github.repository == 'apache/airflow'"
STATUS_CHECK_FUNCTIONS = frozenset({"always()", "cancelled()", "failure()"})


def is_covered(jobs: dict[str, Any], job_name: str) -> bool:
    """Whether the job skips scheduled runs in forks, through its own guard or a job it needs."""
    job = jobs[job_name]
    if_condition = job.get("if") or ""
    if GUARD in if_condition:
        return True
    if any(status_check in if_condition for status_check in STATUS_CHECK_FUNCTIONS):
        return False
    needs = job.get("needs") or []
    if isinstance(needs, str):
        needs = [needs]
    for parent in needs:
        if is_covered(jobs, parent):
            return True
    return False


def check_file(file_path: Path) -> list[str]:
    """Return the jobs of a scheduled workflow that would still run on schedule in forks."""
    if file_path.name in EXEMPTIONS:
        return []
    workflow = yaml.safe_load(file_path.read_text())
    # PyYAML reads the bare `on:` key as boolean True.
    triggers = workflow.get("on", workflow.get(True))
    if not (isinstance(triggers, dict) and "schedule" in triggers):
        return []
    jobs = workflow["jobs"]
    return [job_name for job_name in jobs if not is_covered(jobs, job_name)]


def main(file_paths: list[str]) -> int:
    errors = []
    for file_path in file_paths:
        path = Path(file_path)
        for job_name in check_file(path):
            errors.append(
                f"[red]{path}[/]: job [yellow]{job_name}[/] runs on schedule in forks. "
                f"Add `if: {GUARD}` to it, or make it need a job that has it."
            )
    if not errors:
        return 0
    for error in errors:
        console.print(error)
    console.print(
        "\nWorkflows with other triggers should use "
        f"`if: github.event_name != 'schedule' || {GUARD}`\n"
        "so manual, push and pull request runs still work in forks."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
