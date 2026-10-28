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
# ]
# ///
"""Check that the ``CI passed`` gate job in ``ci-amd.yml`` needs every other job.

``CI passed`` is the check main's ruleset requires, and auto-merge waits for it. It only fails
when a job it needs failed, so a job missing from its ``needs`` could fail and the PR would still
auto-merge. Reporting-only jobs that cannot make a PR wrong are exempt.

``Check for timed-out CI jobs`` fails the gate when a job hit its timeout. It must need the same jobs
as the gate, or a timeout in a job it misses would pass.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

AIRFLOW_ROOT = Path(__file__).parents[3]
CI_WORKFLOW = AIRFLOW_ROOT / ".github/workflows/ci-amd.yml"
GATE_JOB = "ci-passed"
TIMEOUT_JOB = "check-timed-out-jobs"
EXEMPT_JOBS = {"notify-slack", "summarize-warnings"}


def find_problems(jobs: dict) -> list[str]:
    if GATE_JOB not in jobs:
        return [f"job {GATE_JOB!r} is missing"]
    needs = jobs[GATE_JOB].get("needs") or []
    needs = {needs} if isinstance(needs, str) else set(needs)
    expected = set(jobs) - EXEMPT_JOBS - {GATE_JOB}
    timeout_needs = jobs.get(TIMEOUT_JOB, {}).get("needs") or []
    timeout_needs = {timeout_needs} if isinstance(timeout_needs, str) else set(timeout_needs)
    problems = [
        f"job {job!r} is not in {GATE_JOB}.needs, so its failure would not stop auto-merge"
        for job in sorted(expected - needs)
    ]
    problems += [f"{GATE_JOB}.needs lists {job!r}, which is not a job" for job in sorted(needs - set(jobs))]
    problems += [
        f"{GATE_JOB}.needs lists exempt reporting job {job!r}" for job in sorted(needs & EXEMPT_JOBS)
    ]
    if TIMEOUT_JOB in jobs:
        problems += [
            f"{TIMEOUT_JOB}.needs is missing {job!r}, which {GATE_JOB} needs"
            for job in sorted(needs - {TIMEOUT_JOB} - timeout_needs)
        ]
        problems += [
            f"{TIMEOUT_JOB}.needs lists {job!r}, which {GATE_JOB} does not need"
            for job in sorted(timeout_needs - needs)
        ]
    return problems


def main() -> int:
    jobs = yaml.safe_load(CI_WORKFLOW.read_text())["jobs"]
    problems = find_problems(jobs)
    for problem in problems:
        print(f"{CI_WORKFLOW.relative_to(AIRFLOW_ROOT)}: {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
