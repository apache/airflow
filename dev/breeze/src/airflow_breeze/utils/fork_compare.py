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
"""Changed files of a push in a contributor fork, computed against apache/airflow (AIP-120)."""

from __future__ import annotations

import json
from dataclasses import dataclass

from airflow_breeze.global_constants import APACHE_AIRFLOW_GITHUB_REPOSITORY, GithubEvents
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.github import run_gh_command

# The compare API lists at most this many files: a list that reaches it may be missing some, so
# the change set is treated as truncated.
COMPARE_FILE_CAP = 300


@dataclass(frozen=True)
class ForkChanges:
    files: tuple[str, ...]
    truncated: bool


def is_fork_push(github_event: GithubEvents, github_repository: str) -> bool:
    return github_event == GithubEvents.PUSH and github_repository != APACHE_AIRFLOW_GITHUB_REPOSITORY


def get_fork_changes(*, fork_owner: str, sha: str, base: str) -> ForkChanges:
    """
    Three-dot compare of the pushed commit against the upstream base branch.

    Three-dot semantics give the same file list a pull request against upstream would show,
    so a fork whose ``main`` is behind upstream does not inflate the change set. ``base`` is the
    branch line the pushed code belongs to (breeze's default branch at that commit), which a
    branch name cannot be trusted to tell. A renamed file counts under both paths, as in
    ``git diff``. When the compare fails the change set is reported as truncated, so the run
    covers everything instead of failing.
    """
    result = run_gh_command(
        [
            "gh",
            "api",
            f"repos/{APACHE_AIRFLOW_GITHUB_REPOSITORY}/compare/{base}...{fork_owner}:{sha}",
            "--jq",
            "[.files[] | [.filename, (.previous_filename // empty)]]",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console_print(
            f"[warning]Could not compare {fork_owner}:{sha} with {APACHE_AIRFLOW_GITHUB_REPOSITORY}:{base}, "
            f"so everything runs: {result.stderr.strip()}[/]"
        )
        return ForkChanges(files=(), truncated=True)
    entries = json.loads(result.stdout or "[]")
    files = tuple(path for entry in entries for path in entry)
    return ForkChanges(files=files, truncated=len(entries) >= COMPARE_FILE_CAP)
