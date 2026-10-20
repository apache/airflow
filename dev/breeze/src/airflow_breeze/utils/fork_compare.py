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
    merge_base: str | None = None


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

    ``merge_base`` is the upstream commit the changes are measured from. Checks that compare a
    file with its previous version use it instead of the pushed commit's parent, which after a
    merge of upstream ``main`` into the branch is the contributor's own previous tip.
    """
    result = run_gh_command(
        [
            "gh",
            "api",
            f"repos/{APACHE_AIRFLOW_GITHUB_REPOSITORY}/compare/{base}...{fork_owner}:{sha}",
            "--jq",
            "{files: [.files[] | [.filename, (.previous_filename // empty)]], "
            "merge_base: .merge_base_commit.sha}",
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
    compare = json.loads(result.stdout or "{}")
    entries = compare.get("files", [])
    files = tuple(path for entry in entries for path in entry)
    return ForkChanges(
        files=files, truncated=len(entries) >= COMPARE_FILE_CAP, merge_base=compare.get("merge_base")
    )


def get_fork_pull_request_labels(*, fork_owner: str, branch: str) -> tuple[str, ...]:
    """
    Labels of the open apache/airflow pull request from ``fork_owner:branch``.

    A fork push carries no pull request, so labels such as ``full tests needed`` or
    ``allow provider dependency bump`` are read here. A label added later takes effect on the
    next fork run. No pull request, or a failed call, means no labels.
    """
    result = run_gh_command(
        [
            "gh",
            "api",
            "--method",
            "GET",
            f"repos/{APACHE_AIRFLOW_GITHUB_REPOSITORY}/pulls",
            "-f",
            f"head={fork_owner}:{branch}",
            "-f",
            "state=open",
            "--jq",
            "[.[0].labels[]?.name]",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console_print(
            f"[warning]Could not read the labels of the pull request from {fork_owner}:{branch}: "
            f"{result.stderr.strip()}[/]"
        )
        return ()
    return tuple(json.loads(result.stdout or "[]"))


class UpstreamFileError(RuntimeError):
    """A file of apache/airflow could not be read for a reason other than its absence."""


def get_upstream_file(*, path: str, ref: str) -> str | None:
    """
    Content of ``path`` at ``ref`` in apache/airflow, or None when the file does not exist there.

    Any other failure raises: the checks that compare a file with its previous version would
    otherwise pass silently for a change they never saw.
    """
    result = run_gh_command(
        [
            "gh",
            "api",
            "-H",
            "Accept: application/vnd.github.raw",
            f"repos/{APACHE_AIRFLOW_GITHUB_REPOSITORY}/contents/{path}?ref={ref}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout
    if "404" in result.stderr or "Not Found" in result.stderr:
        return None
    raise UpstreamFileError(
        f"Could not read {path} at {ref} in {APACHE_AIRFLOW_GITHUB_REPOSITORY}: {result.stderr.strip()}"
    )
