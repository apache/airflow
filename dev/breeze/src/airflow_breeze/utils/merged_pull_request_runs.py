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
"""
Cancels the workflow runs that are still going for pull requests that have already merged.

Merging a pull request does not stop its `pull_request` runs, which keep using the project's
GitHub-hosted minutes and self-hosted runners for a result nobody reads. The fork CI monitor calls
this on its schedule. See "Cancelling runs of merged pull requests" in
dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.fork_ci_gate import parse_github_datetime

if TYPE_CHECKING:
    from airflow_breeze.utils.fork_ci_github import GitHubClient

# The statuses of a run that has not finished. A run waiting for approval is "completed".
UNFINISHED_RUN_STATUSES = ("queued", "in_progress", "waiting", "pending", "requested")
# A cancelled run whose jobs keep running, for example in a step that ignores the signal, is
# force-cancelled once this long has passed since the merge, which leaves a normal cancel at least
# one monitor run to work.
FORCE_CANCEL_AFTER = timedelta(minutes=30)
BRANCH_BATCH_SIZE = 25


@dataclass(frozen=True)
class UnfinishedRun:
    id: int
    workflow: str
    status: str
    head_repo: str
    head_branch: str
    head_sha: str
    created_at: datetime


@dataclass(frozen=True)
class PullRequestHead:
    number: int
    state: str
    head_repo: str
    head_sha: str
    merged_at: datetime | None


def list_unfinished_runs(client: GitHubClient, repo: str) -> list[UnfinishedRun]:
    runs = []
    for status in UNFINISHED_RUN_STATUSES:
        for page in client.rest_get_pages(
            f"repos/{repo}/actions/runs?event=pull_request&status={status}&per_page=100"
        ):
            for run in page["workflow_runs"]:
                runs.append(
                    UnfinishedRun(
                        id=run["id"],
                        workflow=run.get("name") or "",
                        status=run["status"],
                        head_repo=(run.get("head_repository") or {}).get("full_name") or "",
                        head_branch=run.get("head_branch") or "",
                        head_sha=run["head_sha"],
                        created_at=parse_github_datetime(run["created_at"]),  # type: ignore[arg-type]
                    )
                )
    return runs


PULL_REQUEST_FIELDS = (
    "pageInfo { hasNextPage endCursor } "
    "nodes { number state mergedAt headRefOid headRepository { nameWithOwner } }"
)
BRANCH_PAGE_QUERY = (
    "query($owner: String!, $name: String!, $branch: String!, $cursor: String) "
    "{ repository(owner: $owner, name: $name) { pullRequests(headRefName: $branch, first: 100, "
    f"after: $cursor) {{ {PULL_REQUEST_FIELDS} }} }} }}"
)


def _parse_pull_requests(connection: dict[str, Any] | None) -> list[PullRequestHead]:
    return [
        PullRequestHead(
            number=node["number"],
            state=node["state"],
            head_repo=(node.get("headRepository") or {}).get("nameWithOwner") or "",
            head_sha=node["headRefOid"],
            merged_at=parse_github_datetime(node.get("mergedAt") or ""),
        )
        for node in (connection or {}).get("nodes") or []
    ]


def find_pull_requests_by_branch(
    client: GitHubClient, repo: str, branches: Iterable[str]
) -> dict[str, list[PullRequestHead]]:
    """
    Every pull request of each head branch name, in any state and from any head repository.

    All of them are read, not only the latest: a common branch name such as ``main`` has pull
    requests from many forks, and an open one with a run's head must not be missed.
    """
    owner, name = repo.split("/", 1)
    found: dict[str, list[PullRequestHead]] = {}
    unique = sorted(set(branches))
    for start in range(0, len(unique), BRANCH_BATCH_SIZE):
        batch = unique[start : start + BRANCH_BATCH_SIZE]
        declarations = ", ".join(f"$b{index}: String!" for index in range(len(batch)))
        fields = " ".join(
            f"b{index}: pullRequests(headRefName: $b{index}, first: 100) {{ {PULL_REQUEST_FIELDS} }}"
            for index in range(len(batch))
        )
        data = client.graphql(
            f"query($owner: String!, $name: String!, {declarations}) "
            f"{{ repository(owner: $owner, name: $name) {{ {fields} }} }}",
            {"owner": owner, "name": name, **{f"b{index}": branch for index, branch in enumerate(batch)}},
        )["repository"]
        for index, branch in enumerate(batch):
            connection = data[f"b{index}"] or {}
            found[branch] = _parse_pull_requests(connection)
            while (connection.get("pageInfo") or {}).get("hasNextPage"):
                connection = client.graphql(
                    BRANCH_PAGE_QUERY,
                    {
                        "owner": owner,
                        "name": name,
                        "branch": branch,
                        "cursor": connection["pageInfo"]["endCursor"],
                    },
                )["repository"]["pullRequests"]
                found[branch].extend(_parse_pull_requests(connection))
    return found


def find_merged_pull_request(
    run: UnfinishedRun, pull_requests: Iterable[PullRequestHead]
) -> PullRequestHead | None:
    """
    The merged pull request a run was started for, if no open pull request has the same head.

    A run is matched by head repository, branch and commit, because runs of pull requests from
    forks do not list their pull request. Only runs started before the merge count: a workflow
    triggered by the merge itself, such as one on `closed`, is meant to run.
    """
    same_head = [
        pr
        for pr in pull_requests
        if pr.head_repo.casefold() == run.head_repo.casefold() and pr.head_sha == run.head_sha
    ]
    if any(pr.state == "OPEN" for pr in same_head):
        return None
    return next(
        (
            pr
            for pr in same_head
            if pr.state == "MERGED" and pr.merged_at is not None and run.created_at < pr.merged_at
        ),
        None,
    )


def cancel_merged_pull_request_runs(
    client: GitHubClient, *, repo: str, now: datetime
) -> list[tuple[int, str]]:
    """Cancel the unfinished runs of merged pull requests; return ``(pull request, problem)`` for Slack."""
    runs = list_unfinished_runs(client, repo)
    if not runs:
        return []
    pull_requests = find_pull_requests_by_branch(client, repo, (run.head_branch for run in runs))
    problems: list[tuple[int, str]] = []
    cancelled = 0
    for run in runs:
        merged = find_merged_pull_request(run, pull_requests.get(run.head_branch, []))
        if merged is None:
            continue
        force = (
            run.status == "in_progress"
            and merged.merged_at is not None
            and now - merged.merged_at >= FORCE_CANCEL_AFTER
        )
        try:
            client.cancel_run(merged.number, run.id, force=force)
            cancelled += 1
        except Exception as e:
            # A run that finished meanwhile cannot be cancelled; that is the outcome wanted anyway.
            # gh reports the status as "(HTTP 409)"; a bare "409" could be part of the run id.
            if "(HTTP 409)" in str(e):
                continue
            problems.append((merged.number, f"cancelling run {run.id} ({run.workflow}) failed: {e}"))
    verb = "Would cancel" if client.dry_run else "Cancelled"
    console_print(f"[info]{verb} {cancelled} runs of merged pull requests[/]")
    return problems
