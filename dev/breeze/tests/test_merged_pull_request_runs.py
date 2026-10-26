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
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

from airflow_breeze.utils.fork_ci_github import GitHubClient, GitHubError
from airflow_breeze.utils.merged_pull_request_runs import (
    BRANCH_BATCH_SIZE,
    PullRequestHead,
    UnfinishedRun,
    cancel_merged_pull_request_runs,
    find_merged_pull_request,
    find_pull_requests_by_branch,
)

REPO = "apache/airflow"
NOW = datetime(2026, 10, 25, 19, 0, tzinfo=UTC)
MERGED_AT = NOW - timedelta(minutes=10)
RUN = UnfinishedRun(
    id=7,
    workflow="Tests (AMD)",
    status="in_progress",
    head_repo="someone/airflow",
    head_branch="fix",
    head_sha="aaa",
    created_at=MERGED_AT - timedelta(minutes=20),
)
MERGED = PullRequestHead(
    number=10, state="MERGED", head_repo="someone/airflow", head_sha="aaa", merged_at=MERGED_AT
)


@pytest.fixture
def client():
    github = mock.create_autospec(GitHubClient, instance=True)
    github.repo = REPO
    github.dry_run = False
    return github


@pytest.mark.parametrize(
    ("pull_requests", "expected"),
    [
        pytest.param([MERGED], MERGED, id="merged-pull-request"),
        pytest.param(
            [PullRequestHead(10, "MERGED", "Someone/Airflow", "aaa", MERGED_AT)],
            PullRequestHead(10, "MERGED", "Someone/Airflow", "aaa", MERGED_AT),
            id="head-repository-in-another-case",
        ),
        pytest.param(
            [PullRequestHead(11, "OPEN", "someone/airflow", "aaa", None), MERGED],
            None,
            id="open-pr-same-head",
        ),
        pytest.param(
            [PullRequestHead(10, "MERGED", "other/airflow", "aaa", MERGED_AT)], None, id="other-fork"
        ),
        pytest.param(
            [PullRequestHead(10, "MERGED", "someone/airflow", "bbb", MERGED_AT)], None, id="other-commit"
        ),
        pytest.param(
            [PullRequestHead(10, "MERGED", "someone/airflow", "aaa", RUN.created_at - timedelta(minutes=1))],
            None,
            id="run-started-after-the-merge",
        ),
        pytest.param(
            [PullRequestHead(10, "CLOSED", "someone/airflow", "aaa", None)], None, id="closed-not-merged"
        ),
        pytest.param([], None, id="no-pull-request"),
    ],
)
def test_find_merged_pull_request(pull_requests, expected):
    assert find_merged_pull_request(RUN, pull_requests) == expected


def _run(run_id, *, status="in_progress", branch="fix", sha="aaa", created_at=RUN.created_at):
    return {
        "id": run_id,
        "name": "Tests (AMD)",
        "status": status,
        "head_repository": {"full_name": "someone/airflow"},
        "head_branch": branch,
        "head_sha": sha,
        "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _pr_node(number, state, sha="aaa", merged_at=MERGED_AT):
    return {
        "number": number,
        "state": state,
        "mergedAt": merged_at.strftime("%Y-%m-%dT%H:%M:%SZ") if merged_at else None,
        "headRefOid": sha,
        "headRepository": {"nameWithOwner": "someone/airflow"},
    }


def _runs_by_status(runs):
    def pages(path):
        status = path.split("status=", 1)[1].split("&", 1)[0]
        return [{"workflow_runs": [run for run in runs if run["status"] == status]}]

    return pages


@pytest.mark.parametrize(
    ("merged_minutes_ago", "status", "force"),
    [
        pytest.param(10, "in_progress", False, id="recently-merged"),
        pytest.param(31, "in_progress", True, id="still-running-long-after-the-merge"),
        pytest.param(31, "queued", False, id="queued-long-after-the-merge"),
    ],
)
def test_cancel_merged_pull_request_runs(client, merged_minutes_ago, status, force):
    merged_at = NOW - timedelta(minutes=merged_minutes_ago)
    client.rest_get_pages.side_effect = _runs_by_status(
        [_run(7, status=status, created_at=merged_at - timedelta(minutes=5))]
    )
    client.graphql.return_value = {
        "repository": {"b0": {"nodes": [_pr_node(10, "MERGED", merged_at=merged_at)]}}
    }

    assert cancel_merged_pull_request_runs(client, repo=REPO, now=NOW) == []

    client.cancel_run.assert_called_once_with(10, 7, force=force)


def test_cancel_merged_pull_request_runs_leaves_open_pull_requests_alone(client):
    client.rest_get_pages.side_effect = _runs_by_status([_run(7)])
    client.graphql.return_value = {"repository": {"b0": {"nodes": [_pr_node(10, "OPEN", merged_at=None)]}}}

    cancel_merged_pull_request_runs(client, repo=REPO, now=NOW)

    client.cancel_run.assert_not_called()


def test_cancel_merged_pull_request_runs_without_unfinished_runs(client):
    client.rest_get_pages.side_effect = _runs_by_status([])

    assert cancel_merged_pull_request_runs(client, repo=REPO, now=NOW) == []
    client.graphql.assert_not_called()


@pytest.mark.parametrize(
    ("error", "problems"),
    [
        pytest.param(
            GitHubError(
                "gh api -X POST repos/apache/airflow/actions/runs/7/cancel failed: gh: done (HTTP 409)"
            ),
            [],
            id="finished-meanwhile",
        ),
        pytest.param(
            GitHubError("gh api repos/apache/airflow/actions/runs/1409/cancel failed: HTTP 500"),
            [
                (
                    10,
                    "cancelling run 7 (Tests (AMD)) failed: gh api repos/apache/airflow/actions/runs/1409/cancel failed: HTTP 500",
                )
            ],
            id="failed-with-409-in-the-path",
        ),
    ],
)
def test_cancel_merged_pull_request_runs_reports_failed_cancellations(client, error, problems):
    client.rest_get_pages.side_effect = _runs_by_status([_run(7), _run(8)])
    client.graphql.return_value = {"repository": {"b0": {"nodes": [_pr_node(10, "MERGED")]}}}
    client.cancel_run.side_effect = [error, None]

    assert cancel_merged_pull_request_runs(client, repo=REPO, now=NOW) == problems
    assert client.cancel_run.call_count == 2


def test_find_pull_requests_by_branch_batches_branches(client):
    branches = [f"branch-{index:02d}" for index in range(BRANCH_BATCH_SIZE + 1)]

    def graphql(query, variables):
        batch = sorted(key for key in variables if key.startswith("b"))
        return {"repository": {key: {"nodes": [_pr_node(1, "MERGED")]} for key in batch}}

    client.graphql.side_effect = graphql

    found = find_pull_requests_by_branch(client, REPO, [*branches, branches[0]])

    assert sorted(found) == branches
    assert client.graphql.call_count == 2
    assert client.graphql.call_args_list[0].args[1]["owner"] == "apache"


def test_find_pull_requests_by_branch_reads_every_page(client):
    first = {"pageInfo": {"hasNextPage": True, "endCursor": "c1"}, "nodes": [_pr_node(1, "MERGED")]}
    second = {
        "pageInfo": {"hasNextPage": False, "endCursor": None},
        "nodes": [_pr_node(2, "OPEN", merged_at=None)],
    }
    client.graphql.side_effect = [
        {"repository": {"b0": first}},
        {"repository": {"pullRequests": second}},
    ]

    found = find_pull_requests_by_branch(client, REPO, ["main"])

    assert [pr.number for pr in found["main"]] == [1, 2]
    assert client.graphql.call_args.args[1] == {
        "owner": "apache",
        "name": "airflow",
        "branch": "main",
        "cursor": "c1",
    }


def test_open_pull_request_on_a_later_page_keeps_the_run(client):
    client.rest_get_pages.side_effect = _runs_by_status([_run(7)])
    client.graphql.side_effect = [
        {
            "repository": {
                "b0": {
                    "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                    "nodes": [_pr_node(10, "MERGED")],
                }
            }
        },
        {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": False},
                    "nodes": [_pr_node(11, "OPEN", merged_at=None)],
                }
            }
        },
    ]

    cancel_merged_pull_request_runs(client, repo=REPO, now=NOW)

    client.cancel_run.assert_not_called()


@pytest.mark.parametrize(("dry_run", "summary"), [(False, "Cancelled 1 runs"), (True, "Would cancel 1 runs")])
@mock.patch("airflow_breeze.utils.merged_pull_request_runs.console_print", autospec=True)
def test_cancel_merged_pull_request_runs_summary_tells_a_dry_run(mock_print, client, dry_run, summary):
    client.dry_run = dry_run
    client.rest_get_pages.side_effect = _runs_by_status([_run(7)])
    client.graphql.return_value = {"repository": {"b0": {"nodes": [_pr_node(10, "MERGED")]}}}

    cancel_merged_pull_request_runs(client, repo=REPO, now=NOW)

    assert summary in mock_print.call_args.args[0]
