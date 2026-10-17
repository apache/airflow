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

import json
import subprocess
from unittest import mock

import pytest

from airflow_breeze.utils.fork_ci_github import GitHubClient, GitHubError


def _completed(returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


@mock.patch("airflow_breeze.utils.fork_ci_github.time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_retries_on_rate_limit(mock_run, mock_sleep):
    mock_run.side_effect = [
        _completed(1, stderr="API rate limit exceeded"),
        _completed(0, stdout=json.dumps({"data": {"ok": True}})),
    ]
    client = GitHubClient()

    assert client.graphql("query { ok }") == {"ok": True}
    assert client.calls["graphql"] == 2
    mock_sleep.assert_called_once_with(30)


@mock.patch("airflow_breeze.utils.fork_ci_github.time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_gives_up_after_the_last_attempt_without_waiting(mock_run, mock_sleep):
    mock_run.return_value = _completed(1, stderr="API rate limit exceeded")

    with pytest.raises(GitHubError, match="still rate limited after 4 attempts"):
        GitHubClient().graphql("query { ok }")
    assert [c.args[0] for c in mock_sleep.call_args_list] == [30, 60, 120]


@mock.patch(
    "airflow_breeze.utils.fork_ci_github.subprocess.run",
    autospec=True,
    side_effect=subprocess.TimeoutExpired(["gh"], 120),
)
def test_a_hung_gh_is_an_error(mock_run):
    with pytest.raises(GitHubError, match="timed out after 120s"):
        GitHubClient().graphql("query { ok }")
    assert mock_run.call_args.kwargs["timeout"] == 120


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_raises_on_other_errors(mock_run):
    mock_run.return_value = _completed(1, stderr="HTTP 404: Not Found")

    with pytest.raises(GitHubError, match="404"):
        GitHubClient().rest_get("repos/x/y")


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
@pytest.mark.parametrize(
    ("response", "allow_partial", "expected"),
    [
        pytest.param({"data": {"a": None}, "errors": [{"m": "x"}]}, True, {"a": None}, id="partial-allowed"),
        pytest.param({"data": {"a": None}, "errors": [{"m": "x"}]}, False, GitHubError, id="partial-refused"),
        pytest.param({"data": None, "errors": [{"m": "x"}]}, True, GitHubError, id="no-data"),
    ],
)
def test_graphql_errors(mock_run, response, allow_partial, expected):
    mock_run.return_value = _completed(stdout=json.dumps(response))

    if expected is GitHubError:
        with pytest.raises(GitHubError, match="GraphQL errors"):
            GitHubClient().graphql("q", allow_partial=allow_partial)
    else:
        assert GitHubClient().graphql("q", allow_partial=allow_partial) == expected


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_rest_get_flattens_pages(mock_run):
    mock_run.return_value = _completed(stdout=json.dumps([[{"n": 1}], [{"n": 2}]]))

    assert GitHubClient().rest_get("repos/x/pulls/1/files", paginate=True) == [{"n": 1}, {"n": 2}]
    assert mock_run.call_args.args[0] == ["gh", "api", "repos/x/pulls/1/files", "--paginate", "--slurp"]


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_rest_get_flattens_object_pages(mock_run):
    pages = [{"total_count": 2, "jobs": [{"n": 1}]}, {"total_count": 2, "jobs": [{"n": 2}]}]
    mock_run.return_value = _completed(stdout=json.dumps(pages))

    assert GitHubClient().rest_get("repos/x/actions/runs/1/jobs", paginate=True, items_key="jobs") == [
        {"n": 1},
        {"n": 2},
    ]


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_rest_get_pages_keeps_page_fields(mock_run):
    pages = [{"total_count": 1500, "workflow_runs": [{"id": 1}]}]
    mock_run.return_value = _completed(stdout=json.dumps(pages))

    assert GitHubClient().rest_get_pages("repos/x/actions/runs") == pages


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_dry_run_does_not_mutate(mock_run):
    client = GitHubClient(dry_run=True)

    client.close(1)
    client.remove_label(1, "fork ci required")

    mock_run.assert_not_called()


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_remove_label_quotes_the_name(mock_run):
    mock_run.return_value = _completed()

    GitHubClient().remove_label(1, "ready for maintainer review")

    assert mock_run.call_args.args[0] == [
        "gh",
        "api",
        "-X",
        "DELETE",
        "repos/apache/airflow/issues/1/labels/ready%20for%20maintainer%20review",
    ]
