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


@pytest.mark.parametrize(
    "stderr",
    [
        "gh: HTTP 504",
        "gh: Server Error (HTTP 502)",
        'Post "https://api.github.com/graphql": read tcp 10.1.0.4:51234->140.82.112.6:443: read: connection reset by peer',
        'Post "https://api.github.com/graphql": unexpected EOF',
        'Post "https://api.github.com/graphql": net/http: TLS handshake timeout',
        'Post "https://api.github.com/graphql": dial tcp 140.82.112.6:443: i/o timeout',
    ],
)
@mock.patch("airflow_breeze.utils.fork_ci_github.time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_retries_on_server_errors(mock_run, mock_sleep, stderr):
    mock_run.side_effect = [
        _completed(1, stderr=stderr),
        _completed(1, stderr=stderr),
        _completed(0, stdout=json.dumps({"data": {"ok": True}})),
    ]
    client = GitHubClient()

    assert client.graphql("query { ok }") == {"ok": True}
    assert client.calls["graphql"] == 3
    assert [c.args[0] for c in mock_sleep.call_args_list] == [10, 20]


@mock.patch("airflow_breeze.utils.fork_ci_github.time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_gives_up_on_a_lasting_server_error(mock_run, mock_sleep):
    mock_run.return_value = _completed(1, stderr="gh: HTTP 504")

    with pytest.raises(GitHubError, match="still failing after 4 attempts: gh: HTTP 504"):
        GitHubClient().rest_get("repos/x/y")
    assert [c.args[0] for c in mock_sleep.call_args_list] == [10, 20, 40]


@mock.patch("airflow_breeze.utils.fork_ci_github.time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_does_not_repeat_creating_a_comment_after_a_server_error(mock_run, mock_sleep):
    mock_run.return_value = _completed(1, stderr="gh: Server Error (HTTP 502)")

    with pytest.raises(GitHubError, match="502"):
        GitHubClient().create_comment(10, "body")
    mock_run.assert_called_once()
    mock_sleep.assert_not_called()


@pytest.mark.parametrize(
    "stderr",
    [
        'Post "https://api.github.com/repos/x/y/issues/10/comments": net/http: TLS handshake timeout',
        'Post "https://api.github.com/repos/x/y/issues/10/comments": dial tcp: lookup api.github.com: no such host',
    ],
)
@mock.patch("airflow_breeze.utils.fork_ci_github.time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_repeats_creating_a_comment_that_did_not_connect(mock_run, mock_sleep, stderr):
    mock_run.side_effect = [_completed(1, stderr=stderr), _completed(0, stdout="{}")]

    GitHubClient().create_comment(10, "body")
    assert mock_run.call_count == 2
    mock_sleep.assert_called_once_with(10)


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
@pytest.mark.parametrize("returncode", [pytest.param(0, id="exit-0"), pytest.param(1, id="gh-exits-1")])
def test_graphql_errors(mock_run, returncode, response, allow_partial, expected):
    mock_run.return_value = _completed(returncode, stdout=json.dumps(response), stderr="gh: errors")

    if expected is GitHubError:
        with pytest.raises(
            GitHubError, match="GraphQL errors" if returncode == 0 or allow_partial else "failed"
        ):
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
    client.delete_run(1, 7)

    mock_run.assert_not_called()


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
def test_delete_run(mock_run):
    mock_run.return_value = _completed()

    GitHubClient().delete_run(1, 7)

    assert mock_run.call_args.args[0] == ["gh", "api", "-X", "DELETE", "repos/apache/airflow/actions/runs/7"]


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


@mock.patch("airflow_breeze.utils.fork_ci_github.subprocess.run", autospec=True)
@pytest.mark.parametrize(
    ("kwargs", "context"),
    [
        pytest.param({}, "Fork CI / Tests (AMD)", id="fork-ci-status"),
        pytest.param({"context": "CI passed"}, "CI passed", id="ci-passed"),
    ],
)
def test_set_status_context(mock_run, kwargs, context):
    mock_run.return_value = _completed()

    GitHubClient().set_status(1, "abc", "success", "https://x", "desc", **kwargs)

    assert json.loads(mock_run.call_args.kwargs["input"])["context"] == context
