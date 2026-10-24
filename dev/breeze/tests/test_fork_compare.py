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

from airflow_breeze.global_constants import GithubEvents
from airflow_breeze.utils.fork_compare import (
    COMPARE_FILE_CAP,
    ForkChanges,
    UpstreamFileError,
    get_fork_changes,
    get_fork_pull_request_labels,
    get_upstream_file,
    is_fork_push,
)


@pytest.mark.parametrize(
    ("event", "repository", "expected"),
    [
        pytest.param(GithubEvents.PUSH, "someone/airflow", True, id="fork-push"),
        pytest.param(GithubEvents.PUSH, "apache/airflow", False, id="upstream-push"),
        pytest.param(GithubEvents.PULL_REQUEST, "someone/airflow", False, id="fork-pr"),
    ],
)
def test_is_fork_push(event, repository, expected):
    assert is_fork_push(event, repository) is expected


@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
@pytest.mark.parametrize(
    ("files", "expected"),
    [
        pytest.param([], ForkChanges(files=(), truncated=False), id="empty"),
        pytest.param([["a.py"], ["b.md"]], ForkChanges(files=("a.py", "b.md"), truncated=False), id="small"),
        pytest.param(
            [["docs/x.yml", ".github/workflows/x.yml"]],
            ForkChanges(files=("docs/x.yml", ".github/workflows/x.yml"), truncated=False),
            id="rename-keeps-both-paths",
        ),
        pytest.param(
            [[f"f{i}.py"] for i in range(COMPARE_FILE_CAP)],
            ForkChanges(files=tuple(f"f{i}.py" for i in range(COMPARE_FILE_CAP)), truncated=True),
            id="at-cap",
        ),
    ],
)
def test_get_fork_changes(mock_gh, files, expected):
    mock_gh.return_value = subprocess.CompletedProcess(
        [], 0, stdout=json.dumps({"files": files, "merge_base": None}), stderr=""
    )

    assert get_fork_changes(fork_owner="someone", sha="abc", base="main") == expected

    assert mock_gh.call_args.args[0][:3] == ["gh", "api", "repos/apache/airflow/compare/main...someone:abc"]


@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
def test_get_fork_changes_runs_everything_when_the_compare_fails(mock_gh):
    mock_gh.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="Not Found")

    assert get_fork_changes(fork_owner="someone", sha="abc", base="main") == ForkChanges(
        files=(), truncated=True
    )


@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
def test_get_fork_changes_returns_the_merge_base(mock_gh):
    mock_gh.return_value = subprocess.CompletedProcess(
        [], 0, stdout=json.dumps({"files": [["a.py"]], "merge_base": "base-sha"}), stderr=""
    )

    assert get_fork_changes(fork_owner="someone", sha="abc", base="main").merge_base == "base-sha"


@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
@pytest.mark.parametrize(
    ("returncode", "stdout", "expected"),
    [
        pytest.param(0, '["full tests needed"]', ("full tests needed",), id="pull-request"),
        pytest.param(0, "[]", (), id="no-pull-request"),
        pytest.param(1, "", (), id="call-fails"),
    ],
)
def test_get_fork_pull_request_labels(mock_gh, returncode, stdout, expected):
    mock_gh.return_value = subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="boom")

    assert get_fork_pull_request_labels(fork_owner="someone", branch="fix") == expected

    assert "head=someone:fix" in mock_gh.call_args.args[0]


@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
@pytest.mark.parametrize(
    ("returncode", "stderr", "expected"),
    [
        pytest.param(0, "", "content", id="found"),
        pytest.param(1, "gh: Not Found (HTTP 404)", None, id="missing"),
    ],
)
def test_get_upstream_file(mock_gh, returncode, stderr, expected):
    mock_gh.return_value = subprocess.CompletedProcess([], returncode, stdout="content", stderr=stderr)

    assert get_upstream_file(path="pyproject.toml", ref="base-sha") == expected

    assert mock_gh.call_args.args[0][-1] == "repos/apache/airflow/contents/pyproject.toml?ref=base-sha"


@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
def test_get_upstream_file_fails_on_errors_other_than_absence(mock_gh):
    mock_gh.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="HTTP 502: Bad Gateway")

    with pytest.raises(UpstreamFileError, match="502"):
        get_upstream_file(path="pyproject.toml", ref="base-sha")
