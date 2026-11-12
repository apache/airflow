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
    _get_local_git_changes,
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


@mock.patch("airflow_breeze.utils.fork_compare._get_local_git_changes", autospec=True)
@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
def test_get_fork_changes_falls_back_to_local_git_at_cap(mock_gh, mock_local_git):
    mock_gh.return_value = subprocess.CompletedProcess(
        [],
        0,
        stdout=json.dumps(
            {"files": [[f"f{i}.py"] for i in range(COMPARE_FILE_CAP)], "merge_base": "base-sha"}
        ),
        stderr="",
    )
    mock_local_git.return_value = ForkChanges(
        files=tuple(f"f{i}.py" for i in range(350)), truncated=False, merge_base="local-mb-sha"
    )

    result = get_fork_changes(fork_owner="someone", sha="abc", base="main")

    assert result == ForkChanges(
        files=tuple(f"f{i}.py" for i in range(350)), truncated=False, merge_base="local-mb-sha"
    )
    mock_local_git.assert_called_once_with(base="main", sha="abc", known_merge_base="base-sha")


@mock.patch("airflow_breeze.utils.fork_compare._get_local_git_changes", autospec=True)
@mock.patch("airflow_breeze.utils.fork_compare.run_gh_command", autospec=True)
def test_get_fork_changes_falls_back_to_local_git_when_gh_fails(mock_gh, mock_local_git):
    mock_gh.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="API rate limit exceeded")
    mock_local_git.return_value = ForkChanges(
        files=("file1.py", "file2.py"), truncated=False, merge_base="local-mb-sha"
    )

    result = get_fork_changes(fork_owner="someone", sha="abc", base="main")

    assert result == ForkChanges(files=("file1.py", "file2.py"), truncated=False, merge_base="local-mb-sha")
    mock_local_git.assert_called_once_with(base="main", sha="abc", known_merge_base=None)


@mock.patch("airflow_breeze.utils.fork_compare.run_command", autospec=True)
def test_get_local_git_changes_uses_known_merge_base(mock_run_cmd):
    mock_run_cmd.side_effect = [
        subprocess.CompletedProcess([], 0, stdout="", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="M\0foo.py\0", stderr=""),
    ]

    result = _get_local_git_changes(base="main", sha="head-sha", known_merge_base="known-mb")

    assert result == ForkChanges(files=("foo.py",), truncated=False, merge_base="known-mb")


@mock.patch("airflow_breeze.utils.fork_compare.run_command", autospec=True)
def test_get_local_git_changes_success(mock_run_cmd):
    mock_run_cmd.side_effect = [
        subprocess.CompletedProcess([], 0, stdout="", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="mb-sha-123\n", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="M\0foo.py\0R100\0old.py\0new.py\0", stderr=""),
    ]

    result = _get_local_git_changes(base="main", sha="head-sha")

    assert result == ForkChanges(
        files=("foo.py", "old.py", "new.py"), truncated=False, merge_base="mb-sha-123"
    )


@mock.patch("time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_compare.run_command", autospec=True)
def test_get_local_git_changes_retries_fetch(mock_run_cmd, mock_sleep):
    mock_run_cmd.side_effect = [
        subprocess.CompletedProcess([], 1, stdout="", stderr="transient network error"),
        subprocess.CompletedProcess([], 0, stdout="", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="mb-sha\n", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="M\0bar.py\0", stderr=""),
    ]

    result = _get_local_git_changes(base="main", sha="head-sha")

    assert result == ForkChanges(files=("bar.py",), truncated=False, merge_base="mb-sha")
    mock_sleep.assert_called_once_with(1)


@mock.patch("time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_compare.run_command", autospec=True)
def test_get_local_git_changes_falls_back_to_existing_upstream_ref(mock_run_cmd, mock_sleep):
    mock_run_cmd.side_effect = [
        subprocess.CompletedProcess([], 1, stdout="", stderr="network error 1"),
        subprocess.CompletedProcess([], 1, stdout="", stderr="network error 2"),
        subprocess.CompletedProcess([], 1, stdout="", stderr="network error 3"),
        subprocess.CompletedProcess([], 0, stdout="refs/remotes/upstream/main\n", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="mb-sha\n", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="M\0cached.py\0", stderr=""),
    ]

    result = _get_local_git_changes(base="main", sha="head-sha")

    assert result == ForkChanges(files=("cached.py",), truncated=False, merge_base="mb-sha")
    assert mock_sleep.call_count == 2


@mock.patch("time.sleep", autospec=True)
@mock.patch("airflow_breeze.utils.fork_compare.run_command", autospec=True)
def test_get_local_git_changes_returns_none_when_fetch_and_ref_fail(mock_run_cmd, mock_sleep):
    mock_run_cmd.side_effect = [
        subprocess.CompletedProcess([], 1, stdout="", stderr="error 1"),
        subprocess.CompletedProcess([], 1, stdout="", stderr="error 2"),
        subprocess.CompletedProcess([], 1, stdout="", stderr="error 3"),
        subprocess.CompletedProcess([], 1, stdout="", stderr="ref missing"),
    ]

    result = _get_local_git_changes(base="main", sha="head-sha")

    assert result is None


@mock.patch("airflow_breeze.utils.fork_compare.run_command", autospec=True)
def test_get_local_git_changes_parses_null_delimited_with_spaces_and_copies(mock_run_cmd):
    diff_output = (
        "M\0path with spaces/file 1.py\0"
        "C100\0source path/original.py\0dest path/copy.py\0"
        "R090\0old dir/rename.py\0new dir/renamed.py\0"
        "D\0deleted.py\0"
    )
    mock_run_cmd.side_effect = [
        subprocess.CompletedProcess([], 0, stdout="", stderr=""),
        subprocess.CompletedProcess([], 0, stdout="mb-sha\n", stderr=""),
        subprocess.CompletedProcess([], 0, stdout=diff_output, stderr=""),
    ]

    result = _get_local_git_changes(base="main", sha="head-sha")

    assert result == ForkChanges(
        files=(
            "path with spaces/file 1.py",
            "source path/original.py",
            "dest path/copy.py",
            "old dir/rename.py",
            "new dir/renamed.py",
            "deleted.py",
        ),
        truncated=False,
        merge_base="mb-sha",
    )


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

    assert get_fork_pull_request_labels(fork_owner="someone", branch="fix", base="v3-1-test") == expected

    assert "head=someone:fix" in mock_gh.call_args.args[0]
    assert "base=v3-1-test" in mock_gh.call_args.args[0]


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
