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
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.ci_commands import get_changed_files_since, selective_check
from airflow_breeze.utils.fork_compare import ForkChanges

PUSHED_COMMIT = "b" * 40
BASE_COMMIT = "a" * 40


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected"),
    [
        pytest.param(0, "a.py\nb/c.py\n", ("a.py", "b/c.py"), id="changed files"),
        pytest.param(0, "", (), id="no changes"),
        pytest.param(128, "", None, id="git diff failed"),
    ],
)
@patch("airflow_breeze.commands.ci_commands.run_command", autospec=True)
def test_get_changed_files_since(mock_run_command, returncode, stdout, expected):
    mock_run_command.return_value = CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr="")

    assert get_changed_files_since(base_ref=BASE_COMMIT, commit_ref=PUSHED_COMMIT) == expected
    assert mock_run_command.call_args.args[0] == [
        "git",
        "diff",
        "--name-only",
        "--no-renames",
        BASE_COMMIT,
        PUSHED_COMMIT,
    ]


def _invoke_selective_check(*extra_args: str):
    return CliRunner().invoke(
        selective_check,
        ["--commit-ref", PUSHED_COMMIT, "--github-event-name", "push", *extra_args],
        catch_exceptions=False,
    )


@pytest.mark.parametrize(
    ("changed_since_base", "expected_commit_ref", "expected_files"),
    [
        pytest.param(("a.py", "b.py"), PUSHED_COMMIT, ("a.py", "b.py"), id="files since the base commit"),
        pytest.param(None, None, (), id="all tests when the diff cannot be computed"),
    ],
)
@patch("airflow_breeze.commands.ci_commands.get_changed_files", autospec=True)
@patch("airflow_breeze.commands.ci_commands.get_changed_files_since", autospec=True)
@patch("airflow_breeze.utils.selective_checks.SelectiveChecks", autospec=True)
def test_selective_tests_on_push_select_from_files_since_base_commit(
    mock_selective_checks,
    mock_get_changed_files_since,
    mock_get_changed_files,
    changed_since_base,
    expected_commit_ref,
    expected_files,
):
    mock_get_changed_files_since.return_value = changed_since_base

    _invoke_selective_check("--selective-tests-on-push", "--push-base-commit", BASE_COMMIT)

    mock_get_changed_files_since.assert_called_once_with(base_ref=BASE_COMMIT, commit_ref=PUSHED_COMMIT)
    mock_get_changed_files.assert_not_called()
    assert mock_selective_checks.call_args.kwargs["commit_ref"] == expected_commit_ref
    assert mock_selective_checks.call_args.kwargs["files"] == expected_files


@patch("airflow_breeze.commands.ci_commands.get_changed_files", autospec=True)
@patch("airflow_breeze.commands.ci_commands.get_changed_files_since", autospec=True)
@patch("airflow_breeze.utils.selective_checks.SelectiveChecks", autospec=True)
def test_selective_tests_on_push_without_a_base_commit_run_all_tests(
    mock_selective_checks, mock_get_changed_files_since, mock_get_changed_files
):
    _invoke_selective_check("--selective-tests-on-push")

    mock_get_changed_files_since.assert_not_called()
    mock_get_changed_files.assert_not_called()
    assert mock_selective_checks.call_args.kwargs["commit_ref"] is None
    assert mock_selective_checks.call_args.kwargs["files"] == ()


@patch("airflow_breeze.commands.ci_commands.get_changed_files", autospec=True, return_value=("a.py",))
@patch("airflow_breeze.commands.ci_commands.get_changed_files_since", autospec=True)
@patch("airflow_breeze.utils.selective_checks.SelectiveChecks", autospec=True)
def test_push_without_selective_tests_on_push_uses_the_pushed_commit(
    mock_selective_checks, mock_get_changed_files_since, mock_get_changed_files
):
    _invoke_selective_check("--push-base-commit", BASE_COMMIT)

    mock_get_changed_files_since.assert_not_called()
    mock_get_changed_files.assert_called_once_with(commit_ref=PUSHED_COMMIT)
    assert mock_selective_checks.call_args.kwargs["files"] == ("a.py",)


def _invoke_fork_selective_check(repository: str):
    return _invoke_selective_check(
        "--github-repository",
        repository,
        "--github-context",
        json.dumps({"ref_name": "backport-abc-v3-1-test"}),
        "--default-branch",
        "v3-1-test",
    )


@patch("airflow_breeze.commands.ci_commands.get_fork_pull_request_labels", autospec=True)
@patch("airflow_breeze.commands.ci_commands.get_changed_files", autospec=True)
@patch("airflow_breeze.commands.ci_commands.get_fork_changes", autospec=True)
@patch("airflow_breeze.utils.selective_checks.SelectiveChecks", autospec=True)
def test_fork_push_takes_changed_files_from_compare(
    mock_selective_checks, mock_fork_changes, mock_changed_files, mock_labels
):
    mock_fork_changes.return_value = ForkChanges(files=("README.md",), truncated=True, merge_base="base-sha")
    mock_labels.return_value = ("full tests needed",)

    _invoke_fork_selective_check("someone/airflow")

    mock_fork_changes.assert_called_once_with(fork_owner="someone", sha=PUSHED_COMMIT, base="v3-1-test")
    mock_changed_files.assert_not_called()
    mock_labels.assert_called_once_with(
        fork_owner="someone", branch="backport-abc-v3-1-test", base="v3-1-test"
    )
    kwargs = mock_selective_checks.call_args.kwargs
    assert kwargs["files"] == ("README.md",)
    assert kwargs["fork_push"] is True
    assert kwargs["fork_changes_truncated"] is True
    assert kwargs["fork_merge_base"] == "base-sha"
    assert kwargs["pr_labels"] == ("full tests needed",)


@patch("airflow_breeze.commands.ci_commands.get_fork_pull_request_labels", autospec=True)
@patch("airflow_breeze.commands.ci_commands.get_changed_files", autospec=True, return_value=("README.md",))
@patch("airflow_breeze.commands.ci_commands.get_fork_changes", autospec=True)
@patch("airflow_breeze.utils.selective_checks.SelectiveChecks", autospec=True)
def test_upstream_push_uses_git_diff(
    mock_selective_checks, mock_fork_changes, mock_changed_files, mock_labels
):
    _invoke_fork_selective_check("apache/airflow")

    mock_fork_changes.assert_not_called()
    mock_labels.assert_not_called()
    assert mock_selective_checks.call_args.kwargs["fork_push"] is False
