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
from unittest import mock

from click.testing import CliRunner

from airflow_breeze.commands.ci_commands import selective_check
from airflow_breeze.utils.fork_compare import ForkChanges


def _invoke(repository: str, event: str = "push"):
    return CliRunner().invoke(
        selective_check,
        [
            "--commit-ref",
            "abc123",
            "--github-event-name",
            event,
            "--github-repository",
            repository,
            "--github-context",
            json.dumps({"ref_name": "backport-abc-v3-1-test"}),
            "--default-branch",
            "v3-1-test",
        ],
    )


@mock.patch("airflow_breeze.commands.ci_commands.get_fork_pull_request_labels", autospec=True)
@mock.patch("airflow_breeze.commands.ci_commands.get_changed_files", autospec=True)
@mock.patch("airflow_breeze.commands.ci_commands.get_fork_changes", autospec=True)
def test_fork_push_takes_changed_files_from_compare(mock_fork_changes, mock_changed_files, mock_labels):
    mock_fork_changes.return_value = ForkChanges(files=("README.md",), truncated=True, merge_base="base-sha")
    mock_labels.return_value = ("full tests needed",)

    with mock.patch("airflow_breeze.utils.selective_checks.SelectiveChecks", autospec=True) as mock_sc:
        result = _invoke("someone/airflow")

    assert result.exit_code == 0, result.output
    mock_fork_changes.assert_called_once_with(fork_owner="someone", sha="abc123", base="v3-1-test")
    mock_changed_files.assert_not_called()
    kwargs = mock_sc.call_args.kwargs
    assert kwargs["files"] == ("README.md",)
    assert kwargs["fork_push"] is True
    assert kwargs["fork_changes_truncated"] is True
    assert kwargs["fork_merge_base"] == "base-sha"
    mock_labels.assert_called_once_with(fork_owner="someone", branch="backport-abc-v3-1-test")
    assert kwargs["pr_labels"] == ("full tests needed",)


@mock.patch("airflow_breeze.commands.ci_commands.get_changed_files", autospec=True)
@mock.patch("airflow_breeze.commands.ci_commands.get_fork_changes", autospec=True)
@mock.patch("airflow_breeze.commands.ci_commands.get_fork_pull_request_labels", autospec=True)
def test_upstream_push_uses_git_diff(mock_labels, mock_fork_changes, mock_changed_files):
    mock_changed_files.return_value = ("README.md",)

    with mock.patch("airflow_breeze.utils.selective_checks.SelectiveChecks", autospec=True) as mock_sc:
        result = _invoke("apache/airflow")

    assert result.exit_code == 0, result.output
    mock_fork_changes.assert_not_called()
    mock_labels.assert_not_called()
    assert mock_sc.call_args.kwargs["fork_push"] is False
