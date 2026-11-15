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

from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.ci_commands import use_fork_ci, use_project_ci
from airflow_breeze.utils.fork_ci_github import GitHubClient, GitHubError
from airflow_breeze.utils.fork_ci_messages import (
    CI_PASSED_PROJECT_DESCRIPTION,
    CI_PASSED_WAITING_PROJECT_CI_DESCRIPTION,
)
from airflow_breeze.utils.fork_ci_pr_move import move_to_fork_ci, move_to_project_ci

HEAD = "a" * 40
FORK_SUCCESS = {
    "context": "CI passed",
    "state": "success",
    "description": "Fork CI passed; base main@bbbbbbbbbb was 3 commits behind, no overlap",
}


def _client(labels=(), statuses=(), state="open"):
    client = mock.create_autospec(GitHubClient, instance=True)
    client.repo = "apache/airflow"
    pr = {"state": state, "head": {"sha": HEAD}, "labels": [{"name": label} for label in labels]}

    def rest_get(path, paginate=False, items_key=None):
        if path == "repos/apache/airflow/pulls/42":
            return pr
        if path == f"repos/apache/airflow/commits/{HEAD}/status?per_page=100":
            return [{"context": "Fork CI / Tests (AMD)", "state": "success"}, *statuses]
        raise AssertionError(path)

    client.rest_get.side_effect = rest_get
    return client


def test_move_to_project_ci_withholds_the_fork_success_before_labelling():
    client = _client(labels=("fork ci required",), statuses=[FORK_SUCCESS])

    done = move_to_project_ci(client, 42)

    assert client.mock_calls[2:] == [
        mock.call.set_status(
            42,
            HEAD,
            "pending",
            "https://github.com/apache/airflow/pull/42/checks",
            CI_PASSED_WAITING_PROJECT_CI_DESCRIPTION,
            context="CI passed",
        ),
        mock.call.add_labels(42, ["use project ci"]),
        mock.call.close(42),
        mock.call.reopen(42),
    ]
    assert done == [
        "withheld the fork CI `CI passed` success on aaaaaaaaaa",
        "added `use project ci`",
        "closed and reopened it to start project CI",
    ]


@pytest.mark.parametrize(
    "statuses",
    [
        pytest.param([], id="no-ci-passed"),
        pytest.param(
            [{"context": "CI passed", "state": "success", "description": CI_PASSED_PROJECT_DESCRIPTION}],
            id="project-ci-success",
        ),
        pytest.param([{**FORK_SUCCESS, "state": "pending"}], id="already-pending"),
    ],
)
def test_move_to_project_ci_keeps_a_status_it_must_not_withhold(statuses):
    client = _client(statuses=statuses)

    move_to_project_ci(client, 42, restart=False)

    client.set_status.assert_not_called()
    client.add_labels.assert_called_once_with(42, ["use project ci"])
    client.close.assert_not_called()


def test_move_to_project_ci_replaces_use_fork_ci():
    client = _client(labels=("use fork ci",))

    done = move_to_project_ci(client, 42, restart=False)

    client.remove_label.assert_called_once_with(42, "use fork ci")
    assert done == ["removed `use fork ci`", "added `use project ci`"]


def test_move_to_project_ci_on_a_labelled_pull_request_still_restarts_it():
    client = _client(labels=("use project ci",))

    assert move_to_project_ci(client, 42) == ["closed and reopened it to start project CI"]
    client.add_labels.assert_not_called()


@pytest.mark.parametrize(
    ("labels", "removed", "added", "done"),
    [
        pytest.param(
            ("use project ci",),
            ["use project ci"],
            [["use fork ci"]],
            ["removed `use project ci`", "added `use fork ci`"],
            id="from-project-ci",
        ),
        pytest.param((), [], [["use fork ci"]], ["added `use fork ci`"], id="unlabelled"),
        pytest.param(("use fork ci",), [], [], [], id="already-on-fork-ci"),
    ],
)
def test_move_to_fork_ci(labels, removed, added, done):
    client = _client(labels=labels)

    assert move_to_fork_ci(client, 42) == done
    assert [c.args[1] for c in client.remove_label.call_args_list] == removed
    assert [c.args[1] for c in client.add_labels.call_args_list] == added
    client.set_status.assert_not_called()


@pytest.mark.parametrize("move", [move_to_project_ci, move_to_fork_ci])
def test_moving_a_closed_pull_request_fails(move):
    client = _client(state="closed")

    with pytest.raises(ValueError, match="not open"):
        move(client, 42)

    client.add_labels.assert_not_called()


@pytest.mark.parametrize(
    ("command", "args", "target", "kwargs"),
    [
        pytest.param(use_project_ci, ["42"], "move_to_project_ci", {"restart": True}, id="use-project-ci"),
        pytest.param(
            use_project_ci, ["42", "--no-restart"], "move_to_project_ci", {"restart": False}, id="no-restart"
        ),
        pytest.param(use_fork_ci, ["42"], "move_to_fork_ci", {}, id="use-fork-ci"),
    ],
)
def test_commands_move_the_pull_request(command, args, target, kwargs):
    with mock.patch(f"airflow_breeze.utils.fork_ci_pr_move.{target}", autospec=True) as move:
        move.return_value = ["added a label"]
        result = CliRunner().invoke(command, args)

    assert result.exit_code == 0, result.output
    assert move.call_args.args[1] == 42
    assert move.call_args.kwargs == kwargs


@pytest.mark.parametrize("error", [GitHubError("gh api failed: HTTP 404"), ValueError("not open")])
def test_commands_fail_on_errors(error):
    with mock.patch("airflow_breeze.utils.fork_ci_pr_move.move_to_fork_ci", autospec=True) as move:
        move.side_effect = error
        result = CliRunner().invoke(use_fork_ci, ["42"])

    assert result.exit_code == 1
