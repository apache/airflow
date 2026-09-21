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

from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.commands.ci_commands import remove_backport_labels

MODULE = "airflow_breeze.commands.ci_commands"


def _result(returncode: int = 0, stdout: str = "") -> MagicMock:
    return MagicMock(returncode=returncode, stdout=stdout)


def _call(run_command, **kwargs):
    with patch(f"{MODULE}.run_command", run_command):
        remove_backport_labels(branch_name="ci-upgrade-main", command_env={}, **kwargs)


def _edit_call(run_command):
    """Return the argv of the `gh pr edit` invocation, or None when it never ran."""
    for call in run_command.call_args_list:
        argv = call.args[0]
        if "edit" in argv:
            return argv
    return None


@pytest.mark.parametrize(
    "labels",
    ["backport-to-v3-3-test", "backport-to-v3-3-test,backport-to-airflow-ctl/v0-1-test"],
)
def test_every_backport_label_is_removed_in_one_call(labels):
    run_command = MagicMock(side_effect=[_result(stdout=labels), _result()])

    _call(run_command)

    argv = _edit_call(run_command)
    assert argv is not None
    removed = {argv[index + 1] for index, arg in enumerate(argv) if arg == "--remove-label"}
    assert removed == set(labels.split(","))
    assert run_command.call_count == 2


def test_nothing_is_edited_when_no_backport_label_is_present():
    run_command = MagicMock(side_effect=[_result(stdout="")])

    _call(run_command)

    assert _edit_call(run_command) is None


def test_labels_are_left_alone_when_they_cannot_be_read():
    """Without the label list we cannot tell which to remove - never guess and edit blindly."""
    run_command = MagicMock(side_effect=[_result(returncode=1)])

    _call(run_command)

    assert _edit_call(run_command) is None


def test_a_failed_edit_does_not_raise():
    """The upgrade PR itself is already pushed; a label left behind must not fail the run."""
    run_command = MagicMock(side_effect=[_result(stdout="backport-to-v3-3-test"), _result(returncode=1)])

    _call(run_command)

    assert _edit_call(run_command) is not None
