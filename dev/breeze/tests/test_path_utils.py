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

import os
import subprocess
from unittest import mock

from airflow_breeze.utils.path_utils import reinstall_if_setup_changed


@mock.patch.dict(os.environ, {"BREEZE_SELF_UPGRADE_TIMEOUT": "3"})
@mock.patch("airflow_breeze.utils.path_utils.inform_about_self_upgrade")
@mock.patch("airflow_breeze.utils.path_utils.subprocess.run")
def test_reinstall_if_setup_changed_returns_true_when_modified(mock_subprocess_run, mock_inform):
    completed = subprocess.CompletedProcess(
        args=["uv", "tool", "upgrade", "..."],
        returncode=0,
        stdout="Updated successfully",
        stderr="... Modified ...",
    )
    mock_subprocess_run.return_value = completed

    result = reinstall_if_setup_changed()

    assert result is True
    mock_inform.assert_called_once()
    mock_subprocess_run.assert_called_once_with(
        ["uv", "tool", "upgrade", "apache-airflow-breeze"],
        cwd=mock.ANY,
        check=True,
        text=True,
        capture_output=True,
        timeout=3,
    )


@mock.patch.dict(os.environ, {"BREEZE_SELF_UPGRADE_TIMEOUT": "1"})
@mock.patch("airflow_breeze.utils.path_utils.get_console")
@mock.patch("airflow_breeze.utils.path_utils.subprocess.run")
def test_reinstall_if_setup_changed_timeout(mock_subprocess_run, mock_get_console):
    """When subprocess.run times out, the function should return False and print a warning."""
    mock_subprocess_run.side_effect = subprocess.TimeoutExpired(cmd="uv", timeout=1)
    mock_console = mock.MagicMock()
    mock_get_console.return_value = mock_console

    res = reinstall_if_setup_changed()

    assert res is False
    assert mock_console.print.called


@mock.patch.dict(os.environ, {"BREEZE_SELF_UPGRADE_TIMEOUT": "2"})
@mock.patch("airflow_breeze.utils.path_utils.get_console")
@mock.patch("airflow_breeze.utils.path_utils.get_verbose")
@mock.patch("airflow_breeze.utils.path_utils.subprocess.run")
def test_reinstall_if_setup_changed_calledprocesserror_verbose(
    mock_subprocess_run, mock_get_verbose, mock_get_console
):
    """When subprocess.run raises CalledProcessError and verbose is True, we print stderr and return False."""
    err = subprocess.CalledProcessError(returncode=1, cmd="uv", stderr="some error text")
    mock_subprocess_run.side_effect = err
    mock_get_verbose.return_value = True
    mock_console = mock.MagicMock()
    mock_get_console.return_value = mock_console

    res = reinstall_if_setup_changed()

    assert res is False
    # In verbose mode the function should print at least once.
    assert mock_console.print.called
