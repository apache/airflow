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
from unittest.mock import call

import pytest

from airflow_breeze.utils.docker_command_utils import (
    check_docker_compose_version,
    check_docker_context,
    check_docker_version,
)


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_unknown(
    mock_get_console, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    check_docker_version(verbose=True)
    expected_run_command_calls = [
        call(
            ["docker", "version", "--format", "{{.Client.Version}}"],
            verbose=True,
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            check=False,
        ),
    ]
    mock_run_command.assert_has_calls(expected_run_command_calls)
    mock_get_console.return_value.print.assert_called_with(
        """
[warning]Your version of docker is unknown. If the scripts fail, please make sure to[/]
[warning]install docker at least: 20.10.0 version.[/]
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_too_low(
    mock_get_console, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "0.9"
    check_docker_version(verbose=True)
    mock_check_docker_permission_denied.assert_called_with(True)
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_get_console.return_value.print.assert_called_with(
        """
[warning]Your version of docker is too old:0.9.\nPlease upgrade to at least 20.10.0[/]
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_ok(mock_get_console, mock_run_command, mock_check_docker_permission_denied):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "20.10.0"
    check_docker_version(verbose=True)
    mock_check_docker_permission_denied.assert_called_with(True)
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_get_console.return_value.print.assert_called_with("[success]Good version of Docker: 20.10.0.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_higher(mock_get_console, mock_run_command, mock_check_docker_permission_denied):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "21.10.0"
    check_docker_version(verbose=True)
    mock_check_docker_permission_denied.assert_called_with(True)
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_get_console.return_value.print.assert_called_with("[success]Good version of Docker: 21.10.0.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_compose_version_unknown(mock_get_console, mock_run_command):
    check_docker_compose_version(verbose=True)
    expected_run_command_calls = [
        call(
            ["docker-compose", "--version"],
            verbose=True,
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
        ),
    ]
    mock_run_command.assert_has_calls(expected_run_command_calls)
    mock_get_console.return_value.print.assert_called_with(
        """
[warning]Unknown docker-compose version. At least 1.29 is needed![/]
[warning]If Breeze fails upgrade to latest available docker-compose version.[/]
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_compose_version_low(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "1.28.5"
    check_docker_compose_version(verbose=True)
    mock_run_command.assert_called_with(
        ["docker-compose", "--version"],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
    )
    expected_print_calls = [
        call(
            """
[warning]You have too old version of docker-compose: 1.28.5! At least 1.29 needed! Please upgrade!
"""
        ),
        call(
            """
See https://docs.docker.com/compose/install/ for instructions.
Make sure docker-compose you install is first on the PATH variable of yours.
"""
        ),
    ]
    mock_get_console.return_value.print.assert_has_calls(expected_print_calls)


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_compose_version_ok(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "1.29.0"
    check_docker_compose_version(verbose=True)
    mock_run_command.assert_called_with(
        ["docker-compose", "--version"],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
    )
    mock_get_console.return_value.print.assert_called_with(
        "[success]Good version of docker-compose: 1.29.0[/]"
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_compose_version_higher(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "1.29.2"
    check_docker_compose_version(verbose=True)
    mock_run_command.assert_called_with(
        ["docker-compose", "--version"],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
    )
    mock_get_console.return_value.print.assert_called_with(
        "[success]Good version of docker-compose: 1.29.2[/]"
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_context_default(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "default"
    check_docker_context(verbose=True)
    mock_run_command.assert_called_with(
        ["docker", "info", "--format", "{{json .ClientInfo.Context}}"],
        verbose=True,
        no_output_dump_on_exception=False,
        text=True,
        capture_output=True,
    )
    mock_get_console.return_value.print.assert_called_with("[success]Good Docker context used: default.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_context_other(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "other"
    with pytest.raises(SystemExit):
        check_docker_context(verbose=True)
    mock_run_command.assert_called_with(
        ["docker", "info", "--format", "{{json .ClientInfo.Context}}"],
        verbose=True,
        no_output_dump_on_exception=False,
        text=True,
        capture_output=True,
    )
    mock_get_console.return_value.print.assert_called_with(
        "[error]Docker is not using the default context, used context is: other[/]\n"
        "[warning]Please make sure Docker is using the default context.[/]\n"
        '[warning]You can try switching contexts by running: "docker context use default"[/]'
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_context_command_failed(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 1
    check_docker_context(verbose=True)
    mock_run_command.assert_called_with(
        ["docker", "info", "--format", "{{json .ClientInfo.Context}}"],
        verbose=True,
        no_output_dump_on_exception=False,
        text=True,
        capture_output=True,
    )
    mock_get_console.return_value.print.assert_called_with(
        "[warning]Could not check for Docker context.[/]\n"
        '[warning]Please make sure that Docker is using the right context by running "docker info" and '
        "checking the active Context.[/]"
    )
