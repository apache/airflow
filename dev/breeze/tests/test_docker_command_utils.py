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

from unittest import mock
from unittest.mock import call

from airflow_breeze.utils.docker_command_utils import check_docker_compose_version, check_docker_version


@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_version_unknown(mock_console, mock_run_command):
    check_docker_version(verbose=True)
    expected_run_command_calls = [
        call(
            ['docker', 'info'],
            verbose=True,
            check=True,
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
        ),
        call(
            ['docker', 'version', '--format', '{{.Client.Version}}'],
            verbose=True,
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
        ),
    ]

    mock_run_command.assert_has_calls(expected_run_command_calls)
    mock_console.print.assert_called_with(
        "Your version of docker is unknown. If the scripts fail, please make sure to"
        "                     install docker at least: 20.10.0 version."
    )


@mock.patch('airflow_breeze.utils.docker_command_utils.check_docker_permission')
@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_version_too_low(mock_console, mock_run_command, mock_check_docker_permission):
    mock_check_docker_permission.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "0.9"
    check_docker_version(verbose=True)
    mock_check_docker_permission.assert_called_with(True)
    mock_run_command.assert_called_with(
        ['docker', 'version', '--format', '{{.Client.Version}}'],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
    )
    mock_console.print.assert_called_with(
        "Your version of docker is too old:0.9. Please upgrade to                     at least 20.10.0"
    )


@mock.patch('airflow_breeze.utils.docker_command_utils.check_docker_permission')
@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_version_ok(mock_console, mock_run_command, mock_check_docker_permission):
    mock_check_docker_permission.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "20.10.0"
    check_docker_version(verbose=True)
    mock_check_docker_permission.assert_called_with(True)
    mock_run_command.assert_called_with(
        ['docker', 'version', '--format', '{{.Client.Version}}'],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
    )
    mock_console.print.assert_called_with("Good version of Docker: 20.10.0.")


@mock.patch('airflow_breeze.utils.docker_command_utils.check_docker_permission')
@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_version_higher(mock_console, mock_run_command, mock_check_docker_permission):
    mock_check_docker_permission.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "21.10.0"
    check_docker_version(verbose=True)
    mock_check_docker_permission.assert_called_with(True)
    mock_run_command.assert_called_with(
        ['docker', 'version', '--format', '{{.Client.Version}}'],
        verbose=True,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
    )
    mock_console.print.assert_called_with("Good version of Docker: 21.10.0.")


@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_compose_version_unknown(mock_console, mock_run_command):
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
    mock_console.print.assert_called_with(
        'Unknown docker-compose version. At least 1.29 is needed! \
        If Breeze fails upgrade to latest available docker-compose version'
    )


@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_compose_version_low(mock_console, mock_run_command):
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
            'You have too old version of docker-compose: 1.28.5! \
                At least 1.29 is needed! Please upgrade!'
        ),
        call(
            'See https://docs.docker.com/compose/install/ for instructions. \
                Make sure docker-compose you install is first on the PATH variable of yours.'
        ),
    ]
    mock_console.print.assert_has_calls(expected_print_calls)


@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_compose_version_ok(mock_console, mock_run_command):
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
    mock_console.print.assert_called_with("Good version of docker-compose: 1.29.0")


@mock.patch('airflow_breeze.utils.docker_command_utils.run_command')
@mock.patch('airflow_breeze.utils.docker_command_utils.console')
def test_check_docker_compose_version_higher(mock_console, mock_run_command):
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
    mock_console.print.assert_called_with("Good version of docker-compose: 1.29.2")
