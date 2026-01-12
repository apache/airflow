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
from unittest.mock import call

import pytest

from airflow_breeze.utils.docker_command_utils import (
    autodetect_docker_context,
    check_docker_compose_version,
    check_docker_version,
)


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_unknown(
    mock_get_console, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    with pytest.raises(SystemExit) as e:
        check_docker_version()
    assert e.value.code == 1
    expected_run_command_calls = [
        call(
            ["docker", "version", "--format", "{{.Client.Version}}"],
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            check=False,
            dry_run_override=False,
        ),
    ]
    mock_run_command.assert_has_calls(expected_run_command_calls)
    mock_get_console.return_value.print.assert_called_with(
        """
[warning]Your version of docker is unknown. If the scripts fail, please make sure to[/]
[warning]install docker at least: 25.0.0 version.[/]
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
    with pytest.raises(SystemExit) as e:
        check_docker_version()
    assert e.value.code == 1
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_get_console.return_value.print.assert_called_with(
        """
[error]Your version of docker is too old: 0.9.\n[/]\n[warning]Please upgrade to at least 25.0.0.\n[/]\n\
You can find installation instructions here: https://docs.docker.com/engine/install/
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_ok(mock_get_console, mock_run_command, mock_check_docker_permission_denied):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "25.0.0"
    check_docker_version()
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_get_console.return_value.print.assert_called_with("[success]Good version of Docker: 25.0.0.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_higher(mock_get_console, mock_run_command, mock_check_docker_permission_denied):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "25.0.0"
    check_docker_version()
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_get_console.return_value.print.assert_called_with("[success]Good version of Docker: 25.0.0.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_version_higher_rancher_desktop(
    mock_get_console, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "25.0.0-rd"
    check_docker_version()
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_get_console.return_value.print.assert_called_with("[success]Good version of Docker: 25.0.0-r.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_compose_version_unknown(mock_get_console, mock_run_command):
    with pytest.raises(SystemExit) as e:
        check_docker_compose_version()
    assert e.value.code == 1
    expected_run_command_calls = [
        call(
            ["docker", "compose", "version"],
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            dry_run_override=False,
        ),
    ]
    mock_run_command.assert_has_calls(expected_run_command_calls)
    mock_get_console.return_value.print.assert_called_with(
        """
[error]Unknown docker-compose version.[/]\n[warning]At least 2.20.2 needed! Please upgrade!\n[/]
See https://docs.docker.com/compose/install/ for installation instructions.\n
Make sure docker-compose you install is first on the PATH variable of yours.\n
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_compose_version_low(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "1.28.5"
    with pytest.raises(SystemExit) as e:
        check_docker_compose_version()
    assert e.value.code == 1
    mock_run_command.assert_called_with(
        ["docker", "compose", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    mock_get_console.return_value.print.assert_called_with(
        """
[error]You have too old version of docker-compose: 1.28.5!\n[/]
[warning]At least 2.20.2 needed! Please upgrade!\n[/]
See https://docs.docker.com/compose/install/ for installation instructions.\n
Make sure docker-compose you install is first on the PATH variable of yours.\n
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_console")
def test_check_docker_compose_version_ok(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "2.20.2"
    check_docker_compose_version()
    mock_run_command.assert_called_with(
        ["docker", "compose", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    mock_get_console.return_value.print.assert_called_with(
        "[success]Good version of docker-compose: 2.20.2[/]"
    )


def _fake_ctx_output(*names: str) -> str:
    return "\n".join(json.dumps({"Name": name, "DockerEndpoint": f"unix://{name}"}) for name in names)


@pytest.mark.parametrize(
    ("context_output", "selected_context", "console_output"),
    [
        (
            _fake_ctx_output("default"),
            "default",
            "[info]Using 'default' as context",
        ),
        ("\n", "default", "[warning]Could not detect docker builder"),
        (
            _fake_ctx_output("a", "b"),
            "a",
            "[warning]Could not use any of the preferred docker contexts",
        ),
        (
            _fake_ctx_output("a", "desktop-linux"),
            "desktop-linux",
            "[info]Using 'desktop-linux' as context",
        ),
        (
            _fake_ctx_output("a", "default"),
            "default",
            "[info]Using 'default' as context",
        ),
        (
            _fake_ctx_output("a", "default", "desktop-linux"),
            "desktop-linux",
            "[info]Using 'desktop-linux' as context",
        ),
        (
            '[{"Name": "desktop-linux", "DockerEndpoint": "unix://desktop-linux"}]',
            "desktop-linux",
            "[info]Using 'desktop-linux' as context",
        ),
    ],
)
def test_autodetect_docker_context(context_output: str, selected_context: str, console_output: str):
    with mock.patch("airflow_breeze.utils.docker_command_utils.run_command") as mock_run_command:
        mock_run_command.return_value.returncode = 0
        mock_run_command.return_value.stdout = context_output
        with mock.patch("airflow_breeze.utils.docker_command_utils.get_console") as mock_get_console:
            mock_get_console.return_value.input.return_value = selected_context
            assert autodetect_docker_context() == selected_context
            mock_get_console.return_value.print.assert_called_once()
            assert console_output in mock_get_console.return_value.print.call_args[0][0]


SOCKET_INFO = json.dumps(
    [
        {
            "Name": "default",
            "Metadata": {},
            "Endpoints": {"docker": {"Host": "unix:///not-standard/docker.sock", "SkipTLSVerify": False}},
            "TLSMaterial": {},
            "Storage": {"MetadataPath": "\u003cIN MEMORY\u003e", "TLSPath": "\u003cIN MEMORY\u003e"},
        }
    ]
)

SOCKET_INFO_DESKTOP_LINUX = json.dumps(
    [
        {
            "Name": "desktop-linux",
            "Metadata": {},
            "Endpoints": {
                "docker": {"Host": "unix:///VERY_NON_STANDARD/docker.sock", "SkipTLSVerify": False}
            },
            "TLSMaterial": {},
            "Storage": {"MetadataPath": "\u003cIN MEMORY\u003e", "TLSPath": "\u003cIN MEMORY\u003e"},
        }
    ]
)
