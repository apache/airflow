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
import socket
import stat
import subprocess
from unittest import mock

import psutil
import pytest

from airflow_breeze.global_constants import SIMPLE_AUTH_MANAGER_VITE_DEV_PORT, VITE_DEV_PORT
from airflow_breeze.utils.run_utils import (
    _find_local_port_listeners,
    _find_occupied_local_ports,
    _format_occupied_ports_error,
    change_directory_permission,
    change_file_permission,
    check_if_buildx_plugin_installed,
    run_command,
    run_compile_ui_assets,
)


def test_change_file_permission(tmp_path):
    tmpfile = tmp_path / "test.config"
    tmpfile.write_text("content")
    change_file_permission(tmpfile)
    mode = tmpfile.stat().st_mode
    assert not (mode & stat.S_IWGRP)
    assert not (mode & stat.S_IWOTH)


def test_change_directory_permission(tmp_path):
    subdir = tmp_path / "testdir"
    subdir.mkdir()
    change_directory_permission(subdir)
    mode = subdir.stat().st_mode
    assert not (mode & stat.S_IWGRP)
    assert not (mode & stat.S_IWOTH)
    assert mode & stat.S_IXGRP
    assert mode & stat.S_IXOTH


@mock.patch("airflow_breeze.utils.run_utils.subprocess.run")
def test_run_command_dry_run_quiet_does_not_execute(mock_subprocess_run):
    result = run_command(["echo", "hello"], dry_run_override=True, quiet=True)

    mock_subprocess_run.assert_not_called()
    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""


@pytest.mark.parametrize("verbose", [False, True])
@mock.patch("airflow_breeze.utils.run_utils.subprocess.run")
def test_run_command_forwards_timeout(mock_subprocess_run, verbose):
    mock_subprocess_run.side_effect = subprocess.TimeoutExpired(["echo", "hello"], 30)

    with pytest.raises(subprocess.TimeoutExpired):
        run_command(["echo", "hello"], timeout=30, verbose_override=verbose)

    assert mock_subprocess_run.call_args.kwargs["timeout"] == 30


def test_find_occupied_local_ports():
    with socket.socket() as unused_socket:
        unused_socket.bind(("127.0.0.1", 0))
        unoccupied_port = str(unused_socket.getsockname()[1])

    with socket.create_server(("127.0.0.1", 0)) as server:
        occupied_port = str(server.getsockname()[1])

        assert _find_occupied_local_ports((unoccupied_port, occupied_port)) == [occupied_port]


@mock.patch("airflow_breeze.utils.run_utils._run_compile_internally")
@mock.patch("airflow_breeze.utils.run_utils._find_occupied_local_ports", return_value=[])
def test_run_compile_ui_assets_checks_both_dev_ports(mock_find_occupied_ports, mock_run_compile):
    result = run_compile_ui_assets(
        dev=True, run_in_background=False, force_clean=False, additional_ui_hooks=[]
    )

    assert result == mock_run_compile.return_value
    mock_find_occupied_ports.assert_called_once_with((VITE_DEV_PORT, SIMPLE_AUTH_MANAGER_VITE_DEV_PORT))


@pytest.mark.parametrize(
    "occupied_ports",
    [
        pytest.param([VITE_DEV_PORT], id="airflow-ui"),
        pytest.param([SIMPLE_AUTH_MANAGER_VITE_DEV_PORT], id="simple-auth-manager-ui"),
    ],
)
@mock.patch("airflow_breeze.utils.run_utils._clean_ui_assets")
@mock.patch("airflow_breeze.utils.run_utils._find_local_port_listeners", return_value={})
@mock.patch("airflow_breeze.utils.run_utils._find_occupied_local_ports")
@mock.patch("airflow_breeze.utils.run_utils.console_print")
def test_run_compile_ui_assets_exits_before_cleanup_when_dev_port_is_occupied(
    mock_console_print, mock_find_occupied_ports, mock_find_listeners, mock_clean_ui_assets, occupied_ports
):
    mock_find_occupied_ports.return_value = occupied_ports

    with pytest.raises(SystemExit) as ctx:
        run_compile_ui_assets(dev=True, run_in_background=False, force_clean=True, additional_ui_hooks=[])

    assert ctx.value.code == 1
    mock_find_listeners.assert_called_once_with(occupied_ports)
    mock_console_print.assert_called_once_with(
        "[error]Cannot start UI development servers because the following local port(s) "
        f"are already in use: {', '.join(occupied_ports)}.[/]\n"
        "[info]Stop the processes using these ports and try again.[/]"
    )
    mock_clean_ui_assets.assert_not_called()


def test_find_local_port_listeners_reports_current_process():
    with socket.create_server(("127.0.0.1", 0)) as server:
        occupied_port = str(server.getsockname()[1])

        listeners = _find_local_port_listeners([occupied_port])

    assert list(listeners) == [occupied_port]
    assert listeners[occupied_port] == (os.getpid(), " ".join(psutil.Process().cmdline()))


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(psutil.AccessDenied(pid=1), id="access-denied"),
        pytest.param(psutil.NoSuchProcess(pid=1), id="no-such-process"),
    ],
)
@mock.patch("psutil.process_iter")
def test_find_local_port_listeners_skips_processes_that_cannot_be_inspected(mock_process_iter, error):
    failing_process = mock.Mock(spec=psutil.Process)
    failing_process.net_connections.side_effect = error
    listening_process = mock.Mock(spec=psutil.Process, pid=42)
    listening_process.net_connections.return_value = [
        mock.Mock(status=psutil.CONN_LISTEN, laddr=mock.Mock(port=5173)),
    ]
    listening_process.cmdline.return_value = ["node", "vite.js"]
    mock_process_iter.return_value = [failing_process, listening_process]

    assert _find_local_port_listeners(["5173"]) == {"5173": (42, "node vite.js")}


@pytest.mark.parametrize(
    ("listeners", "expected_lines"),
    [
        pytest.param(
            {},
            ["[info]Stop the processes using these ports and try again.[/]"],
            id="no-listener-found",
        ),
        pytest.param(
            {"5173": (42, "node vite.js --port 5173 [x]")},
            [
                "[info]Port 5173 is used by PID 42: node vite.js --port 5173 \\[x][/]",
                "[info]Stop the processes using these ports (for example `kill 42`) and try again.[/]",
            ],
            id="one-listener",
        ),
        pytest.param(
            {"5173": (42, "node"), "5174": (43, "node")},
            [
                "[info]Port 5173 is used by PID 42: node[/]",
                "[info]Port 5174 is used by PID 43: node[/]",
                "[info]Stop the processes using these ports (for example `kill 42 43`) and try again.[/]",
            ],
            id="two-listeners",
        ),
        pytest.param(
            {"5174": (42, "node"), "5173": (42, "node")},
            [
                "[info]Port 5173 is used by PID 42: node[/]",
                "[info]Port 5174 is used by PID 42: node[/]",
                "[info]Stop the processes using these ports (for example `kill 42`) and try again.[/]",
            ],
            id="same-process-on-both-ports",
        ),
    ],
)
def test_format_occupied_ports_error(listeners, expected_lines):
    message = _format_occupied_ports_error(["5173", "5174"], listeners)

    assert message.split("\n") == [
        "[error]Cannot start UI development servers because the following local port(s) "
        "are already in use: 5173, 5174.[/]",
        *expected_lines,
    ]


@mock.patch("airflow_breeze.utils.run_utils._run_compile_internally")
@mock.patch("airflow_breeze.utils.run_utils._find_occupied_local_ports")
def test_run_compile_ui_assets_does_not_check_dev_ports_for_static_build(
    mock_find_occupied_ports, mock_run_compile
):
    result = run_compile_ui_assets(
        dev=False, run_in_background=False, force_clean=False, additional_ui_hooks=[]
    )

    assert result == mock_run_compile.return_value
    mock_find_occupied_ports.assert_not_called()


@mock.patch("airflow_breeze.utils.run_utils.run_command")
@mock.patch("airflow_breeze.utils.run_utils.console_print")
def test_check_buildah_is_installed(mock_console_print, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "buildah 1.33.7"
    assert check_if_buildx_plugin_installed() is False
    mock_run_command.assert_called_with(
        ["docker", "buildx", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_console_print.assert_called_with(
        "[warning]Detected buildah installation.[/]\n"
        "[warning]The Dockerfiles are only compatible with BuildKit.[/]\n"
        "[warning]Please see the syntax declaration at the top of the Dockerfiles for BuildKit version\n"
    )


@mock.patch("airflow_breeze.utils.run_utils.run_command")
@mock.patch("airflow_breeze.utils.run_utils.console_print")
def test_check_buildkit_is_installed(mock_console_print, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "github.com/docker/buildx v0.29.1-desktop.1"
    assert check_if_buildx_plugin_installed() is True
    mock_run_command.assert_called_with(
        ["docker", "buildx", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_console_print.assert_called_with(
        "[success]Docker BuildKit is installed and will be used for the image build.[/]\n"
    )


@mock.patch("airflow_breeze.utils.run_utils.run_command")
@mock.patch("airflow_breeze.utils.run_utils.console_print")
def test_check_buildx_not_detected(mock_console_print, mock_run_command):
    mock_run_command.return_value.returncode = 1
    assert check_if_buildx_plugin_installed() is False
    mock_run_command.assert_called_with(
        ["docker", "buildx", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_console_print.assert_not_called()
