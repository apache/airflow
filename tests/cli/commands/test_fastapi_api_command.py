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
import sys
import time
from unittest import mock

import pytest
from rich.console import Console

from airflow.cli.commands import fastapi_api_command
from airflow.exceptions import AirflowConfigException
from tests.cli.commands._common_cli_classes import _CommonCLIGunicornTestClass

console = Console(width=400, color_system="standard")


@pytest.mark.db_test
class TestCliFastAPI(_CommonCLIGunicornTestClass):
    main_process_regexp = r"airflow fastapi-api"

    @pytest.mark.execution_timeout(210)
    def test_cli_fastapi_api_background(self, tmp_path):
        parent_path = tmp_path / "gunicorn"
        parent_path.mkdir()
        pidfile_fastapi_api = parent_path / "pidflow-fastapi-api.pid"
        pidfile_monitor = parent_path / "pidflow-fastapi-api-monitor.pid"
        stdout = parent_path / "airflow-fastapi-api.out"
        stderr = parent_path / "airflow-fastapi-api.err"
        logfile = parent_path / "airflow-fastapi-api.log"
        try:
            # Run fastapi-api as daemon in background. Note that the wait method is not called.
            console.print("[magenta]Starting airflow fastapi-api --daemon")
            env = os.environ.copy()
            proc = subprocess.Popen(
                [
                    "airflow",
                    "fastapi-api",
                    "--daemon",
                    "--pid",
                    os.fspath(pidfile_fastapi_api),
                    "--stdout",
                    os.fspath(stdout),
                    "--stderr",
                    os.fspath(stderr),
                    "--log-file",
                    os.fspath(logfile),
                ],
                env=env,
            )
            assert proc.poll() is None

            pid_monitor = self._wait_pidfile(pidfile_monitor)
            console.print(f"[blue]Monitor started at {pid_monitor}")
            pid_fastapi_api = self._wait_pidfile(pidfile_fastapi_api)
            console.print(f"[blue]FastAPI API started at {pid_fastapi_api}")
            console.print("[blue]Running airflow fastapi-api process:")
            # Assert that the fastapi-api and gunicorn processes are running (by name rather than pid).
            assert self._find_process(r"airflow fastapi-api --daemon", print_found_process=True)
            console.print("[blue]Waiting for gunicorn processes:")
            # wait for gunicorn to start
            for _ in range(30):
                if self._find_process(r"^gunicorn"):
                    break
                console.print("[blue]Waiting for gunicorn to start ...")
                time.sleep(1)
            console.print("[blue]Running gunicorn processes:")
            assert self._find_all_processes("^gunicorn", print_found_process=True)
            console.print("[magenta]fastapi-api process started successfully.")
            console.print(
                "[magenta]Terminating monitor process and expect "
                "fastapi-api and gunicorn processes to terminate as well"
            )
            self._terminate_multiple_process([pid_fastapi_api, pid_monitor])
            self._check_processes(ignore_running=False)
            console.print("[magenta]All fastapi-api and gunicorn processes are terminated.")
        except Exception:
            console.print("[red]Exception occurred. Dumping all logs.")
            # Dump all logs
            for file in parent_path.glob("*"):
                console.print(f"Dumping {file} (size: {file.stat().st_size})")
                console.print(file.read_text())
            raise

    def test_cli_fastapi_api_debug(self, app):
        with mock.patch("subprocess.Popen") as Popen, mock.patch.object(
            fastapi_api_command, "GunicornMonitor"
        ):
            port = "9092"
            hostname = "somehost"
            args = self.parser.parse_args(["fastapi-api", "--port", port, "--hostname", hostname, "--debug"])
            fastapi_api_command.fastapi_api(args)

            Popen.assert_called_with(
                [
                    "fastapi",
                    "dev",
                    "airflow/api_fastapi/main.py",
                    "--port",
                    port,
                    "--host",
                    hostname,
                ],
                close_fds=True,
            )

    def test_cli_fastapi_api_args(self, ssl_cert_and_key):
        cert_path, key_path = ssl_cert_and_key

        with mock.patch("subprocess.Popen") as Popen, mock.patch.object(
            fastapi_api_command, "GunicornMonitor"
        ):
            args = self.parser.parse_args(
                [
                    "fastapi-api",
                    "--access-logformat",
                    "custom_log_format",
                    "--pid",
                    "/tmp/x.pid",
                    "--ssl-cert",
                    str(cert_path),
                    "--ssl-key",
                    str(key_path),
                ]
            )
            fastapi_api_command.fastapi_api(args)

            Popen.assert_called_with(
                [
                    sys.executable,
                    "-m",
                    "gunicorn",
                    "--workers",
                    "4",
                    "--worker-class",
                    "airflow.cli.commands.fastapi_api_command.AirflowUvicornWorker",
                    "--timeout",
                    "120",
                    "--bind",
                    "0.0.0.0:9091",
                    "--name",
                    "airflow-fastapi-api",
                    "--pid",
                    "/tmp/x.pid",
                    "--access-logfile",
                    "-",
                    "--error-logfile",
                    "-",
                    "--config",
                    "python:airflow.api_fastapi.gunicorn_config",
                    "--certfile",
                    str(cert_path),
                    "--keyfile",
                    str(key_path),
                    "--access-logformat",
                    "custom_log_format",
                    "airflow.api_fastapi.app:cached_app()",
                    "--preload",
                ],
                close_fds=True,
            )

    @pytest.mark.parametrize(
        "ssl_arguments, error_pattern",
        [
            (["--ssl-cert", "_.crt", "--ssl-key", "_.key"], "does not exist _.crt"),
            (["--ssl-cert", "_.crt"], "Need both.*certificate.*key"),
            (["--ssl-key", "_.key"], "Need both.*key.*certificate"),
        ],
    )
    def test_get_ssl_cert_and_key_filepaths_with_incorrect_usage(self, ssl_arguments, error_pattern):
        args = self.parser.parse_args(["fastapi-api"] + ssl_arguments)
        with pytest.raises(AirflowConfigException, match=error_pattern):
            fastapi_api_command._get_ssl_cert_and_key_filepaths(args)

    def test_get_ssl_cert_and_key_filepaths_with_correct_usage(self, ssl_cert_and_key):
        cert_path, key_path = ssl_cert_and_key

        args = self.parser.parse_args(
            ["fastapi-api"] + ["--ssl-cert", str(cert_path), "--ssl-key", str(key_path)]
        )
        assert fastapi_api_command._get_ssl_cert_and_key_filepaths(args) == (str(cert_path), str(key_path))

    @pytest.fixture
    def ssl_cert_and_key(self, tmp_path):
        cert_path, key_path = tmp_path / "_.crt", tmp_path / "_.key"
        cert_path.touch()
        key_path.touch()
        return cert_path, key_path
