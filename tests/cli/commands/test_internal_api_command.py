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

import psutil
import pytest
from rich.console import Console

from airflow import settings
from airflow.cli import cli_parser
from airflow.cli.commands import internal_api_command
from airflow.cli.commands.internal_api_command import GunicornMonitor
from airflow.settings import _ENABLE_AIP_44
from tests_common.test_utils.config import conf_vars

from tests.cli.commands._common_cli_classes import _CommonCLIGunicornTestClass

console = Console(width=400, color_system="standard")


class TestCLIGetNumReadyWorkersRunning:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def setup_method(self):
        self.children = mock.MagicMock()
        self.child = mock.MagicMock()
        self.process = mock.MagicMock()
        self.monitor = GunicornMonitor(
            gunicorn_master_pid=1,
            num_workers_expected=4,
            master_timeout=60,
            worker_refresh_interval=60,
            worker_refresh_batch_size=2,
            reload_on_plugin_change=True,
        )

    def test_ready_prefix_on_cmdline(self):
        self.child.cmdline.return_value = [settings.GUNICORN_WORKER_READY_PREFIX]
        self.process.children.return_value = [self.child]

        with mock.patch("psutil.Process", return_value=self.process):
            assert self.monitor._get_num_ready_workers_running() == 1

    def test_ready_prefix_on_cmdline_no_children(self):
        self.process.children.return_value = []

        with mock.patch("psutil.Process", return_value=self.process):
            assert self.monitor._get_num_ready_workers_running() == 0

    def test_ready_prefix_on_cmdline_zombie(self):
        self.child.cmdline.return_value = []
        self.process.children.return_value = [self.child]

        with mock.patch("psutil.Process", return_value=self.process):
            assert self.monitor._get_num_ready_workers_running() == 0

    def test_ready_prefix_on_cmdline_dead_process(self):
        self.child.cmdline.side_effect = psutil.NoSuchProcess(11347)
        self.process.children.return_value = [self.child]

        with mock.patch("psutil.Process", return_value=self.process):
            assert self.monitor._get_num_ready_workers_running() == 0


@pytest.mark.db_test
@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
class TestCliInternalAPI(_CommonCLIGunicornTestClass):
    main_process_regexp = r"airflow internal-api"

    @pytest.mark.execution_timeout(210)
    def test_cli_internal_api_background(self, tmp_path):
        parent_path = tmp_path / "gunicorn"
        parent_path.mkdir()
        pidfile_internal_api = parent_path / "pidflow-internal-api.pid"
        pidfile_monitor = parent_path / "pidflow-internal-api-monitor.pid"
        stdout = parent_path / "airflow-internal-api.out"
        stderr = parent_path / "airflow-internal-api.err"
        logfile = parent_path / "airflow-internal-api.log"
        try:
            # Run internal-api as daemon in background. Note that the wait method is not called.
            console.print("[magenta]Starting airflow internal-api --daemon")
            env = os.environ.copy()
            env["AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION"] = "true"
            proc = subprocess.Popen(
                [
                    "airflow",
                    "internal-api",
                    "--daemon",
                    "--pid",
                    os.fspath(pidfile_internal_api),
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
            pid_internal_api = self._wait_pidfile(pidfile_internal_api)
            console.print(f"[blue]Internal API started at {pid_internal_api}")
            console.print("[blue]Running airflow internal-api process:")
            # Assert that the internal-api and gunicorn processes are running (by name rather than pid).
            assert self._find_process(r"airflow internal-api --daemon", print_found_process=True)
            console.print("[blue]Waiting for gunicorn processes:")
            # wait for gunicorn to start
            for _ in range(30):
                if self._find_process(r"^gunicorn"):
                    break
                console.print("[blue]Waiting for gunicorn to start ...")
                time.sleep(1)
            console.print("[blue]Running gunicorn processes:")
            assert self._find_all_processes("^gunicorn", print_found_process=True)
            console.print("[magenta]Internal-api process started successfully.")
            console.print(
                "[magenta]Terminating monitor process and expect "
                "internal-api and gunicorn processes to terminate as well"
            )
            self._terminate_multiple_process([pid_internal_api, pid_monitor])
            self._check_processes(ignore_running=False)
            console.print("[magenta]All internal-api and gunicorn processes are terminated.")
        except Exception:
            console.print("[red]Exception occurred. Dumping all logs.")
            # Dump all logs
            for file in parent_path.glob("*"):
                console.print(f"Dumping {file} (size: {file.stat().st_size})")
                console.print(file.read_text())
            raise

    @conf_vars({("core", "database_access_isolation"): "true"})
    def test_cli_internal_api_debug(self, app):
        with mock.patch(
            "airflow.cli.commands.internal_api_command.create_app", return_value=app
        ), mock.patch.object(app, "run") as app_run:
            args = self.parser.parse_args(
                [
                    "internal-api",
                    "--debug",
                ]
            )
            internal_api_command.internal_api(args)

            app_run.assert_called_with(
                debug=True,
                use_reloader=False,
                port=9080,
                host="0.0.0.0",
            )

    @conf_vars({("core", "database_access_isolation"): "true"})
    def test_cli_internal_api_args(self):
        with mock.patch("subprocess.Popen") as Popen, mock.patch.object(
            internal_api_command, "GunicornMonitor"
        ):
            args = self.parser.parse_args(
                [
                    "internal-api",
                    "--access-logformat",
                    "custom_log_format",
                    "--pid",
                    "/tmp/x.pid",
                ]
            )
            internal_api_command.internal_api(args)

            Popen.assert_called_with(
                [
                    sys.executable,
                    "-m",
                    "gunicorn",
                    "--workers",
                    "4",
                    "--worker-class",
                    "sync",
                    "--timeout",
                    "120",
                    "--bind",
                    "0.0.0.0:9080",
                    "--name",
                    "airflow-internal-api",
                    "--pid",
                    "/tmp/x.pid",
                    "--access-logfile",
                    "-",
                    "--error-logfile",
                    "-",
                    "--config",
                    "python:airflow.api_internal.gunicorn_config",
                    "--access-logformat",
                    "custom_log_format",
                    "airflow.cli.commands.internal_api_command:cached_app()",
                    "--preload",
                ],
                close_fds=True,
            )
