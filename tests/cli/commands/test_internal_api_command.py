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
import tempfile
import time
from unittest import mock

import psutil
import pytest

from airflow import settings
from airflow.cli import cli_parser
from airflow.cli.commands import internal_api_command
from airflow.cli.commands.internal_api_command import GunicornMonitor
from airflow.utils.cli import setup_locations


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


class TestCliInternalAPi:
    @pytest.fixture(autouse=True)
    def _make_parser(self):
        self.parser = cli_parser.get_parser()

    @pytest.fixture(autouse=True)
    def _cleanup(self):
        self._check_processes()
        self._clean_pidfiles()

        yield

        self._check_processes(ignore_running=True)
        self._clean_pidfiles()

    def _check_processes(self, ignore_running=False):
        # Confirm that internal-api hasn't been launched.
        # pgrep returns exit status 1 if no process matched.
        # Use more specific regexps (^) to avoid matching pytest run when running specific method.
        # For instance, we want to be able to do: pytest -k 'gunicorn'
        exit_code_pgrep_internal_api = subprocess.Popen(["pgrep", "-c", "-f", "airflow internal-api"]).wait()
        exit_code_pgrep_gunicorn = subprocess.Popen(["pgrep", "-c", "-f", "^gunicorn"]).wait()
        if exit_code_pgrep_internal_api != 1 or exit_code_pgrep_gunicorn != 1:
            subprocess.Popen(["ps", "-ax"]).wait()
            if exit_code_pgrep_internal_api != 1:
                subprocess.Popen(["pkill", "-9", "-f", "airflow-internal-api"]).wait()
            if exit_code_pgrep_gunicorn != 1:
                subprocess.Popen(["pkill", "-9", "-f", "^gunicorn"]).wait()
            if not ignore_running:
                raise AssertionError(
                    "Background processes are running that prevent the test from passing successfully."
                )

    def _clean_pidfiles(self):
        pidfile_internal_api = setup_locations("internal-api")[0]
        pidfile_monitor = setup_locations("internal-api-monitor")[0]
        if os.path.exists(pidfile_internal_api):
            os.remove(pidfile_internal_api)
        if os.path.exists(pidfile_monitor):
            os.remove(pidfile_monitor)

    def _wait_pidfile(self, pidfile):
        start_time = time.monotonic()
        while True:
            try:
                with open(pidfile) as file:
                    return int(file.read())
            except Exception:
                if start_time - time.monotonic() > 60:
                    raise
                time.sleep(1)

    @pytest.mark.execution_timeout(210)
    def test_cli_internal_api_background(self):
        with tempfile.TemporaryDirectory(prefix="gunicorn") as tmpdir:
            pidfile_internal_api = f"{tmpdir}/pidflow-internal-api.pid"
            pidfile_monitor = f"{tmpdir}/pidflow-internal-api-monitor.pid"
            stdout = f"{tmpdir}/airflow-internal-api.out"
            stderr = f"{tmpdir}/airflow-internal-api.err"
            logfile = f"{tmpdir}/airflow-internal-api.log"
            try:
                # Run internal-api as daemon in background. Note that the wait method is not called.

                proc = subprocess.Popen(
                    [
                        "airflow",
                        "internal-api",
                        "--daemon",
                        "--pid",
                        pidfile_internal_api,
                        "--stdout",
                        stdout,
                        "--stderr",
                        stderr,
                        "--log-file",
                        logfile,
                    ]
                )
                assert proc.poll() is None

                pid_monitor = self._wait_pidfile(pidfile_monitor)
                self._wait_pidfile(pidfile_internal_api)

                # Assert that gunicorn and its monitor are launched.
                assert 0 == subprocess.Popen(["pgrep", "-f", "-c", "airflow internal-api --daemon"]).wait()
                # wait for gunicorn to start
                for i in range(30):
                    if 0 == subprocess.Popen(["pgrep", "-f", "-c", "^gunicorn"]).wait():
                        break
                    time.sleep(1)
                assert (
                    0 == subprocess.Popen(["pgrep", "-c", "-f", "gunicorn: master"]).wait()
                )  # NOT inclusive

                # Terminate monitor process.
                proc = psutil.Process(pid_monitor)
                proc.terminate()
                assert proc.wait(120) in (0, None)

                self._check_processes()
            except Exception:
                # List all logs
                subprocess.Popen(["ls", "-lah", tmpdir]).wait()
                # Dump all logs
                subprocess.Popen(["bash", "-c", f"ls {tmpdir}/* | xargs -n 1 -t cat"]).wait()
                raise

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
                    "--access-logformat",
                    "custom_log_format",
                    "airflow.cli.commands.internal_api_command:cached_app()",
                    "--preload",
                ],
                close_fds=True,
            )
