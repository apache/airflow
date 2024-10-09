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
from airflow.cli.commands import webserver_command
from airflow.cli.commands.webserver_command import GunicornMonitor
from tests.cli.commands._common_cli_classes import _CommonCLIGunicornTestClass

from dev.tests_common.test_utils.config import conf_vars

console = Console(width=400, color_system="standard")


class TestGunicornMonitor:
    def setup_method(self) -> None:
        self.monitor = GunicornMonitor(
            gunicorn_master_pid=1,
            num_workers_expected=4,
            master_timeout=60,
            worker_refresh_interval=60,
            worker_refresh_batch_size=2,
            reload_on_plugin_change=True,
        )
        mock.patch.object(self.monitor, "_generate_plugin_state", return_value={}).start()
        mock.patch.object(self.monitor, "_get_num_ready_workers_running", return_value=4).start()
        mock.patch.object(self.monitor, "_get_num_workers_running", return_value=4).start()
        mock.patch.object(self.monitor, "_spawn_new_workers", return_value=None).start()
        mock.patch.object(self.monitor, "_kill_old_workers", return_value=None).start()
        mock.patch.object(self.monitor, "_reload_gunicorn", return_value=None).start()

    @mock.patch("airflow.cli.commands.webserver_command.sleep")
    def test_should_wait_for_workers_to_start(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 0
        self.monitor._get_num_workers_running.return_value = 4
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_not_called()
        self.monitor._kill_old_workers.assert_not_called()
        self.monitor._reload_gunicorn.assert_not_called()

    @mock.patch("airflow.cli.commands.webserver_command.sleep")
    def test_should_kill_excess_workers(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 10
        self.monitor._get_num_workers_running.return_value = 10
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_not_called()
        self.monitor._kill_old_workers.assert_called_once_with(2)
        self.monitor._reload_gunicorn.assert_not_called()

    @mock.patch("airflow.cli.commands.webserver_command.sleep")
    def test_should_start_new_workers_when_missing(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 3
        self.monitor._get_num_workers_running.return_value = 3
        self.monitor._check_workers()
        # missing one worker, starting just 1
        self.monitor._spawn_new_workers.assert_called_once_with(1)
        self.monitor._kill_old_workers.assert_not_called()
        self.monitor._reload_gunicorn.assert_not_called()

    @mock.patch("airflow.cli.commands.webserver_command.sleep")
    def test_should_start_new_batch_when_missing_many_workers(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 1
        self.monitor._get_num_workers_running.return_value = 1
        self.monitor._check_workers()
        # missing 3 workers, but starting single batch (2)
        self.monitor._spawn_new_workers.assert_called_once_with(2)
        self.monitor._kill_old_workers.assert_not_called()
        self.monitor._reload_gunicorn.assert_not_called()

    @mock.patch("airflow.cli.commands.webserver_command.sleep")
    def test_should_start_new_workers_when_refresh_interval_has_passed(self, mock_sleep):
        self.monitor._last_refresh_time -= 200
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_called_once_with(2)
        self.monitor._kill_old_workers.assert_not_called()
        self.monitor._reload_gunicorn.assert_not_called()
        assert abs(self.monitor._last_refresh_time - time.monotonic()) < 5

    @mock.patch("airflow.cli.commands.webserver_command.sleep")
    def test_should_reload_when_plugin_has_been_changed(self, mock_sleep):
        self.monitor._generate_plugin_state.return_value = {"AA": 12}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()
        self.monitor._kill_old_workers.assert_not_called()
        self.monitor._reload_gunicorn.assert_not_called()

        self.monitor._generate_plugin_state.return_value = {"AA": 32}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()
        self.monitor._kill_old_workers.assert_not_called()
        self.monitor._reload_gunicorn.assert_not_called()

        self.monitor._generate_plugin_state.return_value = {"AA": 32}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()
        self.monitor._kill_old_workers.assert_not_called()
        self.monitor._reload_gunicorn.assert_called_once_with()
        assert abs(self.monitor._last_refresh_time - time.monotonic()) < 5


class TestGunicornMonitorGeneratePluginState:
    def test_should_detect_changes_in_directory(self, tmp_path):
        with mock.patch(
            "airflow.cli.commands.webserver_command.settings.PLUGINS_FOLDER", os.fspath(tmp_path)
        ):
            (tmp_path / "file1.txt").write_text("A" * 100)
            path2 = tmp_path / "nested/nested/nested/nested/file2.txt"
            path2.parent.mkdir(parents=True)
            path2.write_text("A" * 200)
            (tmp_path / "file3.txt").write_text("A" * 300)

            monitor = GunicornMonitor(
                gunicorn_master_pid=1,
                num_workers_expected=4,
                master_timeout=60,
                worker_refresh_interval=60,
                worker_refresh_batch_size=2,
                reload_on_plugin_change=True,
            )

            # When the files have not changed, the result should be constant
            state_a = monitor._generate_plugin_state()
            state_b = monitor._generate_plugin_state()

            assert state_a == state_b
            assert 3 == len(state_a)

            # Should detect new file
            (tmp_path / "file4.txt").write_text("A" * 400)

            state_c = monitor._generate_plugin_state()

            assert state_b != state_c
            assert 4 == len(state_c)

            # Should detect changes in files
            (tmp_path / "file4.txt").write_text("A" * 450)

            state_d = monitor._generate_plugin_state()

            assert state_c != state_d
            assert 4 == len(state_d)

            # Should support large files
            (tmp_path / "file4.txt").write_text("A" * 4_000_000)

            state_d = monitor._generate_plugin_state()

            assert state_c != state_d
            assert 4 == len(state_d)


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
class TestCliWebServer(_CommonCLIGunicornTestClass):
    main_process_regexp = r"airflow webserver"

    @pytest.mark.execution_timeout(400)
    def test_cli_webserver_background(self, tmp_path):
        with mock.patch.dict(
            "os.environ",
            AIRFLOW__CORE__DAGS_FOLDER="/dev/null",
            AIRFLOW__CORE__LOAD_EXAMPLES="False",
            AIRFLOW__WEBSERVER__WORKERS="1",
        ):
            pidfile_webserver = tmp_path / "pidflow-webserver.pid"
            pidfile_monitor = tmp_path / "pidflow-webserver-monitor.pid"
            stdout = tmp_path / "airflow-webserver.out"
            stderr = tmp_path / "airflow-webserver.err"
            logfile = tmp_path / "airflow-webserver.log"
            try:
                # Run webserver as daemon in background. Note that the wait method is not called.

                proc = subprocess.Popen(
                    [
                        "airflow",
                        "webserver",
                        "--daemon",
                        "--pid",
                        os.fspath(pidfile_webserver),
                        "--stdout",
                        os.fspath(stdout),
                        "--stderr",
                        os.fspath(stderr),
                        "--log-file",
                        os.fspath(logfile),
                    ]
                )
                assert proc.poll() is None

                pid_monitor = self._wait_pidfile(pidfile_monitor)
                console.print(f"[blue]Monitor started at {pid_monitor}")
                pid_webserver = self._wait_pidfile(pidfile_webserver)
                console.print(f"[blue]Webserver started at {pid_webserver}")
                console.print("[blue]Running airflow webserver process:")
                # Assert that the webserver and gunicorn processes are running (by name rather than pid).
                assert self._find_process(r"airflow webserver", print_found_process=True)
                console.print("[blue]Waiting for gunicorn processes:")
                # wait for gunicorn to start
                for _ in range(120):
                    if self._find_process(r"^gunicorn"):
                        break
                    console.print("[blue]Waiting for gunicorn to start ...")
                    time.sleep(1)
                else:
                    pytest.fail("Gunicorn processes not found after 120 seconds")
                console.print("[blue]Running gunicorn processes:")
                assert self._find_all_processes("^gunicorn", print_found_process=True)
                console.print("[magenta]Webserver process started successfully.")
                console.print(
                    "[magenta]Terminating monitor process and expect "
                    "Webserver and gunicorn processes to terminate as well"
                )
                proc = psutil.Process(pid_monitor)
                proc.terminate()
                assert proc.wait(120) in (0, None)
                self._check_processes(ignore_running=False)
                console.print("[magenta]All Webserver and gunicorn processes are terminated.")
            except Exception:
                console.print("[red]Exception occurred. Dumping all logs.")
                # Dump all logs
                for file in tmp_path.glob("*"):
                    console.print(f"Dumping {file} (size: {file.stat().st_size})")
                    console.print(file.read_text())
                raise

    # Patch for causing webserver timeout
    @mock.patch(
        "airflow.cli.commands.webserver_command.GunicornMonitor._get_num_workers_running", return_value=0
    )
    def test_cli_webserver_shutdown_when_gunicorn_master_is_killed(self, _):
        # Shorten timeout so that this test doesn't take too long time
        args = self.parser.parse_args(["webserver"])
        with conf_vars({("webserver", "web_server_master_timeout"): "10"}):
            with pytest.raises(SystemExit) as ctx:
                webserver_command.webserver(args)
        assert ctx.value.code == 1

    def test_cli_webserver_debug(self, app):
        with mock.patch("airflow.www.app.create_app", return_value=app), mock.patch.object(
            app, "run"
        ) as app_run:
            args = self.parser.parse_args(
                [
                    "webserver",
                    "--debug",
                ]
            )
            webserver_command.webserver(args)

            app_run.assert_called_with(
                debug=True,
                use_reloader=False,
                port=8080,
                host="0.0.0.0",
                ssl_context=None,
            )

    def test_cli_webserver_args(self):
        with mock.patch("subprocess.Popen") as Popen, mock.patch.object(webserver_command, "GunicornMonitor"):
            args = self.parser.parse_args(
                [
                    "webserver",
                    "--access-logformat",
                    "custom_log_format",
                    "--pid",
                    "/tmp/x.pid",
                ]
            )
            webserver_command.webserver(args)

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
                    "0.0.0.0:8080",
                    "--name",
                    "airflow-webserver",
                    "--pid",
                    "/tmp/x.pid",
                    "--config",
                    "python:airflow.www.gunicorn_config",
                    "--access-logfile",
                    "-",
                    "--error-logfile",
                    "-",
                    "--access-logformat",
                    "custom_log_format",
                    "airflow.www.app:cached_app()",
                    "--preload",
                ],
                close_fds=True,
            )
