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
"""Tests for GunicornMonitor."""

from __future__ import annotations

import signal
from unittest import mock

import httpx
import psutil

from airflow.cli.commands.gunicorn_monitor import GunicornMonitor, create_monitor_from_config


class TestGunicornMonitor:
    """Tests for the GunicornMonitor class."""

    def test_init(self):
        """Test GunicornMonitor initialization."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/monitor/health",
        )

        assert monitor.gunicorn_master_pid == 12345
        assert monitor.num_workers_expected == 4
        assert monitor.worker_refresh_interval == 1800
        assert monitor.worker_refresh_batch_size == 1
        assert monitor.health_check_url == "http://localhost:8080/api/v2/monitor/health"

    def test_init_batch_size_warning(self, caplog):
        """Test that batch size is reduced when greater than worker count."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=2,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=5,  # Greater than workers
        )

        assert monitor.worker_refresh_batch_size == 2  # Reduced to match workers
        assert "reducing batch size to match worker count" in caplog.text

    def test_get_num_workers_running(self):
        """Test getting the number of running workers."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        mock_proc = mock.MagicMock(spec=psutil.Process)
        mock_children = [mock.MagicMock() for _ in range(3)]
        mock_proc.children.return_value = mock_children

        with mock.patch.object(monitor, "_get_gunicorn_master_proc", return_value=mock_proc):
            assert monitor._get_num_workers_running() == 3

    def test_get_num_workers_running_no_such_process(self):
        """Test handling when master process doesn't exist."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        with mock.patch.object(monitor, "_get_gunicorn_master_proc", side_effect=psutil.NoSuchProcess(12345)):
            assert monitor._get_num_workers_running() == 0

    def test_get_worker_pids(self):
        """Test getting worker PIDs."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        mock_proc = mock.MagicMock(spec=psutil.Process)
        mock_children = [mock.MagicMock(pid=100 + i) for i in range(3)]
        mock_proc.children.return_value = mock_children

        with mock.patch.object(monitor, "_get_gunicorn_master_proc", return_value=mock_proc):
            pids = monitor._get_worker_pids()
            assert pids == [100, 101, 102]

    def test_get_num_ready_workers(self):
        """Test getting the number of ready workers based on process title."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        mock_proc = mock.MagicMock(spec=psutil.Process)
        mock_children = [
            mock.MagicMock(cmdline=mock.MagicMock(return_value=["[ready]", "worker1"])),
            mock.MagicMock(cmdline=mock.MagicMock(return_value=["[ready]", "worker2"])),
            mock.MagicMock(cmdline=mock.MagicMock(return_value=["not_ready", "worker3"])),
        ]
        mock_proc.children.return_value = mock_children

        with mock.patch.object(monitor, "_get_gunicorn_master_proc", return_value=mock_proc):
            assert monitor._get_num_ready_workers() == 2

    def test_spawn_new_workers_sends_sigttin(self):
        """Test that SIGTTIN is sent to spawn new workers."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=2,
        )

        with mock.patch("os.kill") as mock_kill, mock.patch("time.sleep"):
            monitor._spawn_new_workers(2)

            assert mock_kill.call_count == 2
            mock_kill.assert_any_call(12345, signal.SIGTTIN)

    def test_kill_old_workers_sends_sigttou(self):
        """Test that SIGTTOU is sent to kill old workers."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=2,
        )

        with mock.patch("os.kill") as mock_kill, mock.patch("time.sleep"):
            monitor._kill_old_workers(2)

            assert mock_kill.call_count == 2
            mock_kill.assert_any_call(12345, signal.SIGTTOU)

    def test_wait_for_workers_success(self):
        """Test waiting for workers to reach target count."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        # Simulate workers increasing over time
        worker_counts = iter([2, 3, 4, 4])
        with (
            mock.patch.object(monitor, "_get_num_workers_running", side_effect=lambda: next(worker_counts)),
            mock.patch("time.sleep"),
            mock.patch("time.monotonic", side_effect=[0, 1, 2, 3]),
        ):
            result = monitor._wait_for_workers(target_count=4, timeout=60, check_ready=False)
            assert result is True

    def test_wait_for_workers_timeout(self):
        """Test timeout when waiting for workers."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        with (
            mock.patch.object(monitor, "_get_num_workers_running", return_value=2),
            mock.patch("time.sleep"),
            mock.patch("time.monotonic", side_effect=[0, 30, 61]),
        ):  # Exceed timeout
            result = monitor._wait_for_workers(target_count=4, timeout=60, check_ready=False)
            assert result is False

    def test_wait_until_healthy_success(self):
        """Test successful health check."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/monitor/health",
        )

        mock_response = mock.MagicMock(status_code=200)
        with (
            mock.patch("httpx.get", return_value=mock_response),
            mock.patch("time.monotonic", side_effect=[0, 1]),
        ):
            result = monitor._wait_until_healthy(timeout=60)
            assert result is True

    def test_wait_until_healthy_timeout(self):
        """Test health check timeout."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/monitor/health",
        )

        with (
            mock.patch("httpx.get", side_effect=httpx.RequestError("Connection failed")),
            mock.patch("time.sleep"),
            mock.patch("time.monotonic", side_effect=[0, 30, 61]),
        ):
            result = monitor._wait_until_healthy(timeout=60)
            assert result is False

    def test_wait_until_healthy_no_url(self):
        """Test that health check is skipped when no URL is configured."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=1,
            health_check_url=None,
        )

        result = monitor._wait_until_healthy(timeout=60)
        assert result is True

    def test_check_master_alive(self):
        """Test checking if master process is alive."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        mock_proc = mock.MagicMock(spec=psutil.Process)
        mock_proc.is_running.return_value = True

        with mock.patch.object(monitor, "_get_gunicorn_master_proc", return_value=mock_proc):
            assert monitor._check_master_alive() is True

        mock_proc.is_running.return_value = False
        with mock.patch.object(monitor, "_get_gunicorn_master_proc", return_value=mock_proc):
            assert monitor._check_master_alive() is False

    def test_refresh_workers_successful(self):
        """Test a successful worker refresh cycle."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/monitor/health",
        )

        # Mock _wait_for_workers to always return True immediately
        # Mock _get_worker_pids to simulate workers being replaced one at a time
        # Initial: [100, 101, 102, 103] -> Final: [104, 105, 106, 107]

        # Each iteration: get initial pids, spawn, (waits), kill, get updated pids
        worker_pids_sequence = [
            [100, 101, 102, 103],  # Initial call at start of refresh
            [101, 102, 103, 104],  # After batch 1 (100 killed, 104 spawned)
            [102, 103, 104, 105],  # After batch 2 (101 killed, 105 spawned)
            [103, 104, 105, 106],  # After batch 3 (102 killed, 106 spawned)
            [104, 105, 106, 107],  # After batch 4 (103 killed, 107 spawned) - empty original_pids
        ]

        with (
            mock.patch.object(monitor, "_get_worker_pids", side_effect=worker_pids_sequence),
            mock.patch.object(monitor, "_wait_for_workers", return_value=True),
            mock.patch.object(monitor, "_wait_until_healthy", return_value=True),
            mock.patch.object(monitor, "_spawn_new_workers") as mock_spawn,
            mock.patch.object(monitor, "_kill_old_workers") as mock_kill,
            mock.patch.object(monitor, "_get_num_workers_running", return_value=4),
            mock.patch("time.sleep"),
        ):
            monitor._refresh_workers()

            # Should have spawned 4 batches of 1 worker
            assert mock_spawn.call_count == 4
            # Should have killed 4 batches of 1 worker
            assert mock_kill.call_count == 4

    def test_refresh_workers_max_iterations_safety_limit(self, caplog):
        """Test that worker refresh terminates after max iterations to prevent infinite loops."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=2,
            worker_refresh_interval=1800,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/monitor/health",
        )

        # Simulate workers that never get replaced - always return same PIDs
        # This would cause an infinite loop without the max_iterations safety limit
        static_pids = [100, 101]

        with (
            mock.patch.object(monitor, "_get_worker_pids", return_value=static_pids),
            mock.patch.object(monitor, "_wait_for_workers", return_value=True),
            mock.patch.object(monitor, "_wait_until_healthy", return_value=True),
            mock.patch.object(monitor, "_spawn_new_workers") as mock_spawn,
            mock.patch.object(monitor, "_kill_old_workers") as mock_kill,
            mock.patch.object(monitor, "_get_num_workers_running", return_value=2),
            mock.patch("time.sleep"),
        ):
            monitor._refresh_workers()

            # max_iterations = num_workers_expected * 3 = 6
            # Loop should terminate after 6 iterations, not run forever
            assert mock_spawn.call_count == 6
            assert mock_kill.call_count == 6

            # Should log an error about incomplete refresh
            assert "Worker refresh incomplete" in caplog.text
            assert "2 original workers remain" in caplog.text

    def test_stop(self):
        """Test that stop() sets the stop flag."""
        monitor = GunicornMonitor(
            gunicorn_master_pid=12345,
            num_workers_expected=4,
            worker_refresh_interval=0,
            worker_refresh_batch_size=1,
        )

        assert monitor._should_stop is False
        monitor.stop()
        assert monitor._should_stop is True


class TestCreateMonitorFromConfig:
    """Tests for the create_monitor_from_config factory function."""

    def test_create_monitor_basic(self):
        """Test creating a monitor with basic settings."""
        with mock.patch(
            "airflow.cli.commands.gunicorn_monitor.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 2,
            }.get((section, key), fallback),
        ):
            monitor = create_monitor_from_config(
                gunicorn_master_pid=12345,
                num_workers=4,
                host="0.0.0.0",
                port=8080,
                ssl_enabled=False,
            )

            assert monitor.gunicorn_master_pid == 12345
            assert monitor.num_workers_expected == 4
            assert monitor.worker_refresh_interval == 1800
            assert monitor.worker_refresh_batch_size == 2
            # 0.0.0.0 should be converted to 127.0.0.1 for health check
            assert monitor.health_check_url == "http://127.0.0.1:8080/api/v2/monitor/health"

    def test_create_monitor_with_ssl(self):
        """Test creating a monitor with SSL enabled."""
        with mock.patch(
            "airflow.cli.commands.gunicorn_monitor.conf.getint",
            side_effect=lambda section, key, fallback=None: {
                ("api", "worker_refresh_interval"): 1800,
                ("api", "worker_refresh_batch_size"): 1,
            }.get((section, key), fallback),
        ):
            monitor = create_monitor_from_config(
                gunicorn_master_pid=12345,
                num_workers=4,
                host="localhost",
                port=8443,
                ssl_enabled=True,
            )

            assert monitor.health_check_url == "https://localhost:8443/api/v2/monitor/health"


class TestBuildGunicornCommand:
    """Tests for the _build_gunicorn_command function."""

    def test_basic_command(self):
        """Test building a basic gunicorn command."""
        from airflow.cli.commands.api_server_command import _build_gunicorn_command

        cmd = _build_gunicorn_command(
            host="0.0.0.0",
            port=8080,
            num_workers=4,
            worker_timeout=120,
            ssl_cert=None,
            ssl_key=None,
            log_level="info",
            access_log_enabled=True,
            proxy_headers=False,
        )

        assert cmd[0] == "gunicorn"
        assert "airflow.api_fastapi.main:app" in cmd
        assert "--worker-class" in cmd
        assert "uvicorn.workers.UvicornWorker" in cmd
        assert "--bind" in cmd
        assert "0.0.0.0:8080" in cmd
        assert "--workers" in cmd
        assert "4" in cmd
        assert "--preload" in cmd

    def test_command_with_ssl(self):
        """Test building a gunicorn command with SSL."""
        from airflow.cli.commands.api_server_command import _build_gunicorn_command

        cmd = _build_gunicorn_command(
            host="0.0.0.0",
            port=8443,
            num_workers=4,
            worker_timeout=120,
            ssl_cert="/path/to/cert.pem",
            ssl_key="/path/to/key.pem",
            log_level="info",
            access_log_enabled=True,
            proxy_headers=False,
        )

        assert "--certfile" in cmd
        assert "/path/to/cert.pem" in cmd
        assert "--keyfile" in cmd
        assert "/path/to/key.pem" in cmd

    def test_command_with_proxy_headers(self):
        """Test building a gunicorn command with proxy headers."""
        from airflow.cli.commands.api_server_command import _build_gunicorn_command

        cmd = _build_gunicorn_command(
            host="0.0.0.0",
            port=8080,
            num_workers=4,
            worker_timeout=120,
            ssl_cert=None,
            ssl_key=None,
            log_level="info",
            access_log_enabled=True,
            proxy_headers=True,
        )

        assert "--forwarded-allow-ips" in cmd
        assert "*" in cmd

    def test_command_with_access_log_enabled(self):
        """Test that access log is enabled when access_log_enabled=True."""
        from airflow.cli.commands.api_server_command import _build_gunicorn_command

        cmd = _build_gunicorn_command(
            host="0.0.0.0",
            port=8080,
            num_workers=4,
            worker_timeout=120,
            ssl_cert=None,
            ssl_key=None,
            log_level="info",
            access_log_enabled=True,
            proxy_headers=False,
        )

        assert "--access-logfile" in cmd
        assert "-" in cmd  # Logs to stdout

    def test_command_with_access_log_disabled(self):
        """Test that access log is not added when access_log_enabled=False."""
        from airflow.cli.commands.api_server_command import _build_gunicorn_command

        cmd = _build_gunicorn_command(
            host="0.0.0.0",
            port=8080,
            num_workers=4,
            worker_timeout=120,
            ssl_cert=None,
            ssl_key=None,
            log_level="error",
            access_log_enabled=False,
            proxy_headers=False,
        )

        assert "--access-logfile" not in cmd
