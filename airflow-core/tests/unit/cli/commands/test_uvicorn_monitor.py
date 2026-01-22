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
"""Tests for UvicornMonitor."""

from __future__ import annotations

import signal
import urllib.error
from unittest import mock

import psutil
import pytest

from airflow.cli.commands.uvicorn_monitor import UvicornMonitor

# Store reference to real psutil.Process for use in specs before patching
_RealProcess = psutil.Process


class TestUvicornMonitor:
    """Test cases for UvicornMonitor class."""

    @pytest.fixture
    def mock_process(self):
        """Create a mock psutil.Process with proper spec."""
        with mock.patch("psutil.Process", autospec=True) as mock_process_class:
            mock_proc = mock.MagicMock(spec=_RealProcess)
            mock_proc.is_running.return_value = True
            mock_proc.children.return_value = [mock.MagicMock(spec=_RealProcess, pid=1001)]
            mock_process_class.return_value = mock_proc
            yield mock_proc

    def test_init(self, mock_process):
        """Test UvicornMonitor initialization."""
        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        assert monitor.num_workers_expected == 2
        assert monitor.worker_refresh_interval == 60
        assert monitor.worker_refresh_batch_size == 1
        assert monitor.health_check_url == "http://localhost:8080/api/v2/version"

    def test_get_num_workers_running(self, mock_process):
        """Test _get_num_workers_running returns correct count."""
        mock_process.children.return_value = [
            mock.MagicMock(spec=_RealProcess, pid=1001),
            mock.MagicMock(spec=_RealProcess, pid=1002),
        ]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        assert monitor._get_num_workers_running() == 2

    def test_get_worker_pids(self, mock_process):
        """Test _get_worker_pids returns list of PIDs."""
        mock_process.children.return_value = [
            mock.MagicMock(spec=_RealProcess, pid=1001),
            mock.MagicMock(spec=_RealProcess, pid=1002),
        ]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        assert monitor._get_worker_pids() == [1001, 1002]

    def test_get_num_workers_running_no_such_process(self, mock_process):
        """Test _get_num_workers_running handles NoSuchProcess."""
        mock_process.children.side_effect = psutil.NoSuchProcess(1234)

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        assert monitor._get_num_workers_running() == 0

    def test_spawn_new_workers_sends_sigttin(self, mock_process):
        """Test _spawn_new_workers sends SIGTTIN signals."""
        # Initially 2 workers, after spawn should be 3
        mock_process.children.side_effect = [
            [mock.MagicMock(spec=_RealProcess, pid=1001), mock.MagicMock(spec=_RealProcess, pid=1002)],
            [mock.MagicMock(spec=_RealProcess, pid=1001), mock.MagicMock(spec=_RealProcess, pid=1002)],
            [
                mock.MagicMock(spec=_RealProcess, pid=1001),
                mock.MagicMock(spec=_RealProcess, pid=1002),
                mock.MagicMock(spec=_RealProcess, pid=1003),
            ],
            [
                mock.MagicMock(spec=_RealProcess, pid=1001),
                mock.MagicMock(spec=_RealProcess, pid=1002),
                mock.MagicMock(spec=_RealProcess, pid=1003),
            ],
        ]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        success, new_pids = monitor._spawn_new_workers(1)

        assert success is True
        assert new_pids == [1003]
        mock_process.send_signal.assert_called_with(signal.SIGTTIN)

    @mock.patch("os.kill")
    def test_kill_old_workers_sends_sigterm_to_specific_pids(self, mock_kill, mock_process):
        """Test _kill_old_workers sends SIGTERM to specific old worker PIDs."""
        # Start with 3 workers, after kill should be 2
        mock_process.children.side_effect = [
            [mock.MagicMock(spec=_RealProcess, pid=p) for p in [1001, 1002, 1003]],
            [mock.MagicMock(spec=_RealProcess, pid=p) for p in [1002, 1003]],
        ]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        monitor._kill_old_workers(1, [1001, 1002, 1003])

        # Should send SIGTERM to the specific old worker PID (1001), not SIGTTOU to parent
        mock_kill.assert_called_with(1001, signal.SIGTERM)

    def test_wait_for_workers_increase_success(self, mock_process):
        """Test _wait_for_workers returns True when target reached (increase)."""
        mock_process.children.return_value = [
            mock.MagicMock(spec=_RealProcess, pid=1001),
            mock.MagicMock(spec=_RealProcess, pid=1002),
        ]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        result = monitor._wait_for_workers(2, wait_for_increase=True, timeout=1)
        assert result is True

    def test_wait_for_workers_decrease_success(self, mock_process):
        """Test _wait_for_workers returns True when target reached (decrease)."""
        mock_process.children.return_value = [
            mock.MagicMock(spec=_RealProcess, pid=1001),
            mock.MagicMock(spec=_RealProcess, pid=1002),
        ]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        result = monitor._wait_for_workers(2, wait_for_increase=False, timeout=1)
        assert result is True

    @mock.patch("time.monotonic")
    @mock.patch("time.sleep")
    def test_wait_for_workers_timeout(self, mock_sleep, mock_monotonic, mock_process):
        """Test _wait_for_workers returns False on timeout."""
        mock_process.children.return_value = [mock.MagicMock(spec=_RealProcess, pid=1001)]
        mock_monotonic.side_effect = [0, 0, 31, 32]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        result = monitor._wait_for_workers(2, wait_for_increase=True, timeout=30)
        assert result is False

    @mock.patch("urllib.request.urlopen")
    def test_wait_until_healthy_success(self, mock_urlopen, mock_process):
        """Test _wait_until_healthy returns True on successful response."""
        mock_response = mock.MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = mock.MagicMock(return_value=mock_response)
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        result = monitor._wait_until_healthy(timeout=1)
        assert result is True

    @mock.patch("time.monotonic")
    @mock.patch("time.sleep")
    @mock.patch("urllib.request.urlopen")
    def test_wait_until_healthy_timeout(self, mock_urlopen, mock_sleep, mock_monotonic, mock_process):
        """Test _wait_until_healthy returns False on timeout."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        mock_monotonic.side_effect = [0, 0, 121, 122]

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        result = monitor._wait_until_healthy(timeout=120)
        assert result is False

    @mock.patch("os.kill")
    @mock.patch("urllib.request.urlopen")
    @mock.patch("time.monotonic")
    def test_refresh_workers_successful(self, mock_monotonic, mock_urlopen, mock_kill, mock_process):
        """Test _refresh_workers completes successfully."""
        mock_monotonic.return_value = 0

        # Mock health check success
        mock_response = mock.MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = mock.MagicMock(return_value=mock_response)
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        # Set up worker count transitions with PIDs
        call_count = [0]

        def children_side_effect():
            call_count[0] += 1
            if call_count[0] <= 2:
                # Initial: 2 workers
                return [mock.MagicMock(spec=_RealProcess, pid=p) for p in [1001, 1002]]
            if call_count[0] <= 5:
                # After spawn: 3 workers
                return [mock.MagicMock(spec=_RealProcess, pid=p) for p in [1001, 1002, 1003]]
            # After kill: 2 workers
            return [mock.MagicMock(spec=_RealProcess, pid=p) for p in [1002, 1003]]

        mock_process.children.side_effect = children_side_effect

        monitor = UvicornMonitor(
            uvicorn_parent_pid=1234,
            num_workers_expected=2,
            worker_refresh_interval=60,
            worker_refresh_batch_size=1,
            health_check_url="http://localhost:8080/api/v2/version",
        )

        monitor._refresh_workers()

        # Should have sent SIGTTIN to spawn new worker
        assert any(call[0][0] == signal.SIGTTIN for call in mock_process.send_signal.call_args_list)
        # Should have sent SIGTERM directly to old worker PID (1001)
        mock_kill.assert_called_with(1001, signal.SIGTERM)

    def test_refresh_workers_batch_size_limited_to_workers(self, mock_process):
        """Test _refresh_workers limits batch_size to num_workers_expected."""
        mock_process.children.return_value = [mock.MagicMock(spec=_RealProcess, pid=1001)]

        with mock.patch.object(
            UvicornMonitor, "_spawn_new_workers", return_value=(True, [1002])
        ) as mock_spawn:
            with mock.patch.object(UvicornMonitor, "_wait_until_healthy", return_value=True):
                with mock.patch.object(UvicornMonitor, "_kill_old_workers"):
                    with mock.patch("time.monotonic", return_value=0):
                        monitor = UvicornMonitor(
                            uvicorn_parent_pid=1234,
                            num_workers_expected=1,  # Only 1 worker
                            worker_refresh_interval=60,
                            worker_refresh_batch_size=5,  # Batch size larger than workers
                            health_check_url="http://localhost:8080/api/v2/version",
                        )

                        monitor._refresh_workers()

                        # Should have been limited to 1 (min of batch_size and num_workers)
                        mock_spawn.assert_called_once_with(1)
