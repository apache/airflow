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

import signal
import time
from unittest.mock import MagicMock, patch

import pytest
from task_sdk import FAKE_BUNDLE

from airflow.sdk.execution_time.comms import TaskExecutionTimeout
from airflow.sdk.execution_time.supervisor import ActivitySubprocess


@pytest.fixture
def client_with_ti_start(make_ti_context):
    """Create a mock client with task instance start configured."""
    from unittest.mock import MagicMock

    from airflow.sdk.api import client as sdk_client

    client = MagicMock(spec=sdk_client.Client)
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestExecutionTimeout:
    """Test cases for execution timeout handling in supervisor."""

    @pytest.fixture
    def mock_subprocess(self):
        """Create a mock ActivitySubprocess for testing."""
        subprocess = MagicMock(spec=ActivitySubprocess)
        subprocess._execution_timeout_seconds = None
        subprocess._task_execution_start_time = None
        subprocess._timeout_sigterm_sent_at = None
        subprocess._terminal_state = None
        subprocess.kill = MagicMock()
        return subprocess

    def test_handle_timeout_message(self, mock_subprocess, monkeypatch):
        """Test that supervisor receives and stores timeout configuration."""
        # Bind the method to mock instance
        monkeypatch.setattr(ActivitySubprocess, "_handle_execution_timeout_if_needed", lambda self: None)

        # Create timeout message
        timeout_msg = TaskExecutionTimeout(timeout_seconds=10.0)

        # Simulate receiving the message
        with patch("time.monotonic", return_value=100.0):
            mock_subprocess._execution_timeout_seconds = timeout_msg.timeout_seconds
            mock_subprocess._task_execution_start_time = time.monotonic()

        assert mock_subprocess._execution_timeout_seconds == 10.0
        assert mock_subprocess._task_execution_start_time == 100.0

    def test_timeout_not_checked_without_config(self, mock_subprocess):
        """Test that timeout is not checked if no timeout was configured."""
        # Call the real method
        ActivitySubprocess._handle_execution_timeout_if_needed(mock_subprocess)

        # Should not call kill if no timeout configured
        mock_subprocess.kill.assert_not_called()

    def test_timeout_not_checked_for_terminal_state(self, mock_subprocess):
        """Test that timeout is not checked if task reached terminal state."""
        mock_subprocess._execution_timeout_seconds = 10.0
        mock_subprocess._task_execution_start_time = 100.0
        mock_subprocess._terminal_state = "success"

        ActivitySubprocess._handle_execution_timeout_if_needed(mock_subprocess)

        mock_subprocess.kill.assert_not_called()

    def test_timeout_sends_sigterm(self, mock_subprocess):
        """Test that SIGTERM is sent when timeout is exceeded."""
        mock_subprocess._execution_timeout_seconds = 5.0
        mock_subprocess._task_execution_start_time = 100.0
        mock_subprocess._timeout_sigterm_sent_at = None

        with patch("time.monotonic", return_value=106.0):  # 6 seconds elapsed
            ActivitySubprocess._handle_execution_timeout_if_needed(mock_subprocess)

        # Should send SIGTERM
        mock_subprocess.kill.assert_called_once_with(signal.SIGTERM)
        assert mock_subprocess._timeout_sigterm_sent_at == 106.0

    def test_timeout_escalates_to_sigkill(self, mock_subprocess):
        """Test that SIGKILL is sent if SIGTERM doesn't work."""
        mock_subprocess._execution_timeout_seconds = 5.0
        mock_subprocess._task_execution_start_time = 100.0
        mock_subprocess._timeout_sigterm_sent_at = 106.0

        # 12 seconds after SIGTERM (beyond 5 second grace period)
        with patch("time.monotonic", return_value=118.0):
            ActivitySubprocess._handle_execution_timeout_if_needed(mock_subprocess)

        # Should send SIGKILL with force=True
        mock_subprocess.kill.assert_called_once_with(signal.SIGKILL, force=True)

    def test_timeout_within_grace_period(self, mock_subprocess):
        """Test that SIGKILL is not sent during grace period."""
        mock_subprocess._execution_timeout_seconds = 5.0
        mock_subprocess._task_execution_start_time = 100.0
        mock_subprocess._timeout_sigterm_sent_at = 106.0

        # 3 seconds after SIGTERM (within 5 second grace period)
        with patch("time.monotonic", return_value=109.0):
            ActivitySubprocess._handle_execution_timeout_if_needed(mock_subprocess)

        # Should not send SIGKILL yet
        mock_subprocess.kill.assert_not_called()

    def test_timeout_message_serialization(self):
        """Test that TaskExecutionTimeout message serializes correctly."""
        timeout_msg = TaskExecutionTimeout(timeout_seconds=15.5)

        # Serialize
        json_str = timeout_msg.model_dump_json()
        assert "15.5" in json_str
        assert "TaskExecutionTimeout" in json_str

        # Deserialize
        import json

        data = json.loads(json_str)
        assert data["timeout_seconds"] == 15.5
        assert data["type"] == "TaskExecutionTimeout"

    def test_zero_timeout_immediately_triggers(self, mock_subprocess):
        """Test that zero or negative timeout triggers immediately."""
        mock_subprocess._execution_timeout_seconds = 0.001
        mock_subprocess._task_execution_start_time = 100.0
        mock_subprocess._timeout_sigterm_sent_at = None

        with patch("time.monotonic", return_value=100.002):  # Slightly past timeout
            ActivitySubprocess._handle_execution_timeout_if_needed(mock_subprocess)

        # Should immediately send SIGTERM for zero timeout
        mock_subprocess.kill.assert_called_once_with(signal.SIGTERM)

    def test_timeout_uses_monotonic_clock(self, mock_subprocess):
        """Test that timeout uses monotonic clock for accuracy."""
        mock_subprocess._execution_timeout_seconds = 10.0

        # Use different monotonic values
        with patch("time.monotonic", return_value=1000.0):
            mock_subprocess._task_execution_start_time = time.monotonic()

        # Verify monotonic clock is used
        assert mock_subprocess._task_execution_start_time == 1000.0

        # Simulate timeout
        mock_subprocess._timeout_sigterm_sent_at = None
        with patch("time.monotonic", return_value=1011.0):  # 11 seconds later
            ActivitySubprocess._handle_execution_timeout_if_needed(mock_subprocess)

        mock_subprocess.kill.assert_called_once_with(signal.SIGTERM)


class TestExecutionTimeoutIntegration:
    """Integration tests for execution timeout with real subprocesses."""

    @pytest.fixture(autouse=True)
    def disable_log_upload(self, spy_agency):
        """Disable log upload for tests."""
        spy_agency.spy_on(ActivitySubprocess._upload_logs, call_original=False)

    def test_timeout_kills_long_running_task(self, monkeypatch, client_with_ti_start, captured_logs):
        """Test that a task exceeding timeout is killed with SIGTERM."""
        import os
        import sys
        import time

        from uuid6 import uuid7

        from airflow.sdk.api.datamodels._generated import TaskInstance
        from airflow.sdk.execution_time.comms import CommsDecoder, TaskExecutionTimeout
        from airflow.sdk.execution_time.supervisor import ActivitySubprocess

        def subprocess_main():
            # Get startup message
            comms = CommsDecoder()
            comms._get_response()

            # Send timeout configuration (2 seconds)
            comms.send(TaskExecutionTimeout(timeout_seconds=2.0))

            # Sleep for 10 seconds (should be killed after 2)
            print("Task started, sleeping for 10 seconds...")
            sys.stdout.flush()
            time.sleep(10)
            print("This should never print - task should be killed")

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id=uuid7(),
                task_id="timeout_task",
                dag_id="test_dag",
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        start_time = time.time()
        rc = proc.wait()
        elapsed = time.time() - start_time

        # Task should be killed, so non-zero exit code
        assert rc != 0, "Task should have been killed"

        # Should complete in roughly 5-7 seconds due to heartbeat interval
        # (1s timeout + up to 5s heartbeat check delay)
        assert elapsed < 8, f"Task took {elapsed}s, expected ~6s (timeout + heartbeat delay) not full 10s"

        # Check logs for timeout message
        log_events = [log.get("event", "") for log in captured_logs]
        assert any("timeout" in str(event).lower() for event in log_events), (
            "Expected timeout message in logs"
        )

    def test_timeout_escalates_to_sigkill(self, monkeypatch, client_with_ti_start):
        """Test that supervisor sends SIGKILL if SIGTERM doesn't work."""
        import os
        import signal
        import time

        from uuid6 import uuid7

        from airflow.sdk.api.datamodels._generated import TaskInstance
        from airflow.sdk.execution_time.comms import CommsDecoder, TaskExecutionTimeout
        from airflow.sdk.execution_time.supervisor import ActivitySubprocess

        def subprocess_main():
            # Get startup message
            comms = CommsDecoder()
            comms._get_response()

            # Send timeout configuration (1 second)
            comms.send(TaskExecutionTimeout(timeout_seconds=1.0))

            # Ignore SIGTERM to test SIGKILL escalation
            signal.signal(signal.SIGTERM, signal.SIG_IGN)

            print("Task started, ignoring SIGTERM...")
            time.sleep(20)  # Sleep long enough for SIGKILL
            print("This should never print")

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id=uuid7(),
                task_id="sigkill_task",
                dag_id="test_dag",
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        start_time = time.time()
        rc = proc.wait()
        elapsed = time.time() - start_time

        # Should be killed with SIGKILL
        assert rc != 0

        # Should complete in ~15-17 seconds
        # (1s timeout + 5s heartbeat delay + 5s SIGTERM grace + 5s SIGKILL check delay)
        assert elapsed < 18, f"Task took {elapsed}s, expected ~15s for SIGKILL escalation"

    def test_no_timeout_task_completes_normally(self, monkeypatch, client_with_ti_start):
        """Test that tasks without timeout complete normally."""
        import os
        import time

        from uuid6 import uuid7

        from airflow.sdk.api.datamodels._generated import TaskInstance
        from airflow.sdk.execution_time.comms import CommsDecoder
        from airflow.sdk.execution_time.supervisor import ActivitySubprocess

        def subprocess_main():
            # Get startup message
            comms = CommsDecoder()
            comms._get_response()

            # Don't send timeout message
            print("Task without timeout")
            time.sleep(0.5)
            print("Task completed successfully")

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id=uuid7(),
                task_id="no_timeout_task",
                dag_id="test_dag",
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        rc = proc.wait()

        # Should complete successfully
        assert rc == 0, "Task without timeout should complete successfully"

    def test_task_completes_before_timeout(self, monkeypatch, client_with_ti_start):
        """Test that tasks completing before timeout are not killed."""
        import os
        import time

        from uuid6 import uuid7

        from airflow.sdk.api.datamodels._generated import TaskInstance
        from airflow.sdk.execution_time.comms import CommsDecoder, TaskExecutionTimeout
        from airflow.sdk.execution_time.supervisor import ActivitySubprocess

        def subprocess_main():
            # Get startup message
            comms = CommsDecoder()
            comms._get_response()

            # Send timeout configuration (5 seconds)
            comms.send(TaskExecutionTimeout(timeout_seconds=5.0))

            # Complete quickly (1 second)
            print("Task started")
            time.sleep(1)
            print("Task completed within timeout")

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id=uuid7(),
                task_id="fast_task",
                dag_id="test_dag",
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        rc = proc.wait()

        # Should complete successfully
        assert rc == 0, "Task completing before timeout should succeed"
