#
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

from unittest import mock

import pytest

from airflow.providers.ssh.triggers.ssh_remote_job import SSHRemoteJobTrigger


def _make_trigger(**overrides):
    kwargs = dict(
        ssh_conn_id="test_conn",
        remote_host=None,
        job_id="test_job",
        job_dir="/tmp/job",
        log_file="/tmp/job/stdout.log",
        exit_code_file="/tmp/job/exit_code",
        remote_os="posix",
    )
    kwargs.update(overrides)
    return SSHRemoteJobTrigger(**kwargs)


class TestSSHRemoteJobTrigger:
    def test_serialization(self):
        """Test that the trigger can be serialized correctly."""
        trigger = SSHRemoteJobTrigger(
            ssh_conn_id="test_conn",
            remote_host="test.example.com",
            job_id="test_job_123",
            job_dir="/tmp/airflow-ssh-jobs/test_job_123",
            log_file="/tmp/airflow-ssh-jobs/test_job_123/stdout.log",
            exit_code_file="/tmp/airflow-ssh-jobs/test_job_123/exit_code",
            remote_os="posix",
            poll_interval=10,
            log_chunk_size=32768,
            log_offset=1000,
            max_reconnect_attempts=7,
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.ssh.triggers.ssh_remote_job.SSHRemoteJobTrigger"
        assert kwargs["ssh_conn_id"] == "test_conn"
        assert kwargs["remote_host"] == "test.example.com"
        assert kwargs["job_id"] == "test_job_123"
        assert kwargs["job_dir"] == "/tmp/airflow-ssh-jobs/test_job_123"
        assert kwargs["log_file"] == "/tmp/airflow-ssh-jobs/test_job_123/stdout.log"
        assert kwargs["exit_code_file"] == "/tmp/airflow-ssh-jobs/test_job_123/exit_code"
        assert kwargs["remote_os"] == "posix"
        assert kwargs["poll_interval"] == 10
        assert kwargs["log_chunk_size"] == 32768
        assert kwargs["log_offset"] == 1000
        assert kwargs["max_reconnect_attempts"] == 7

    def test_serialization_round_trips(self):
        """The serialized kwargs must be sufficient to rebuild the trigger."""
        trigger = _make_trigger(poll_interval=3, max_reconnect_attempts=2)
        _, kwargs = trigger.serialize()
        rebuilt = SSHRemoteJobTrigger(**kwargs)
        assert rebuilt.serialize() == trigger.serialize()

    def test_default_values(self):
        """Test default parameter values."""
        trigger = _make_trigger()

        assert trigger.poll_interval == 5
        assert trigger.log_chunk_size == 65536
        assert trigger.log_offset == 0
        assert trigger.max_reconnect_attempts == 5

    @pytest.mark.asyncio
    async def test_run_job_completed_success(self):
        """Test trigger when job completes successfully."""
        trigger = _make_trigger()

        with (
            mock.patch.object(trigger, "_connect", return_value=mock.MagicMock()),
            mock.patch.object(trigger, "_close", return_value=None),
            mock.patch.object(trigger, "_check_completion", return_value=0),
            mock.patch.object(trigger, "_read_log_chunk", return_value=("Final output\n", 100)),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert events[0].payload["done"] is True
        assert events[0].payload["exit_code"] == 0
        assert events[0].payload["log_chunk"] == "Final output\n"

    @pytest.mark.asyncio
    async def test_run_job_completed_failure(self):
        """Test trigger when job completes with failure."""
        trigger = _make_trigger()

        with (
            mock.patch.object(trigger, "_connect", return_value=mock.MagicMock()),
            mock.patch.object(trigger, "_close", return_value=None),
            mock.patch.object(trigger, "_check_completion", return_value=1),
            mock.patch.object(trigger, "_read_log_chunk", return_value=("Error output\n", 50)),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "failed"
        assert events[0].payload["done"] is True
        assert events[0].payload["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_run_reuses_single_connection_across_polls(self):
        """The connection is opened once and reused for every poll, not per command."""
        trigger = _make_trigger(poll_interval=0.01)

        poll_count = 0

        async def mock_check_completion(_):
            nonlocal poll_count
            poll_count += 1
            return None if poll_count < 3 else 0

        with (
            mock.patch.object(trigger, "_connect", return_value=mock.MagicMock()) as mock_connect,
            mock.patch.object(trigger, "_close", return_value=None) as mock_close,
            mock.patch.object(trigger, "_check_completion", side_effect=mock_check_completion),
            mock.patch.object(trigger, "_read_log_chunk", return_value=("output\n", 50)),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert poll_count == 3
        # One connect for the whole loop, closed once at teardown.
        assert mock_connect.call_count == 1
        assert mock_close.call_count == 1

    @pytest.mark.asyncio
    async def test_run_reconnects_on_connection_drop(self):
        """A connection-level error mid-poll drops the connection and reconnects."""
        trigger = _make_trigger(poll_interval=0.01, max_reconnect_attempts=3)

        calls = {"check": 0}

        async def flaky_check(_):
            calls["check"] += 1
            if calls["check"] == 1:
                raise OSError("Error reading SSH protocol banner")
            return 0

        with (
            mock.patch.object(trigger, "_connect", return_value=mock.MagicMock()) as mock_connect,
            mock.patch.object(trigger, "_close", return_value=None) as mock_close,
            mock.patch.object(trigger, "_check_completion", side_effect=flaky_check),
            mock.patch.object(trigger, "_read_log_chunk", return_value=("out\n", 10)),
            mock.patch("asyncio.sleep", new=mock.AsyncMock()),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        # Initial connect + one reconnect after the dropped poll.
        assert mock_connect.call_count == 2
        # Dropped connection closed during reconnect, plus final teardown close.
        assert mock_close.call_count == 2

    @pytest.mark.asyncio
    async def test_run_gives_up_after_max_reconnects(self):
        """When connections keep failing, the trigger emits a single error event."""
        trigger = _make_trigger(max_reconnect_attempts=2)

        with (
            mock.patch.object(trigger, "_connect", side_effect=OSError("connection refused")),
            mock.patch.object(trigger, "_close", return_value=None),
            mock.patch("asyncio.sleep", new=mock.AsyncMock()),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert events[0].payload["done"] is True
        assert events[0].payload["exit_code"] is None
        assert "connection refused" in events[0].payload["message"]

    @pytest.mark.asyncio
    async def test_run_gives_up_when_polls_keep_failing_despite_reconnects(self):
        """A connection that handshakes but whose polls keep failing must still hit the cap.

        Regression: the reconnect budget must not reset on a bare successful handshake, or a
        connection that reconnects fine but never completes a poll (e.g. ChannelOpenError under
        sshd MaxSessions) would loop forever and the task would defer indefinitely.
        """
        trigger = _make_trigger(max_reconnect_attempts=2)

        with (
            mock.patch.object(trigger, "_connect", return_value=mock.MagicMock()) as mock_connect,
            mock.patch.object(trigger, "_close", return_value=None),
            mock.patch.object(
                trigger, "_check_completion", side_effect=OSError("channel open failed")
            ) as mock_check,
            mock.patch("asyncio.sleep", new=mock.AsyncMock()),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert "channel open failed" in events[0].payload["message"]
        # Budget = 2 -> third consecutive failure ends it (handshake succeeds each round
        # but never resets the counter because no poll ever completes).
        assert mock_check.call_count == 3
        assert mock_connect.call_count == 3

    @pytest.mark.asyncio
    async def test_run_handles_unexpected_exception(self):
        """A non-connection error surfaces immediately as an error event."""
        trigger = _make_trigger()

        with (
            mock.patch.object(trigger, "_connect", return_value=mock.MagicMock()),
            mock.patch.object(trigger, "_close", return_value=None),
            mock.patch.object(trigger, "_check_completion", side_effect=ValueError("boom")),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert events[0].payload["done"] is True
        assert "boom" in events[0].payload["message"]

    def test_get_hook(self):
        """Test hook creation."""
        trigger = _make_trigger(remote_host="custom.host.com")

        hook = trigger._get_hook()
        assert hook.ssh_conn_id == "test_conn"
        assert hook.host == "custom.host.com"

    @pytest.mark.asyncio
    async def test_run_command_uses_existing_connection(self):
        """_run_command runs on the passed connection without opening a new one."""
        trigger = _make_trigger(command_timeout=12.0)

        result = mock.MagicMock()
        result.exit_status = 0
        result.stdout = "42"
        result.stderr = ""
        conn = mock.MagicMock()
        conn.run = mock.AsyncMock(return_value=result)

        exit_code, stdout, stderr = await trigger._run_command(conn, "echo 42")

        conn.run.assert_awaited_once_with("echo 42", timeout=12.0, check=False)
        assert (exit_code, stdout, stderr) == (0, "42", "")
