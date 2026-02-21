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

    def test_default_values(self):
        """Test default parameter values."""
        trigger = SSHRemoteJobTrigger(
            ssh_conn_id="test_conn",
            remote_host=None,
            job_id="test_job",
            job_dir="/tmp/job",
            log_file="/tmp/job/stdout.log",
            exit_code_file="/tmp/job/exit_code",
            remote_os="posix",
        )

        assert trigger.poll_interval == 5
        assert trigger.log_chunk_size == 65536
        assert trigger.log_offset == 0

    @pytest.mark.asyncio
    async def test_run_job_completed_success(self):
        """Test trigger when job completes successfully."""
        trigger = SSHRemoteJobTrigger(
            ssh_conn_id="test_conn",
            remote_host=None,
            job_id="test_job",
            job_dir="/tmp/job",
            log_file="/tmp/job/stdout.log",
            exit_code_file="/tmp/job/exit_code",
            remote_os="posix",
        )

        with mock.patch.object(trigger, "_check_completion", return_value=0):
            with mock.patch.object(trigger, "_read_log_chunk", return_value=("Final output\n", 100)):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "success"
                assert events[0].payload["done"] is True
                assert events[0].payload["exit_code"] == 0
                assert events[0].payload["log_chunk"] == "Final output\n"

    @pytest.mark.asyncio
    async def test_run_job_completed_failure(self):
        """Test trigger when job completes with failure."""
        trigger = SSHRemoteJobTrigger(
            ssh_conn_id="test_conn",
            remote_host=None,
            job_id="test_job",
            job_dir="/tmp/job",
            log_file="/tmp/job/stdout.log",
            exit_code_file="/tmp/job/exit_code",
            remote_os="posix",
        )

        with mock.patch.object(trigger, "_check_completion", return_value=1):
            with mock.patch.object(trigger, "_read_log_chunk", return_value=("Error output\n", 50)):
                events = []
                async for event in trigger.run():
                    events.append(event)

                assert len(events) == 1
                assert events[0].payload["status"] == "failed"
                assert events[0].payload["done"] is True
                assert events[0].payload["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_run_job_polls_until_completion(self):
        """Test trigger polls without yielding until job completes."""
        trigger = SSHRemoteJobTrigger(
            ssh_conn_id="test_conn",
            remote_host=None,
            job_id="test_job",
            job_dir="/tmp/job",
            log_file="/tmp/job/stdout.log",
            exit_code_file="/tmp/job/exit_code",
            remote_os="posix",
            poll_interval=0.01,
        )

        poll_count = 0

        async def mock_check_completion(_):
            nonlocal poll_count
            poll_count += 1
            # Return None (still running) for first 2 polls, then exit code 0
            if poll_count < 3:
                return None
            return 0

        with mock.patch.object(trigger, "_check_completion", side_effect=mock_check_completion):
            with mock.patch.object(trigger, "_read_log_chunk", return_value=("output\n", 50)):
                events = []
                async for event in trigger.run():
                    events.append(event)

                # Only one event should be yielded (the completion event)
                assert len(events) == 1
                assert events[0].payload["status"] == "success"
                assert events[0].payload["done"] is True
                assert events[0].payload["exit_code"] == 0
                # Should have polled 3 times
                assert poll_count == 3

    @pytest.mark.asyncio
    async def test_run_handles_exception(self):
        """Test trigger handles exceptions gracefully."""
        trigger = SSHRemoteJobTrigger(
            ssh_conn_id="test_conn",
            remote_host=None,
            job_id="test_job",
            job_dir="/tmp/job",
            log_file="/tmp/job/stdout.log",
            exit_code_file="/tmp/job/exit_code",
            remote_os="posix",
        )

        with mock.patch.object(trigger, "_check_completion", side_effect=Exception("Connection failed")):
            events = []
            async for event in trigger.run():
                events.append(event)

            assert len(events) == 1
            assert events[0].payload["status"] == "error"
            assert events[0].payload["done"] is True
            assert "Connection failed" in events[0].payload["message"]

    def test_get_hook(self):
        """Test hook creation."""
        trigger = SSHRemoteJobTrigger(
            ssh_conn_id="test_conn",
            remote_host="custom.host.com",
            job_id="test_job",
            job_dir="/tmp/job",
            log_file="/tmp/job/stdout.log",
            exit_code_file="/tmp/job/exit_code",
            remote_os="posix",
        )

        hook = trigger._get_hook()
        assert hook.ssh_conn_id == "test_conn"
        assert hook.host == "custom.host.com"
