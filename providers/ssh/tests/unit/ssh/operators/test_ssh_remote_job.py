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

from airflow.exceptions import TaskDeferred
from airflow.providers.common.compat.sdk import AirflowException, AirflowSkipException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh_remote_job import SSHRemoteJobOperator
from airflow.providers.ssh.triggers.ssh_remote_job import SSHRemoteJobTrigger


class TestSSHRemoteJobOperator:
    @pytest.fixture(autouse=True)
    def mock_ssh_hook(self):
        """Mock the SSHHook to avoid connection lookup."""
        with mock.patch.object(
            SSHRemoteJobOperator, "ssh_hook", new_callable=mock.PropertyMock
        ) as mock_hook_prop:
            mock_hook = mock.create_autospec(SSHHook, instance=True)
            mock_hook.remote_host = "test.host.com"
            mock_ssh_client = mock.MagicMock()
            mock_hook.get_conn.return_value.__enter__.return_value = mock_ssh_client
            mock_hook.get_conn.return_value.__exit__.return_value = None
            mock_hook.exec_ssh_client_command.return_value = (0, b"", b"")
            mock_hook_prop.return_value = mock_hook
            self.mock_hook = mock_hook
            self.mock_hook_prop = mock_hook_prop
            yield

    def test_init_default_values(self):
        """Test operator initialization with default values."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )
        assert op.ssh_conn_id == "test_conn"
        assert op.command == "/path/to/script.sh"
        assert op.poll_interval == 5
        assert op.log_chunk_size == 65536
        assert op.cleanup == "never"
        assert op.remote_os == "auto"

    def test_init_custom_values(self):
        """Test operator initialization with custom values."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
            remote_host="custom.host.com",
            poll_interval=10,
            log_chunk_size=32768,
            timeout=3600,
            cleanup="on_success",
            remote_os="posix",
            skip_on_exit_code=[42, 43],
        )
        assert op.remote_host == "custom.host.com"
        assert op.poll_interval == 10
        assert op.log_chunk_size == 32768
        assert op.timeout == 3600
        assert op.cleanup == "on_success"
        assert op.remote_os == "posix"
        assert 42 in op.skip_on_exit_code
        assert 43 in op.skip_on_exit_code

    def test_template_fields(self):
        """Test that template fields are defined correctly."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )
        assert "command" in op.template_fields
        assert "environment" in op.template_fields
        assert "remote_host" in op.template_fields
        assert "remote_base_dir" in op.template_fields

    def test_execute_defers_to_trigger(self):
        """Test that execute submits job and defers to trigger."""
        self.mock_hook.exec_ssh_client_command.return_value = (
            0,
            b"af_test_dag_test_task_run1_try1_abc123",
            b"",
        )

        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
            remote_os="posix",
        )

        mock_ti = mock.MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.run_id = "run1"
        mock_ti.try_number = 1
        context = {"ti": mock_ti}

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context)

        assert isinstance(exc_info.value.trigger, SSHRemoteJobTrigger)
        assert exc_info.value.method_name == "execute_complete"

    def test_execute_raises_if_no_command(self):
        """Test that execute raises if command is not specified."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="",
        )
        # Set command to empty after init
        op.command = ""

        with pytest.raises(AirflowException, match="command not specified"):
            op.execute({})

    @mock.patch.object(SSHRemoteJobOperator, "defer")
    def test_execute_complete_re_defers_if_not_done(self, mock_defer):
        """Test that execute_complete re-defers if job is not done."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )

        event = {
            "done": False,
            "status": "running",
            "job_id": "test_job_123",
            "job_dir": "/tmp/airflow-ssh-jobs/test_job_123",
            "log_file": "/tmp/airflow-ssh-jobs/test_job_123/stdout.log",
            "exit_code_file": "/tmp/airflow-ssh-jobs/test_job_123/exit_code",
            "remote_os": "posix",
            "log_chunk": "Some output\n",
            "log_offset": 100,
            "exit_code": None,
        }

        op.execute_complete({}, event)

        mock_defer.assert_called_once()
        call_kwargs = mock_defer.call_args[1]
        assert isinstance(call_kwargs["trigger"], SSHRemoteJobTrigger)
        assert call_kwargs["trigger"].log_offset == 100

    def test_execute_complete_success(self):
        """Test execute_complete with successful completion."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )

        event = {
            "done": True,
            "status": "success",
            "exit_code": 0,
            "job_id": "test_job_123",
            "job_dir": "/tmp/airflow-ssh-jobs/test_job_123",
            "log_file": "/tmp/airflow-ssh-jobs/test_job_123/stdout.log",
            "exit_code_file": "/tmp/airflow-ssh-jobs/test_job_123/exit_code",
            "log_chunk": "Final output\n",
            "log_offset": 200,
            "remote_os": "posix",
        }

        # Should complete without exception
        op.execute_complete({}, event)

    def test_execute_complete_failure(self):
        """Test execute_complete with non-zero exit code."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )

        event = {
            "done": True,
            "status": "failed",
            "exit_code": 1,
            "job_id": "test_job_123",
            "job_dir": "/tmp/airflow-ssh-jobs/test_job_123",
            "log_file": "/tmp/airflow-ssh-jobs/test_job_123/stdout.log",
            "exit_code_file": "/tmp/airflow-ssh-jobs/test_job_123/exit_code",
            "log_chunk": "Error output\n",
            "log_offset": 200,
            "remote_os": "posix",
        }

        with pytest.raises(AirflowException, match="exit code: 1"):
            op.execute_complete({}, event)

    def test_execute_complete_skip_on_exit_code(self):
        """Test execute_complete skips on configured exit code."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
            skip_on_exit_code=42,
        )

        event = {
            "done": True,
            "status": "failed",
            "exit_code": 42,
            "job_id": "test_job_123",
            "job_dir": "/tmp/airflow-ssh-jobs/test_job_123",
            "log_file": "/tmp/airflow-ssh-jobs/test_job_123/stdout.log",
            "exit_code_file": "/tmp/airflow-ssh-jobs/test_job_123/exit_code",
            "log_chunk": "",
            "log_offset": 0,
            "remote_os": "posix",
        }

        with pytest.raises(AirflowSkipException):
            op.execute_complete({}, event)

    def test_execute_complete_with_cleanup(self):
        """Test execute_complete performs cleanup when configured."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
            cleanup="on_success",
        )

        event = {
            "done": True,
            "status": "success",
            "exit_code": 0,
            "job_id": "test_job_123",
            "job_dir": "/tmp/airflow-ssh-jobs/test_job_123",
            "log_file": "/tmp/airflow-ssh-jobs/test_job_123/stdout.log",
            "exit_code_file": "/tmp/airflow-ssh-jobs/test_job_123/exit_code",
            "log_chunk": "",
            "log_offset": 0,
            "remote_os": "posix",
        }

        op.execute_complete({}, event)

        # Verify cleanup command was executed
        self.mock_hook.exec_ssh_client_command.assert_called_once()
        call_args = self.mock_hook.exec_ssh_client_command.call_args
        assert "rm -rf" in call_args[0][1]

    def test_on_kill(self):
        """Test on_kill attempts to kill remote process."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )

        # Simulate that execute was called
        op._job_id = "test_job_123"
        op._detected_os = "posix"
        from airflow.providers.ssh.utils.remote_job import RemoteJobPaths

        op._paths = RemoteJobPaths(job_id="test_job_123", remote_os="posix")

        op.on_kill()

        # Verify kill command was executed
        self.mock_hook.exec_ssh_client_command.assert_called_once()
        call_args = self.mock_hook.exec_ssh_client_command.call_args
        assert "kill" in call_args[0][1]

    def test_on_kill_after_rehydration(self):
        """Test on_kill retrieves job info from XCom after operator rehydration."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )

        # Instance variables are None (simulating rehydration)
        # Don't set _job_id, _paths, _detected_os

        # Mock task_instance with XCom data
        mock_ti = mock.MagicMock()
        mock_ti.xcom_pull.return_value = {
            "job_id": "test_job_123",
            "pid_file": "/tmp/airflow-ssh-jobs/test_job_123/pid",
            "remote_os": "posix",
        }
        op.task_instance = mock_ti

        op.on_kill()

        # Verify XCom was called to get job info
        mock_ti.xcom_pull.assert_called_once_with(key="ssh_remote_job")

        # Verify kill command was executed
        self.mock_hook.exec_ssh_client_command.assert_called_once()
        call_args = self.mock_hook.exec_ssh_client_command.call_args
        assert "kill" in call_args[0][1]

    def test_on_kill_no_active_job(self):
        """Test on_kill does nothing if no active job."""
        op = SSHRemoteJobOperator(
            task_id="test_task",
            ssh_conn_id="test_conn",
            command="/path/to/script.sh",
        )

        # Should not raise even without active job
        op.on_kill()
