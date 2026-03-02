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

from base64 import b64encode
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from airflow.triggers.base import TriggerEvent

from tests_common.test_utils.operators.run_deferrable import execute_operator


class TestWinRMOperator:
    def test_no_winrm_hook_no_ssh_conn_id(self):
        op = WinRMOperator(task_id="test_task_id", winrm_hook=None, ssh_conn_id=None, command="not_empty")
        exception_msg = "Cannot operate without winrm_hook."
        with pytest.raises(AirflowException, match=exception_msg):
            op.execute(None)

    def test_no_command(self):
        winrm_hook = WinRMHook(transport="ntml", remote_host="localhost", password="secret")
        op = WinRMOperator(task_id="test_task_id", winrm_hook=winrm_hook, command=None)
        exception_msg = "No command specified so nothing to execute here."
        with pytest.raises(AirflowException, match=exception_msg):
            op.execute(None)

    @mock.patch("airflow.providers.microsoft.winrm.operators.winrm.WinRMHook")
    def test_default_returning_0_command(self, mock_hook):
        stdout = [b"O", b"K"]
        command = "not_empty"
        working_dir = "c:\\temp"
        mock_hook.run.return_value = (0, stdout, [])
        op = WinRMOperator(
            task_id="test_task_id", winrm_hook=mock_hook, command=command, working_directory=working_dir
        )
        execute_result = op.execute(None)
        assert execute_result == b64encode(b"".join(stdout)).decode("utf-8")
        mock_hook.run.assert_called_once_with(
            command=command,
            ps_path=None,
            output_encoding="utf-8",
            return_output=True,
            working_directory=working_dir,
        )

    @mock.patch("airflow.providers.microsoft.winrm.operators.winrm.WinRMHook")
    def test_default_returning_1_command(self, mock_hook):
        stderr = [b"K", b"O"]
        command = "not_empty"
        mock_hook.run.return_value = (1, [], stderr)
        op = WinRMOperator(task_id="test_task_id", winrm_hook=mock_hook, command=command)
        exception_msg = f"Error running cmd: {command}, return code: 1, error: KO"
        with pytest.raises(AirflowException, match=exception_msg):
            op.execute(None)

    @mock.patch("airflow.providers.microsoft.winrm.operators.winrm.WinRMHook")
    @pytest.mark.parametrize("expected_return_code", [1, [1, 2], range(1, 3)])
    @pytest.mark.parametrize("real_return_code", [0, 1, 2])
    def test_expected_return_code_command(self, mock_hook, expected_return_code, real_return_code):
        stdout = [b"O", b"K"]
        stderr = [b"K", b"O"]
        command = "not_empty"
        mock_hook.run.return_value = (real_return_code, stdout, stderr)
        op = WinRMOperator(
            task_id="test_task_id",
            winrm_hook=mock_hook,
            command=command,
            expected_return_code=expected_return_code,
        )

        should_task_succeed = op.validate_return_code(real_return_code)

        if should_task_succeed:
            execute_result = op.execute(None)
            assert execute_result == b64encode(b"".join(stdout)).decode("utf-8")
            mock_hook.run.assert_called_once_with(
                command=command,
                ps_path=None,
                output_encoding="utf-8",
                return_output=True,
                working_directory=None,
            )
        else:
            exception_msg = f"Error running cmd: {command}, return code: {real_return_code}, error: KO"
            with pytest.raises(AirflowException, match=exception_msg):
                op.execute(None)

    @mock.patch("airflow.providers.microsoft.winrm.operators.winrm.WinRMHook")
    @mock.patch("airflow.providers.microsoft.winrm.triggers.winrm.WinRMHook")
    def test_execute_deferrable_success(self, mock_operator_hook, mock_trigger_hook):
        mock_hook_instance = MagicMock(spec=WinRMHook)
        mock_hook_instance.ssh_conn_id = "winrm_default"
        mock_operator_hook.return_value = mock_hook_instance
        mock_trigger_hook.return_value = mock_hook_instance
        mock_conn = MagicMock()
        mock_hook_instance.get_async_conn = AsyncMock(return_value=mock_conn)
        mock_hook_instance.get_conn.return_value = mock_conn
        mock_hook_instance.get_command_output.return_value = (b"hello", b"", 0, True)
        mock_hook_instance.run_command.return_value = (
            "043E496C-A9E5-4284-AFCC-78A90E2BCB65",
            "E4C36903-E59F-43AB-9374-ABA87509F46D",
        )

        operator = WinRMOperator(
            task_id="test_task",
            winrm_hook=mock_hook_instance,
            command="dir",
            deferrable=True,
        )

        result, events = execute_operator(operator)

        assert len(events) == 1
        assert isinstance(events[0], TriggerEvent)
        assert events[0].payload == {
            "command_id": "E4C36903-E59F-43AB-9374-ABA87509F46D",
            "return_code": 0,
            "shell_id": "043E496C-A9E5-4284-AFCC-78A90E2BCB65",
            "status": "success",
            "stderr": [],
            "stdout": ["aGVsbG8="],
        }
        assert result == "aGVsbG8="
