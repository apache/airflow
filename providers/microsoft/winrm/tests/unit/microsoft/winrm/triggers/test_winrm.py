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

import asyncio
import base64
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.microsoft.winrm.triggers.winrm import WinRMCommandOutputTrigger
from airflow.triggers.base import TriggerEvent


class TestWinRMCommandOutputTrigger:
    def test_serialize(self):
        trigger = WinRMCommandOutputTrigger(
            ssh_conn_id="ssh_conn_id",
            shell_id="043E496C-A9E5-4284-AFCC-78A90E2BCB65",
            command_id="E4C36903-E59F-43AB-9374-ABA87509F46D",
            output_encoding="utf-8",
            return_output=True,
            working_directory="C:\\Temp",
            expected_return_code=0,
            poll_interval=10,
            timeout=None,
            deadline=None,
        )

        actual = trigger.serialize()

        assert isinstance(actual, tuple)
        assert actual[0] == f"{WinRMCommandOutputTrigger.__module__}.{WinRMCommandOutputTrigger.__name__}"
        assert actual[1] == {
            "ssh_conn_id": "ssh_conn_id",
            "shell_id": "043E496C-A9E5-4284-AFCC-78A90E2BCB65",
            "command_id": "E4C36903-E59F-43AB-9374-ABA87509F46D",
            "output_encoding": "utf-8",
            "return_output": True,
            "working_directory": "C:\\Temp",
            "expected_return_code": 0,
            "poll_interval": 10,
            "timeout": None,
            "deadline": None,
        }

    @pytest.mark.parametrize(
        ("monotonic_value", "timeout", "deadline", "expected"),
        [
            (None, None, None, False),
            (200.0, None, 100.0, True),
        ],
    )
    def test_is_expired(self, monotonic_value, timeout, deadline, expected):
        with (
            patch(
                "airflow.providers.microsoft.winrm.triggers.winrm.time.monotonic",
                return_value=monotonic_value,
            )
            if monotonic_value is not None
            else nullcontext()
        ):
            trigger = WinRMCommandOutputTrigger(
                ssh_conn_id="ssh_conn_id",
                shell_id="043E496C-A9E5-4284-AFCC-78A90E2BCB65",
                command_id="E4C36903-E59F-43AB-9374-ABA87509F46D",
                timeout=timeout,
                deadline=deadline,
            )

            assert trigger.is_expired is expected

    @pytest.mark.asyncio
    async def test_run(self):
        trigger = WinRMCommandOutputTrigger(
            ssh_conn_id="ssh_conn_id",
            shell_id="043E496C-A9E5-4284-AFCC-78A90E2BCB65",
            command_id="E4C36903-E59F-43AB-9374-ABA87509F46D",
            poll_interval=1,
            timeout=10,
        )

        mock_conn = MagicMock()
        mock_conn.get_command_output_raw.return_value = (
            b"hello",
            b"",
            0,
            True,
        )
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with (
            patch.object(trigger, "hook", mock_hook),
            patch.object(asyncio, "sleep", return_value=None),
        ):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, TriggerEvent)

        payload = event.payload
        assert payload["status"] == "success"
        assert payload["shell_id"] == "043E496C-A9E5-4284-AFCC-78A90E2BCB65"
        assert payload["command_id"] == "E4C36903-E59F-43AB-9374-ABA87509F46D"
        assert payload["return_code"] == 0
        assert base64.b64decode(payload["stdout"]) == b"hello"
        assert base64.b64decode(payload["stderr"]) == b""
