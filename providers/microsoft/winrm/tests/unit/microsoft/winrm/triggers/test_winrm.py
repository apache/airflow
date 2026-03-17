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
from unittest.mock import AsyncMock, MagicMock, patch

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
            poll_interval=10,
            max_output_chunks=100,
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
            "poll_interval": 10,
            "max_output_chunks": 100,
        }

    @pytest.mark.asyncio
    @patch("airflow.providers.microsoft.winrm.triggers.winrm.WinRMHook")
    async def test_run(self, mock_hook):
        trigger = WinRMCommandOutputTrigger(
            ssh_conn_id="ssh_conn_id",
            shell_id="043E496C-A9E5-4284-AFCC-78B5717DF4D73",
            command_id="78CE100B-04FD-4EE2-8DAF-0751795661BB",
            poll_interval=1,
        )

        mock_hook_instance = MagicMock()
        mock_hook.return_value = mock_hook_instance
        mock_conn = MagicMock()
        mock_hook_instance.get_async_conn = AsyncMock(return_value=mock_conn)
        mock_hook_instance.get_conn.return_value = mock_conn
        mock_hook_instance.get_command_output.return_value = (b"hello", b"", 0, True)

        with patch.object(asyncio, "sleep", return_value=None):
            events = [event async for event in trigger.run()]

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, TriggerEvent)

        payload = event.payload
        assert payload["status"] == "success"
        assert payload["shell_id"] == "043E496C-A9E5-4284-AFCC-78B5717DF4D73"
        assert payload["command_id"] == "78CE100B-04FD-4EE2-8DAF-0751795661BB"
        assert payload["return_code"] == 0
        assert base64.b64decode(payload["stdout"][0]) == b"hello"
        assert not payload["stderr"]
