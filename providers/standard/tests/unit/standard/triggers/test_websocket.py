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
from websockets.asyncio.client import ClientConnection

from airflow.providers.standard.triggers.websocket import WebSocketTrigger


class _FakeConnection:
    """Stands in for the object returned by ``websockets.asyncio.client.connect``."""

    def __init__(self, websocket):
        self._websocket = websocket

    async def __aenter__(self):
        return self._websocket

    async def __aexit__(self, *exc_info):
        return False


class TestWebSocketTrigger:
    URL = "ws://example.com/socket"

    def test_serialization(self):
        """Asserts that the trigger correctly serializes its arguments and classpath."""
        trigger = WebSocketTrigger(url=self.URL, header={"Authorization": "token"}, message_to_send="ping")
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.websocket.WebSocketTrigger"
        assert kwargs == {
            "url": self.URL,
            "header": {"Authorization": "token"},
            "message_to_send": "ping",
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.standard.triggers.websocket.connect", autospec=True)
    async def test_run_yields_event_with_received_message(self, mock_connect):
        mock_websocket = mock.AsyncMock(spec=ClientConnection)
        mock_websocket.recv.return_value = "pong"
        mock_connect.return_value = _FakeConnection(mock_websocket)

        trigger = WebSocketTrigger(url=self.URL, header={"Authorization": "token"}, message_to_send="ping")
        event = await trigger.run().__anext__()

        mock_connect.assert_called_once_with(self.URL, additional_headers={"Authorization": "token"})
        mock_websocket.send.assert_awaited_once_with("ping")
        assert event.payload == "pong"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.standard.triggers.websocket.connect", autospec=True)
    async def test_run_does_not_send_without_message_to_send(self, mock_connect):
        mock_websocket = mock.AsyncMock(spec=ClientConnection)
        mock_websocket.recv.return_value = "pong"
        mock_connect.return_value = _FakeConnection(mock_websocket)

        trigger = WebSocketTrigger(url=self.URL)
        await trigger.run().__anext__()

        mock_websocket.send.assert_not_awaited()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.standard.triggers.websocket.connect", autospec=True)
    async def test_reconstructed_trigger_resends_message_to_send(self, mock_connect):
        """Documents that a triggerer restart or redistribution — which reconstructs the
        trigger from its serialize() output and calls run() again — re-sends
        message_to_send. Airflow does not guarantee a trigger's run() executes only
        once, so callers whose message starts a remote job must make that job
        idempotent; this is not something the trigger itself can enforce."""
        mock_websocket = mock.AsyncMock(spec=ClientConnection)
        mock_websocket.recv.return_value = "pong"
        mock_connect.return_value = _FakeConnection(mock_websocket)

        original = WebSocketTrigger(url=self.URL, message_to_send="start_job")
        _, kwargs = original.serialize()
        reconstructed = WebSocketTrigger(**kwargs)

        await original.run().__anext__()
        await reconstructed.run().__anext__()

        assert mock_websocket.send.await_args_list == [mock.call("start_job"), mock.call("start_job")]
