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

from collections.abc import AsyncIterator
from typing import Any

from websockets.asyncio.client import connect

from airflow.triggers.base import BaseTrigger, TriggerEvent


class WebSocketTrigger(BaseTrigger):
    """
    A trigger that opens a WebSocket connection and fires once a message is received.

    This is meant for deferrable operators that hand off a long-lived request to a remote
    WebSocket server and resume once that server replies, without occupying a worker slot
    while waiting.

    :param url: The ``ws://`` or ``wss://`` URL of the WebSocket server to connect to.
    :param header: Optional headers sent when opening the connection.
    :param message_to_send: Optional message sent right after the connection is established.
    """

    def __init__(
        self,
        url: str,
        header: dict[str, str] | None = None,
        message_to_send: str | bytes | None = None,
        **kwargs,
    ):
        super().__init__()
        self.url = url
        self.header = header
        self.message_to_send = message_to_send

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize WebSocketTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.websocket.WebSocketTrigger",
            {
                "url": self.url,
                "header": self.header,
                "message_to_send": self.message_to_send,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Connect to the WebSocket server and wait for the first message."""
        async with connect(self.url, additional_headers=self.header) as websocket:
            if self.message_to_send is not None:
                await websocket.send(self.message_to_send)
            message = await websocket.recv()
            self.log.info("Received message from %s", self.url)
            yield TriggerEvent(message)
