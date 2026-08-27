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

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from websockets.sync.client import ClientConnection, connect

from airflow.providers.common.compat.sdk import BaseSensorOperator, conf, poke_mode_only
from airflow.providers.standard.triggers.websocket import WebSocketTrigger

if TYPE_CHECKING:
    from airflow.sdk import Context


@poke_mode_only
class WebSocketSensor(BaseSensorOperator):
    """
    Waits for a message on a WebSocket connection.

    WebSocket messages are consumptive: once read, a message cannot be read again, and
    reconnecting can duplicate ``message_to_send`` against the remote server. The
    non-deferrable path therefore keeps a single connection open across pokes instead of
    reconnecting and re-sending on every poke, and this sensor will not behave correctly in
    reschedule mode, since that state would be lost between rescheduled invocations.

    :param url: The ``ws://`` or ``wss://`` URL of the WebSocket server to connect to.
    :param header: Optional headers sent when opening the connection.
    :param message_to_send: Optional message sent right after the connection is established.
    :param deferrable: If waiting for completion, whether to defer the task until done,
        default is ``False``.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:WebSocketSensor`
    """

    template_fields: Sequence[str] = ("url",)

    def __init__(
        self,
        *,
        url: str,
        header: dict[str, str] | None = None,
        message_to_send: str | bytes | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.url = url
        self.header = header
        self.message_to_send = message_to_send
        self.deferrable = deferrable
        self._connection: ClientConnection | None = None

    def poke(self, context: Context) -> bool:
        if self._connection is None:
            self.log.info("Connecting to WebSocket %s", self.url)
            self._connection = connect(self.url, additional_headers=self.header)
            if self.message_to_send is not None:
                self._connection.send(self.message_to_send)
        try:
            self._connection.recv(timeout=self.poke_interval)
        except TimeoutError:
            return False
        self.log.info("Received message from %s", self.url)
        self._connection.close()
        self._connection = None
        return True

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            try:
                super().execute(context=context)
            finally:
                if self._connection is not None:
                    self._connection.close()
                    self._connection = None
            return
        # Each poke opens and consumes a WebSocket connection, so the deferrable path must
        # defer immediately: polling here first would send message_to_send and consume the
        # reply before handing off, leaving the trigger to open a second connection and
        # re-send the request.
        self.defer(
            timeout=datetime.timedelta(seconds=self.timeout),
            trigger=WebSocketTrigger(
                url=self.url,
                header=self.header,
                message_to_send=self.message_to_send,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """Handle the event when the trigger fires and return immediately."""
        self.log.info("%s completed successfully with message: %s", self.task_id, event)
