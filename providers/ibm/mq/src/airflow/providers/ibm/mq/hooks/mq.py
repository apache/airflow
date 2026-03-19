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

import asyncio
import json
import threading
from contextlib import contextmanager, suppress
from typing import Any

from asgiref.sync import sync_to_async

from airflow.sdk.bases.hook import BaseHook

# Backoff parameters for transient consume failures
_BACKOFF_BASE: float = 1.0
_BACKOFF_MAX: float = 60.0
_BACKOFF_FACTOR: float = 2.0


class IBMMQHook(BaseHook):
    conn_name_attr = "conn_id"
    default_conn_name = "mq_default"
    conn_type = "mq"
    hook_name = "IBM MQ"
    default_open_options = "MQOO_INPUT_SHARED"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        queue_manager: str | None = None,
        channel: str | None = None,
        open_options: int | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.queue_manager = queue_manager
        self.channel = channel
        self.open_options = open_options
        self._conn = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for IBM MQ Connection."""
        return {
            "hidden_fields": ["schema"],
            "placeholders": {
                "host": "mq.example.com",
                "port": "1414",
                "login": "app_user",
                "extra": json.dumps(
                    {
                        "queue_manager": "QM1",
                        "channel": "DEV.APP.SVRCONN",
                        "open_options": cls.default_open_options,
                    },
                    indent=2,
                ),
            },
        }

    @classmethod
    def get_open_options_flags(cls, open_options: int) -> list[str]:
        """
        Return the symbolic MQ open option flags set in a given bitmask.

        Each flag corresponds to a constant in 'ibmmq.CMQC' that starts with 'MQOO_'.

        :param open_options: The integer bitmask used when opening an MQ queue
                             (e.g., 'MQOO_INPUT_EXCLUSIVE | MQOO_FAIL_IF_QUIESCING').

        :return: A list of the names of the MQ open flags that are set in the bitmask.
                 For example, '['MQOO_INPUT_EXCLUSIVE', 'MQOO_FAIL_IF_QUIESCING']'.

        Example:
            >>> open_options = ibmmq.CMQC.MQOO_INPUT_SHARED | ibmmq.CMQC.MQOO_FAIL_IF_QUIESCING
            >>> cls.get_open_options_flags(open_options)
            ['MQOO_INPUT_SHARED', 'MQOO_FAIL_IF_QUIESCING']
        """
        import ibmmq

        return [
            name
            for name, value in vars(ibmmq.CMQC).items()
            if name.startswith("MQOO_") and (open_options & value)
        ]

    @contextmanager
    def get_conn(self):
        """
        Sync context manager for IBM MQ connection lifecycle.

        Must be called from the executor thread (not the event loop thread).
        Retrieves the Airflow connection, extracts MQ parameters, and manages
        the IBM MQ connection lifecycle.

        :yield: IBM MQ connection object
        """
        import ibmmq

        connection = BaseHook.get_connection(self.conn_id)
        config = connection.extra_dejson
        queue_manager = self.queue_manager or config.get("queue_manager")
        channel = self.channel or config.get("channel")

        if not queue_manager:
            raise ValueError("queue_manager must be set in Connection extra config or hook init")
        if not channel:
            raise ValueError("channel must be set in Connection extra config or hook init")

        self.open_options = self.open_options or getattr(
            ibmmq.CMQC,
            config.get("open_options", self.default_open_options),
            ibmmq.CMQC.MQOO_INPUT_EXCLUSIVE,
        )

        csp = ibmmq.CSP()
        csp.CSPUserId = connection.login
        csp.CSPPassword = connection.password

        conn = None
        try:
            conn = ibmmq.connect(
                queue_manager,
                channel,
                f"{connection.host}({connection.port})",
                csp=csp,
            )
            yield conn
        finally:
            if conn:
                with suppress(Exception):
                    conn.disconnect()

    def _process_message(self, message: bytes) -> str:
        """
        Process a raw MQ message.

        If the message contains an RFH2 header, the header is unpacked and the
        payload following the header is returned. If unpacking fails, the raw
        message is returned decoded as UTF-8.

        :param message: Raw message received from IBM MQ.
        :return: Decoded message payload.
        """
        import ibmmq

        try:
            rfh2 = ibmmq.RFH2()
            rfh2.unpack(message)

            payload_offset = rfh2.get_length()
            payload = message[payload_offset:]

            decoded = payload.decode("utf-8", errors="ignore")
            self.log.info("Message received from MQ (RFH2 decoded): %s", decoded)
            return decoded
        except ibmmq.PYIFError as error:  # RFH2 header not present or unpack failed
            self.log.warning(
                "Failed to unpack RFH2 header (%s). Returning raw message payload: %s", error, message
            )
            return message.decode("utf-8", errors="ignore")

    async def consume(self, queue_name: str, poll_interval: float = 5) -> str:
        """
        Wait for a single message from the specified IBM MQ queue and return its decoded payload.

        The method retries with exponential back-off whenever the underlying
        ``_consume_sync`` returns ``None`` (connection broken, timeout) or raises
        an unexpected exception, so that an AssetWatcher is never silently killed
        by a transient failure.

        All blocking IBM MQ operations ('connect', 'open', 'get', 'close', 'disconnect') run in a
        separate thread via 'sync_to_async' to satisfy the IBM MQ C client's thread-affinity
        requirement — every operation on a connection must happen from the thread that created it.

        A :class:`threading.Event` stop signal is passed to the worker so that, when this
        coroutine is canceled (e.g. because the Airflow triggerer reassigns the watcher to
        another pod), the background thread exits cleanly after the current 'q.get' call
        times out (at most 'poll_interval' seconds).  Without this, an orphaned thread could
        silently consume a message after cancellation, causing the event to be lost and the
        DAG never to run.

        :param queue_name: Name of the IBM MQ queue to consume messages from.
        :param poll_interval: Interval in seconds used to wait for messages and to control
            how long the underlying MQ 'get' operation blocks before checking again.
        :return: The decoded message payload.
        """
        backoff = _BACKOFF_BASE
        while True:
            stop_event = threading.Event()
            try:
                result = await sync_to_async(self._consume_sync)(
                    queue_name, poll_interval, stop_event
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.warning(
                    "IBM MQ consume encountered an error for queue '%s'; retrying in %.1fs",
                    queue_name,
                    backoff,
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)
                continue
            finally:
                stop_event.set()

            if result is not None:
                return result

            self.log.warning(
                "IBM MQ consume returned no event for queue '%s'; retrying in %.1fs",
                queue_name,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)

    def _consume_sync(
        self,
        queue_name: str,
        poll_interval: float,
        stop_event: threading.Event,
    ) -> str | None:
        """
        Blocking implementation of :meth:`consume` — must be called from a single thread.

        All IBM MQ handles (queue manager connection, queue) are created **and used** within
        this method, satisfying the thread-affinity requirement of the IBM MQ C client library.
        The 'stop_event' is checked between 'q.get' calls so the thread terminates promptly
        after the coroutine side is canceled.
        """
        import ibmmq

        od = ibmmq.OD()
        od.ObjectName = queue_name

        md = ibmmq.MD()
        md.Format = ibmmq.CMQC.MQFMT_STRING
        md.CodedCharSetId = 1208
        md.Encoding = ibmmq.CMQC.MQENC_NATIVE

        gmo = ibmmq.GMO()
        gmo.Options = ibmmq.CMQC.MQGMO_WAIT | ibmmq.CMQC.MQGMO_NO_SYNCPOINT | ibmmq.CMQC.MQGMO_CONVERT
        gmo.WaitInterval = int(poll_interval * 1000)

        try:
            with self.get_conn() as conn:
                if self.open_options is not None:
                    flag_names = self.get_open_options_flags(self.open_options)
                    self.log.info(
                        "Opening MQ queue '%s' with open_options=%s (%s)",
                        queue_name,
                        self.open_options,
                        ", ".join(flag_names),
                    )

                q = ibmmq.Queue(conn, od, self.open_options)
                try:
                    # WaitInterval already blocks for poll_interval seconds when no message is
                    # available, so no additional sleep is needed between iterations.
                    while not stop_event.is_set():
                        try:
                            message = q.get(None, md, gmo)
                            if message:
                                return self._process_message(message)
                        except ibmmq.MQMIError as e:
                            if e.reason == ibmmq.CMQC.MQRC_NO_MSG_AVAILABLE:
                                self.log.debug("No message available...")
                                continue
                            if e.reason == ibmmq.CMQC.MQRC_CONNECTION_BROKEN:
                                self.log.warning(
                                    "MQ connection broken on queue '%s'; will reconnect",
                                    queue_name,
                                )
                                return None
                            self.log.error(
                                "IBM MQ error on queue '%s': completion_code=%s reason_code=%s",
                                queue_name,
                                e.comp,
                                e.reason,
                            )
                            raise
                finally:
                    with suppress(Exception):
                        q.close()
        except (ibmmq.MQMIError, ibmmq.PYIFError):
            self.log.exception(
                "MQ consume failed on queue '%s'",
                queue_name,
            )
            return None
        return None

    async def produce(self, queue_name: str, payload: str) -> None:
        """
        Put a message onto the specified IBM MQ queue.

        All blocking IBM MQ operations run in a separate thread via 'sync_to_async' for the same
        thread-safety reasons as :meth:`consume`.

        :param queue_name: Name of the IBM MQ queue to which the message should be sent.
        :param payload: Message payload to send. The payload will be encoded as UTF-8
            before being placed on the queue.
        :return: None
        """
        await sync_to_async(self._produce_sync)(queue_name, payload)

    def _produce_sync(
        self,
        queue_name: str,
        payload: str,
    ) -> None:
        """
        Blocking implementation of :meth:`produce` — must be called from a single thread.
        """
        import ibmmq

        od = ibmmq.OD()
        od.ObjectName = queue_name

        md = ibmmq.MD()
        md.Format = ibmmq.CMQC.MQFMT_STRING
        md.CodedCharSetId = 1208
        md.Encoding = ibmmq.CMQC.MQENC_NATIVE

        try:
            with self.get_conn() as conn:
                q = ibmmq.Queue(conn, od, ibmmq.CMQC.MQOO_OUTPUT)
                try:
                    q.put(payload.encode("utf-8"), md)
                finally:
                    with suppress(Exception):
                        q.close()
        except Exception:
            self.log.exception(
                "MQ produce failed on queue '%s'",
                queue_name,
            )
            raise
