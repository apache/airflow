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
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any

from asgiref.sync import sync_to_async

from airflow.providers.common.compat.connection import get_async_connection
from airflow.sdk.bases.hook import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection


class IBMMQHook(BaseHook):
    conn_name_attr = "conn_id"
    default_conn_name = "mq_default"
    conn_type = "mq"
    hook_name = "IBM MQ"

    def __init__(self, conn_id: str = default_conn_name):
        super().__init__()
        self.conn_id = conn_id
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
                    },
                    indent=2,
                ),
            },
        }

    @classmethod
    def _connect(cls, conn: Connection):
        import ibmmq

        csp = ibmmq.CSP()
        csp.CSPUserId = conn.login
        csp.CSPPassword = conn.password

        config = conn.extra_dejson

        return ibmmq.connect(
            config["queue_manager"],
            config["channel"],
            f"{conn.host}({conn.port})",
            csp=csp,
        )

    @asynccontextmanager
    async def get_conn(self):
        connection = await get_async_connection(conn_id=self.conn_id)
        conn = None
        try:
            conn = self._connect(connection)
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

    async def consume(self, queue_name: str, poll_interval: float = 5) -> str | None:
        """
        Wait for a single message from the specified IBM MQ queue and return its decoded payload.

        If the MQ connection is lost or another recoverable error occurs, the method logs the
        issue and exits so that the next trigger instance can attempt to reconnect.

        :param queue_name: Name of the IBM MQ queue to consume messages from.
        :param poll_interval: Interval in seconds used to wait for messages and to control
            how long the underlying MQ get operation blocks before checking again.
        :return: The decoded message payload if a message is received, otherwise ``None``.
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
            async with self.get_conn() as qmgr:
                q = ibmmq.Queue(
                    qmgr,
                    od,
                    ibmmq.CMQC.MQOO_INPUT_AS_Q_DEF,
                )

                async_get = sync_to_async(q.get)

                try:
                    while True:
                        try:
                            message = await asyncio.wait_for(
                                async_get(None, md, gmo),
                                timeout=poll_interval + 1,
                            )

                            if message:
                                return self._process_message(message)

                        except ibmmq.MQMIError as e:
                            if e.reason == ibmmq.CMQC.MQRC_NO_MSG_AVAILABLE:
                                self.log.debug("No message available...")
                                await asyncio.sleep(poll_interval)
                                continue
                            if e.reason == ibmmq.CMQC.MQRC_CONNECTION_BROKEN:
                                self.log.warning(
                                    "MQ connection broken, will exit consume; next trigger instance will reconnect"
                                )
                                return None
                            raise

                finally:
                    with suppress(Exception):
                        q.close()

        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("MQ consume failed, exiting; next trigger instance will retry")
            return None

    async def produce(self, queue_name: str, payload: str) -> None:
        """
        Put a message onto the specified IBM MQ queue.

        This method connects to the configured MQ queue manager and sends the
        provided payload as a UTF-8 encoded message to the given queue.

        :param queue_name: Name of the IBM MQ queue to which the message should be sent.
        :param payload: Message payload to send. The payload will be encoded as UTF-8
            before being placed on the queue.
        :return: None
        """
        import ibmmq

        od = ibmmq.OD()
        od.ObjectName = queue_name

        md = ibmmq.MD()
        md.Format = ibmmq.CMQC.MQFMT_STRING
        md.CodedCharSetId = 1208
        md.Encoding = ibmmq.CMQC.MQENC_NATIVE

        async with self.get_conn() as qmgr:
            q = ibmmq.Queue(
                qmgr,
                od,
                ibmmq.CMQC.MQOO_OUTPUT,
            )

            async_put = sync_to_async(q.put)

            try:
                await async_put(payload.encode("utf-8"), md)
            finally:
                with suppress(Exception):
                    q.close()
