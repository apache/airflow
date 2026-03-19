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
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.ibm.mq.hooks.mq import IBMMQHook

MQ_PAYLOAD = """RFH x"MQSTR    <mcd><Msd>jms_map</Msd></mcd>   <jms><Dst>topic://localhost/topic</Dst><Tms>1772121947476</Tms><Dlv>2</Dlv><Uci dt='bin.hex'>414D5143514D4941303054202020202069774D7092F81057</Uci></jms>L<usr><XMSC_CLIENT_ID>local</XMSC_CLIENT_ID><release>26.01.00</release></usr> 4<mqps><Top>topic</Top></mqps>  {}"""


def mq_connection():
    """Create a test MQ connection object."""
    return Connection(
        conn_id="mq_conn",
        conn_type="mq",
        host="mq.example.com",
        login="user",
        password="pass",
        port=1414,
        extra='{"queue_manager": "QM1", "channel": "DEV.APP.SVRCONN"}',
    )


@pytest.fixture
def mock_get_connection():
    """Fixture that mocks BaseHook.get_connection to return a test connection."""
    with patch("airflow.providers.ibm.mq.hooks.mq.BaseHook.get_connection") as mock_conn:
        mock_conn.return_value = mq_connection()
        yield mock_conn


def fake_get(*args, **kwargs):
    import ibmmq

    raise ibmmq.MQMIError("connection broken", reason=ibmmq.CMQC.MQRC_CONNECTION_BROKEN)


@pytest.mark.asyncio
class TestIBMMQHook:
    """Tests for the IBM MQ hook."""

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        # Add a valid MQ connection
        create_connection_without_db(
            Connection(
                conn_id="mq_conn",
                conn_type="mq",
                host="mq.example.com",
                login="user",
                password="pass",
                port=1414,
                extra='{"queue_manager": "QM1", "channel": "DEV.APP.SVRCONN"}',
            )
        )
        self.hook = IBMMQHook("mq_conn")

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    @patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async")
    async def test_consume_message(
        self, mock_sync_to_async, mock_queue_class, mock_connect, mock_get_connection
    ):
        """Test consuming a single message."""

        # Mock connection and queue
        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        # Mock queue instance
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.return_value = MQ_PAYLOAD.format("test message").encode()

        # Mock sync_to_async to call the wrapped function directly for testing
        def mock_sync_to_async_impl(func):
            async def async_wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return async_wrapper

        mock_sync_to_async.side_effect = mock_sync_to_async_impl

        result = await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)
        assert isinstance(result, str)
        assert "test message" in result

        mock_connect.assert_called_once()  # connection established
        mock_queue_class.assert_called_once_with(
            mock_qmgr,
            mock.ANY,
            mock.ANY,
        )

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    @patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async")
    async def test_produce_message(
        self, mock_sync_to_async, mock_queue_class, mock_connect, mock_get_connection
    ):
        """Test producing a message to the queue."""

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        # Mock sync_to_async to call the wrapped function directly for testing
        def mock_sync_to_async_impl(func):
            async def async_wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return async_wrapper

        mock_sync_to_async.side_effect = mock_sync_to_async_impl

        await self.hook.produce(queue_name="QUEUE1", payload="payload")

        mock_connect.assert_called_once()
        mock_queue_class.assert_called_once_with(
            mock_qmgr,
            mock.ANY,
            mock.ANY,
        )
        mock_queue.put.assert_called_once()

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    @patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async")
    async def test_consume_connection_broken(
        self, mock_sync_to_async, mock_queue_class, mock_connect, mock_get_connection, caplog
    ):
        """Test that consume exits gracefully on connection broken."""

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.side_effect = fake_get

        # Mock sync_to_async to call the wrapped function directly for testing
        def mock_sync_to_async_impl(func):
            async def async_wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return async_wrapper

        mock_sync_to_async.side_effect = mock_sync_to_async_impl

        result = await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)
        assert result is None
        assert "MQ connection broken on queue 'QUEUE1', will exit consume; next trigger instance will reconnect" in caplog.text
