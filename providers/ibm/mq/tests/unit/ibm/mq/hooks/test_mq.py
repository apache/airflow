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
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.ibm.mq.hooks.mq import (
    IBMMQHook,
    _BACKOFF_BASE,
    _BACKOFF_FACTOR,
    _BACKOFF_MAX,
)

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
        """Test that consume logs a warning on connection broken."""

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

        # consume() retries on None, so we need to cancel after the first attempt
        with patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError
            with pytest.raises(asyncio.CancelledError):
                await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)

        assert "MQ connection broken on queue 'QUEUE1'; will reconnect" in caplog.text

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_consume_retries_on_none_then_succeeds(self, mock_sleep):
        """When _consume_sync returns None, consume retries with backoff until a message arrives."""
        with patch.object(
            self.hook, "_consume_sync", side_effect=[None, None, "payload after retries"]
        ):
            with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async") as mock_s2a:
                mock_s2a.side_effect = lambda fn: AsyncMock(side_effect=fn)

                result = await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "payload after retries"
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(_BACKOFF_BASE)
        mock_sleep.assert_any_call(_BACKOFF_BASE * _BACKOFF_FACTOR)

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_consume_retries_on_exception_then_succeeds(self, mock_sleep):
        """When _consume_sync raises, consume retries with backoff."""
        with patch.object(
            self.hook, "_consume_sync", side_effect=[ConnectionError("broken"), "recovered"]
        ):
            with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async") as mock_s2a:
                mock_s2a.side_effect = lambda fn: AsyncMock(side_effect=fn)

                result = await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "recovered"
        mock_sleep.assert_called_once_with(_BACKOFF_BASE)

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_consume_backoff_caps_at_max(self, mock_sleep):
        """Backoff delay should not exceed _BACKOFF_MAX."""
        failures_needed = 0
        backoff = _BACKOFF_BASE
        while backoff < _BACKOFF_MAX:
            backoff *= _BACKOFF_FACTOR
            failures_needed += 1
        failures_needed += 3

        with patch.object(
            self.hook, "_consume_sync", side_effect=[None] * failures_needed + ["finally"]
        ):
            with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async") as mock_s2a:
                mock_s2a.side_effect = lambda fn: AsyncMock(side_effect=fn)

                result = await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "finally"
        capped_calls = [c for c in mock_sleep.call_args_list if c.args[0] == _BACKOFF_MAX]
        assert len(capped_calls) >= 3

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_consume_logs_warning_on_none(self, mock_sleep, caplog):
        """A warning is logged when _consume_sync returns None."""
        with patch.object(self.hook, "_consume_sync", side_effect=[None, "message"]):
            with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async") as mock_s2a:
                mock_s2a.side_effect = lambda fn: AsyncMock(side_effect=fn)

                with caplog.at_level("WARNING"):
                    await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)

        assert "IBM MQ consume returned no event" in caplog.text

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_consume_logs_warning_on_exception(self, mock_sleep, caplog):
        """A warning with traceback is logged when _consume_sync raises."""
        with patch.object(self.hook, "_consume_sync", side_effect=[RuntimeError("boom"), "ok"]):
            with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async") as mock_s2a:
                mock_s2a.side_effect = lambda fn: AsyncMock(side_effect=fn)

                with caplog.at_level("WARNING"):
                    await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)

        assert "IBM MQ consume encountered an error" in caplog.text

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_consume_cancelled_error_propagates(self, mock_sleep):
        """CancelledError during backoff sleep propagates out of consume."""
        mock_sleep.side_effect = asyncio.CancelledError

        with patch.object(self.hook, "_consume_sync", return_value=None):
            with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async") as mock_s2a:
                mock_s2a.side_effect = lambda fn: AsyncMock(side_effect=fn)

                with pytest.raises(asyncio.CancelledError):
                    await self.hook.consume(queue_name="QUEUE1", poll_interval=0.1)
