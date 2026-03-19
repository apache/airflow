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

from unittest.mock import patch, MagicMock

import pytest

from airflow.models import Connection
from airflow.providers.ibm.mq.hooks.mq import IBMMQHook
from airflow.providers.ibm.mq.triggers.mq import AwaitMessageTrigger
from airflow.triggers.base import TriggerEvent


def mq_connection():
    """Create a test MQ connection object."""
    return Connection(
        conn_id="mq_default",
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


class TestMQTrigger:
    @pytest.mark.asyncio
    async def test_trigger_serialization(self):
        trigger = AwaitMessageTrigger(
            mq_conn_id="mq_default",
            queue_name="QUEUE1",
            poll_interval=2,
        )
        assert isinstance(trigger, AwaitMessageTrigger)

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.ibm.mq.triggers.mq.AwaitMessageTrigger"
        assert kwargs == {
            "mq_conn_id": "mq_default",
            "queue_name": "QUEUE1",
            "poll_interval": 2,
        }

    @pytest.mark.asyncio
    @patch.object(IBMMQHook, "consume", return_value="test message")
    async def test_trigger_run_message_yielded(self, mock_consume):
        trigger = AwaitMessageTrigger(
            mq_conn_id="mq_default",
            queue_name="QUEUE1",
            poll_interval=0.1,
        )

        event = await anext(trigger.run())
        assert isinstance(event, TriggerEvent)
        assert event.payload == "test message"
        mock_consume.assert_called_once_with(queue_name="QUEUE1", poll_interval=0.1)

    @pytest.mark.asyncio
    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    @patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async")
    async def test_trigger_run_none_on_connection_error(
        self, mock_sync_to_async, mock_queue, mock_connect, mock_get_connection, caplog
    ):
        """Test that the trigger yields None when consume encounters a connection problem."""
        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr
        mock_queue.return_value = MagicMock()

        # Mock sync_to_async to call the wrapped function directly for testing
        def mock_sync_to_async_impl(func):
            async def async_wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return async_wrapper

        mock_sync_to_async.side_effect = mock_sync_to_async_impl

        # Make the queue.get raise MQMIError
        mock_queue.return_value.get.side_effect = fake_get

        trigger = AwaitMessageTrigger(
            mq_conn_id="mq_default",
            queue_name="QUEUE1",
            poll_interval=0.1,
        )

        with caplog.at_level("WARNING"):
            with pytest.raises(StopAsyncIteration):
                await anext(trigger.run())

        assert "MQ connection broken" in caplog.text
