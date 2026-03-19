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

from unittest.mock import AsyncMock, patch

import pytest

from airflow.providers.ibm.mq.hooks.mq import IBMMQHook
from airflow.providers.ibm.mq.triggers.mq import AwaitMessageTrigger
from airflow.triggers.base import TriggerEvent


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
    @patch.object(IBMMQHook, "consume", new_callable=AsyncMock, return_value="test message")
    async def test_trigger_run_yields_event(self, mock_consume):
        """run() delegates to consume() and yields the result as a TriggerEvent."""
        trigger = AwaitMessageTrigger(
            mq_conn_id="mq_default",
            queue_name="QUEUE1",
            poll_interval=0.1,
        )

        event = await anext(trigger.run())
        assert isinstance(event, TriggerEvent)
        assert event.payload == "test message"
        mock_consume.assert_called_once_with(queue_name="QUEUE1", poll_interval=0.1)

