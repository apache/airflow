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
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.triggers.neptune_analytics import (
    NeptuneGraphAvailableTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.triggers.base import TriggerEvent

GRAPH_ID = "test-graph"


class TestNeptuneGraphAvailableTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneGraphAvailableTrigger(graph_id=GRAPH_ID)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.neptune_analytics.NeptuneGraphAvailableTrigger"
        )
        assert "graph_id" in kwargs
        assert kwargs["graph_id"] == GRAPH_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "AVAILABLE"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneGraphAvailableTrigger(graph_id=GRAPH_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp == TriggerEvent({"status": "success", "graph_id": GRAPH_ID})
        assert mock_get_waiter().wait.call_count == 1

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_failure(self, mock_async_conn, mock_get_waiter):
        wait_mock = AsyncMock()
        wait_mock.side_effect = WaiterError(
            name="graph_available",
            reason='Waiter encountered a terminal failure state: For expression "status" we matched expected path: "FAILED"',
            last_response={"status": "FAILED", "graphIdentifier": GRAPH_ID},
        )
        mock_get_waiter.return_value.wait = wait_mock

        trigger = NeptuneGraphAvailableTrigger(graph_id=GRAPH_ID)

        with pytest.raises(AirflowException):
            await trigger.run().asend(None)
