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
    NeptuneGraphDeletedTrigger,
    NeptuneGraphPrivateEndpointAvailableTrigger,
    NeptuneGraphPrivateEndpointDeletedTrigger,
    NeptuneImportTaskCancelledTrigger,
    NeptuneImportTaskCompleteTrigger,
)
from airflow.triggers.base import TriggerEvent

GRAPH_ID = "test-graph"
VPC_ID = "test-vpc"
ENDPOINT_ID = "test-endpoint"
TASK_ID = "test-task-id"


class TestNeptuneGraphAvailableTrigger:
    def test_serialization(self):
        """
        Asserts that the NeptuneGraphAvailableTrigger correctly serializes its arguments
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
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp.payload["status"] == "error"
        assert resp.payload["graph_id"] == GRAPH_ID
        assert "Failed to create Neptune graph" in resp.payload["message"]


class TestNeptuneGraphPrivateEndpointAvailableTrigger:
    def test_serialization(self):
        """
        Asserts that the NeptuneGraphPrivateEndpointAvailableTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneGraphPrivateEndpointAvailableTrigger(graph_id=GRAPH_ID, vpc_id=VPC_ID)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.neptune_analytics.NeptuneGraphPrivateEndpointAvailableTrigger"
        )
        assert "graph_id" in kwargs
        assert kwargs["graph_id"] == GRAPH_ID
        assert "vpc_id" in kwargs
        assert kwargs["vpc_id"] == VPC_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "AVAILABLE"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneGraphPrivateEndpointAvailableTrigger(graph_id=GRAPH_ID, vpc_id=VPC_ID)
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
            name="private_graph_endpoint_available",
            reason='Waiter encountered a terminal failure state: For expression "status" we matched expected path: "FAILED"',
            last_response={"status": "FAILED", "graphIdentifier": GRAPH_ID, "vpcId": VPC_ID},
        )
        mock_get_waiter.return_value.wait = wait_mock

        trigger = NeptuneGraphPrivateEndpointAvailableTrigger(graph_id=GRAPH_ID, vpc_id=VPC_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp.payload["status"] == "error"
        assert resp.payload["graph_id"] == GRAPH_ID
        assert "Failed to create Neptune graph endpoint" in resp.payload["message"]


class TestNeptuneGraphPrivateEndpointDeletedTrigger:
    def test_serialization(self):
        """
        Asserts that the NeptuneGraphPrivateEndpointDeletedTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneGraphPrivateEndpointDeletedTrigger(
            graph_id=GRAPH_ID, vpc_id=VPC_ID, endpoint_id=ENDPOINT_ID
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.neptune_analytics.NeptuneGraphPrivateEndpointDeletedTrigger"
        )
        assert "graph_id" in kwargs
        assert kwargs["graph_id"] == GRAPH_ID
        assert "vpc_id" in kwargs
        assert kwargs["vpc_id"] == VPC_ID
        assert "endpoint_id" in kwargs
        assert kwargs["endpoint_id"] == ENDPOINT_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "DELETED"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneGraphPrivateEndpointDeletedTrigger(
            graph_id=GRAPH_ID, vpc_id=VPC_ID, endpoint_id=ENDPOINT_ID
        )
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp == TriggerEvent({"status": "success", "endpoint_id": ENDPOINT_ID})
        assert mock_get_waiter().wait.call_count == 1

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_failure(self, mock_async_conn, mock_get_waiter):
        wait_mock = AsyncMock()
        wait_mock.side_effect = WaiterError(
            name="private_graph_endpoint_deleted",
            reason='Waiter encountered a terminal failure state: For expression "status" we matched expected path: "FAILED"',
            last_response={"status": "FAILED", "graphIdentifier": GRAPH_ID, "vpcId": VPC_ID},
        )
        mock_get_waiter.return_value.wait = wait_mock

        trigger = NeptuneGraphPrivateEndpointDeletedTrigger(
            graph_id=GRAPH_ID, vpc_id=VPC_ID, endpoint_id=ENDPOINT_ID
        )
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp.payload["status"] == "error"
        assert resp.payload["endpoint_id"] == ENDPOINT_ID
        assert "Failed to delete Neptune graph endpoint" in resp.payload["message"]


class TestNeptuneImportTaskCompleteTrigger:
    def test_serialization(self):
        """
        Asserts that the NeptuneImportTaskCompleteTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneImportTaskCompleteTrigger(import_task_id=TASK_ID)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.neptune_analytics.NeptuneImportTaskCompleteTrigger"
        )
        assert "import_task_id" in kwargs
        assert kwargs["import_task_id"] == TASK_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "COMPLETED"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneImportTaskCompleteTrigger(import_task_id=TASK_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp == TriggerEvent({"status": "success", "import_task_id": TASK_ID})
        assert mock_get_waiter().wait.call_count == 1

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_failure(self, mock_async_conn, mock_get_waiter):
        wait_mock = AsyncMock()
        wait_mock.side_effect = WaiterError(
            name="import_task_successful",
            reason='Waiter encountered a terminal failure state: For expression "status" we matched expected path: "FAILED"',
            last_response={"status": "FAILED", "taskIdentifier": TASK_ID},
        )
        mock_get_waiter.return_value.wait = wait_mock

        trigger = NeptuneImportTaskCompleteTrigger(import_task_id=TASK_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp.payload["status"] == "error"
        assert resp.payload["import_task_id"] == TASK_ID
        assert "Import task failed" in resp.payload["message"]


class TestNeptuneImportTaskCancelledTrigger:
    def test_serialization(self):
        """
        Asserts that the NeptuneImportTaskCancelledTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneImportTaskCancelledTrigger(task_identifier=TASK_ID)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.neptune_analytics.NeptuneImportTaskCancelledTrigger"
        )
        assert "task_identifier" in kwargs
        assert kwargs["task_identifier"] == TASK_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "CANCELLED"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneImportTaskCancelledTrigger(task_identifier=TASK_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp == TriggerEvent({"status": "success", "import_task_id": TASK_ID})
        assert mock_get_waiter().wait.call_count == 1

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_failure(self, mock_async_conn, mock_get_waiter):
        wait_mock = AsyncMock()
        wait_mock.side_effect = WaiterError(
            name="import_task_cancelled",
            reason='Waiter encountered a terminal failure state: For expression "status" we matched expected path: "FAILED"',
            last_response={"status": "FAILED", "taskIdentifier": TASK_ID},
        )
        mock_get_waiter.return_value.wait = wait_mock

        trigger = NeptuneImportTaskCancelledTrigger(task_identifier=TASK_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp.payload["status"] == "error"
        assert resp.payload["import_task_id"] == TASK_ID
        assert "Import task cancellation failed" in resp.payload["message"]


class TestNeptuneGraphDeletedTrigger:
    def test_serialization(self):
        """
        Asserts that the NeptuneGraphDeletedTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneGraphDeletedTrigger(graph_id=GRAPH_ID)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath == "airflow.providers.amazon.aws.triggers.neptune_analytics.NeptuneGraphDeletedTrigger"
        )
        assert "graph_id" in kwargs
        assert kwargs["graph_id"] == GRAPH_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune_analytics.NeptuneAnalyticsHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "DELETED"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneGraphDeletedTrigger(graph_id=GRAPH_ID)
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
            name="graph_deleted",
            reason='Waiter encountered a terminal failure state: For expression "status" we matched expected path: "FAILED"',
            last_response={"status": "FAILED", "graphIdentifier": GRAPH_ID},
        )
        mock_get_waiter.return_value.wait = wait_mock

        trigger = NeptuneGraphDeletedTrigger(graph_id=GRAPH_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp.payload["status"] == "error"
        assert resp.payload["graph_id"] == GRAPH_ID
        assert "Failed to delete Neptune graph" in resp.payload["message"]
