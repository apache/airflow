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

from airflow.providers.amazon.aws.triggers.neptune import (
    NeptuneClusterAvailableTrigger,
    NeptuneClusterInstancesAvailableTrigger,
    NeptuneClusterStoppedTrigger,
)
from airflow.triggers.base import TriggerEvent

CLUSTER_ID = "test-cluster"
REGION_NAME = "eu-west-2"
VERIFY = False
BOTOCORE_CONFIG = {"read_timeout": 42}
WAITER_DELAY = 12
WAITER_MAX_ATTEMPTS = 34


@pytest.mark.parametrize(
    "trigger_class",
    [
        NeptuneClusterAvailableTrigger,
        NeptuneClusterStoppedTrigger,
        NeptuneClusterInstancesAvailableTrigger,
    ],
)
def test_hook_configuration_survives_serialization(trigger_class):
    trigger = trigger_class(
        db_cluster_id=CLUSTER_ID,
        aws_conn_id="aws_default",
        region_name=REGION_NAME,
        verify=VERIFY,
        botocore_config=BOTOCORE_CONFIG,
        waiter_delay=WAITER_DELAY,
        waiter_max_attempts=WAITER_MAX_ATTEMPTS,
    )

    assert trigger.region_name == REGION_NAME
    assert trigger.verify == VERIFY
    assert trigger.botocore_config == BOTOCORE_CONFIG
    assert trigger.serialize()[1] == {
        "db_cluster_id": CLUSTER_ID,
        "aws_conn_id": "aws_default",
        "region_name": REGION_NAME,
        "verify": VERIFY,
        "botocore_config": BOTOCORE_CONFIG,
        "waiter_delay": WAITER_DELAY,
        "waiter_max_attempts": WAITER_MAX_ATTEMPTS,
    }


class TestNeptuneClusterAvailableTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneClusterAvailableTrigger(db_cluster_id=CLUSTER_ID)
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.neptune.NeptuneClusterAvailableTrigger"
        assert "db_cluster_id" in kwargs
        assert kwargs["db_cluster_id"] == CLUSTER_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune.NeptuneHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune.NeptuneHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "available"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneClusterAvailableTrigger(db_cluster_id=CLUSTER_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp == TriggerEvent({"status": "success", "db_cluster_id": CLUSTER_ID})
        assert mock_get_waiter().wait.call_count == 1


class TestNeptuneClusterStoppedTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneClusterStoppedTrigger(db_cluster_id=CLUSTER_ID)
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.neptune.NeptuneClusterStoppedTrigger"
        assert "db_cluster_id" in kwargs
        assert kwargs["db_cluster_id"] == CLUSTER_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune.NeptuneHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune.NeptuneHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "stopped"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneClusterStoppedTrigger(db_cluster_id=CLUSTER_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp == TriggerEvent({"status": "success", "db_cluster_id": CLUSTER_ID})
        assert mock_get_waiter().wait.call_count == 1


class TestNeptuneClusterInstancesAvailableTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NeptuneClusterInstancesAvailableTrigger(db_cluster_id=CLUSTER_ID)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.neptune.NeptuneClusterInstancesAvailableTrigger"
        )
        assert "db_cluster_id" in kwargs
        assert kwargs["db_cluster_id"] == CLUSTER_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune.NeptuneHook.get_waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune.NeptuneHook.get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = "available"
        mock_get_waiter().wait = AsyncMock()
        trigger = NeptuneClusterInstancesAvailableTrigger(db_cluster_id=CLUSTER_ID)
        generator = trigger.run()
        resp = await generator.asend(None)

        assert resp == TriggerEvent({"status": "success", "db_cluster_id": CLUSTER_ID})
        assert mock_get_waiter().wait.call_count == 1

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.neptune.NeptuneHook.get_async_conn")
    async def test_run_fail(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.return_value.__aenter__.return_value = a_mock
        wait_mock = AsyncMock()
        wait_mock.side_effect = WaiterError("name", "reason", {"test": [{"lastStatus": "my_status"}]})
        a_mock.get_waiter().wait = wait_mock
        trigger = NeptuneClusterInstancesAvailableTrigger(
            db_cluster_id=CLUSTER_ID, waiter_delay=1, waiter_max_attempts=2
        )

        event = await trigger.run().asend(None)
        assert event.payload["status"] == "error"
        assert "message" in event.payload
