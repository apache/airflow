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

import pytest

from airflow.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterTrigger,
)
from airflow.triggers.base import TriggerEvent

POLLING_PERIOD_SECONDS = 1.0


class TestRedshiftClusterTrigger:
    def test_redshift_cluster_sensor_trigger_serialization(self):
        """
        Asserts that the RedshiftClusterTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger"
        )
        assert kwargs == {
            "aws_conn_id": "test_redshift_conn_id",
            "cluster_identifier": "mock_cluster_identifier",
            "target_status": "available",
            "poke_interval": POLLING_PERIOD_SECONDS,
        }

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async"
    )
    async def test_redshift_cluster_sensor_trigger_success(self, mock_cluster_status):
        """
        Test RedshiftClusterTrigger with the success status
        """
        expected_result = "available"

        mock_cluster_status.return_value = expected_result
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )

        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "success", "message": "target state met"}) == actual
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "expected_result",
        ["Resuming"],
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async"
    )
    async def test_redshift_cluster_sensor_trigger_resuming_status(
        self, mock_cluster_status, expected_result
    ):
        """Test RedshiftClusterTrigger with the success status"""
        mock_cluster_status.return_value = expected_result
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is False

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async"
    )
    async def test_redshift_cluster_sensor_trigger_exception(self, mock_cluster_status):
        """Test RedshiftClusterTrigger with exception"""
        mock_cluster_status.side_effect = Exception("Test exception")
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )

        task = [i async for i in trigger.run()]
        # since we use return as soon as we yield the trigger event
        # at any given point there should be one trigger event returned to the task
        # so we validate for length of task to be 1
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
