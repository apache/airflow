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

import pytest

from airflow.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterTrigger,
)
from airflow.triggers.base import TriggerEvent

pytest.importorskip("aiobotocore")

TASK_ID = "redshift_trigger_check"
POLLING_PERIOD_SECONDS = 1.0


class TestRedshiftClusterTrigger:
    def test_pause_serialization(self):
        """
        Asserts that the RedshiftClusterTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = RedshiftClusterTrigger(
            task_id=TASK_ID,
            poll_interval=POLLING_PERIOD_SECONDS,
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            attempts=10,
            operation_type="pause_cluster",
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger"
        assert kwargs == {
            "task_id": TASK_ID,
            "poll_interval": POLLING_PERIOD_SECONDS,
            "aws_conn_id": "test_redshift_conn_id",
            "cluster_identifier": "mock_cluster_identifier",
            "attempts": 10,
            "operation_type": "pause_cluster",
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.pause_cluster")
    async def test_pause_trigger_run(self, mock_pause_cluster):
        """
        Test trigger event for the pause_cluster response
        """
        trigger = RedshiftClusterTrigger(
            task_id=TASK_ID,
            poll_interval=POLLING_PERIOD_SECONDS,
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            attempts=1,
            operation_type="pause_cluster",
        )
        generator = trigger.run()
        await generator.asend(None)
        mock_pause_cluster.assert_called_once_with(
            cluster_identifier="mock_cluster_identifier", poll_interval=1.0
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.pause_cluster")
    async def test_pause_trigger_failure(self, mock_pause_cluster):
        """Test trigger event when pause cluster raise exception"""
        mock_pause_cluster.side_effect = Exception("Test exception")
        trigger = RedshiftClusterTrigger(
            task_id=TASK_ID,
            poll_interval=POLLING_PERIOD_SECONDS,
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            attempts=1,
            operation_type="pause_cluster",
        )
        task = [i async for i in trigger.run()]
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "operation_type,return_value,response",
        [
            (
                "resume_cluster",
                {"status": "error", "message": "test error"},
                TriggerEvent({"status": "error", "message": "test error"}),
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.resume_cluster")
    async def test_resume_trigger_run_error(
        self, mock_resume_cluster, operation_type, return_value, response
    ):
        """Test RedshiftClusterTrigger resume cluster with success"""
        mock_resume_cluster.return_value = return_value
        trigger = RedshiftClusterTrigger(
            task_id=TASK_ID,
            poll_interval=POLLING_PERIOD_SECONDS,
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            operation_type=operation_type,
            attempts=1,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert response == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "operation_type,return_value,response",
        [
            (
                "resume_cluster",
                {"status": "success", "cluster_state": "available"},
                TriggerEvent({"status": "success", "cluster_state": "available"}),
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.resume_cluster")
    async def test_resume_trigger_run_success(
        self, mock_resume_cluster, operation_type, return_value, response
    ):
        """Test RedshiftClusterTrigger resume cluster with success"""
        mock_resume_cluster.return_value = return_value
        trigger = RedshiftClusterTrigger(
            task_id=TASK_ID,
            poll_interval=POLLING_PERIOD_SECONDS,
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            operation_type=operation_type,
            attempts=1,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert response == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.resume_cluster")
    async def test_resume_trigger_failure(self, mock_resume_cluster):
        """Test RedshiftClusterTrigger resume cluster with failure status"""
        mock_resume_cluster.side_effect = Exception("Test exception")
        trigger = RedshiftClusterTrigger(
            task_id=TASK_ID,
            poll_interval=POLLING_PERIOD_SECONDS,
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            operation_type="resume_cluster",
            attempts=1,
        )
        task = [i async for i in trigger.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
