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

from unittest.mock import AsyncMock, Mock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.triggers.eks import EksCreateClusterTrigger
from airflow.triggers.base import TriggerEvent

EXCEPTION_MOCK = AirflowException("MOCK ERROR")
CLUSTER_NAME = "test_cluster"
WAITER_DELAY = 1
WAITER_MAX_ATTEMPTS = 10
AWS_CONN_ID = "test_conn_id"
REGION_NAME = "test-region"


class TestEksCreateClusterTrigger:
    @pytest.mark.asyncio
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.async_conn")
    @patch("airflow.providers.amazon.aws.triggers.eks.async_wait", return_value=True)
    async def test_when_cluster_is_created_run_should_return_a_success_event(
        self, mock_async_wait, mock_async_conn
    ):
        mock = AsyncMock()
        mock_async_conn.__aenter__.return_value = mock

        trigger = EksCreateClusterTrigger(
            cluster_name=CLUSTER_NAME,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.async_conn")
    @patch(
        "airflow.providers.amazon.aws.triggers.eks.async_wait",
        side_effect=EXCEPTION_MOCK,
    )
    async def test_when_run_raises_exception_it_should_return_a_failure_event(
        self, mock_async_wait, mock_async_conn
    ):
        mock = AsyncMock()
        mock_async_conn.__aenter__.return_value = mock

        trigger = EksCreateClusterTrigger(
            cluster_name=CLUSTER_NAME,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
        )
        trigger.log.error = Mock()

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "failed"})
        trigger.log.error.assert_called_once_with("Error creating cluster: %s", EXCEPTION_MOCK)

    @pytest.mark.asyncio
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.async_conn")
    @patch("airflow.providers.amazon.aws.triggers.eks.async_wait", return_value=True)
    async def test_run_parameterizes_async_wait_correctly(self, mock_async_wait, mock_async_conn):
        mock = AsyncMock()
        mock_async_conn.__aenter__.return_value = mock
        client = mock_async_conn.__aenter__.return_value
        client.get_waiter = Mock(return_value="waiter")

        trigger = EksCreateClusterTrigger(
            cluster_name=CLUSTER_NAME,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
        )

        generator = trigger.run()
        await generator.asend(None)

        client.get_waiter.assert_called_once_with("cluster_active")
        mock_async_wait.assert_called_once_with(
            "waiter",
            WAITER_DELAY,
            WAITER_MAX_ATTEMPTS,
            {"name": CLUSTER_NAME},
            "Error checking Eks cluster",
            "Eks cluster status is",
            ["cluster.status"],
        )
