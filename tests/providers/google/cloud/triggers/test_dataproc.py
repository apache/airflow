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
import logging

import pytest
from google.cloud.dataproc_v1 import ClusterStatus

from airflow.providers.google.cloud.triggers.dataproc import DataprocClusterTrigger
from airflow.triggers.base import TriggerEvent
from tests.providers.google.cloud.utils.compat import async_mock

TEST_PROJECT_ID = "project-id"
TEST_REGION = "region"
TEST_CLUSTER_NAME = "cluster_name"
TEST_POLL_INTERVAL = 5
TEST_GCP_CONN_ID = "google_cloud_default"


@pytest.fixture
def trigger():
    return DataprocClusterTrigger(
        cluster_name=TEST_CLUSTER_NAME,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
    )


@pytest.fixture()
def async_get_cluster():
    def func(**kwargs):
        m = async_mock.MagicMock()
        m.configure_mock(**kwargs)
        f = asyncio.Future()
        f.set_result(m)
        return f

    return func


class TestDataprocClusterTrigger:
    def test_async_cluster_trigger_serialization_should_execute_successfully(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger"
        assert kwargs == {
            "cluster_name": TEST_CLUSTER_NAME,
            "project_id": TEST_PROJECT_ID,
            "region": TEST_REGION,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": None,
            "polling_interval_seconds": TEST_POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_cluster")
    async def test_async_cluster_triggers_on_success_should_execute_successfully(
        self, mock_hook, trigger, async_get_cluster
    ):
        mock_hook.return_value = async_get_cluster(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.RUNNING),
        )

        generator = trigger.run()
        actual_event = await generator.asend(None)

        expected_event = TriggerEvent(
            {
                "cluster_name": TEST_CLUSTER_NAME,
                "cluster_state": ClusterStatus.State.RUNNING,
                "cluster": actual_event.payload["cluster"],
            }
        )
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_cluster")
    async def test_async_cluster_trigger_run_returns_error_event(self, mock_hook, trigger, async_get_cluster):
        mock_hook.return_value = async_get_cluster(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.ERROR),
        )

        actual_event = await (trigger.run()).asend(None)
        await asyncio.sleep(0.5)

        expected_event = TriggerEvent(
            {
                "cluster_name": TEST_CLUSTER_NAME,
                "cluster_state": ClusterStatus.State.ERROR,
                "cluster": actual_event.payload["cluster"],
            }
        )
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_cluster")
    async def test_cluster_run_loop_is_still_running(self, mock_hook, trigger, caplog, async_get_cluster):
        mock_hook.return_value = async_get_cluster(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.CREATING),
        )

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert f"Current state is: {ClusterStatus.State.CREATING}"
        assert f"Sleeping for {TEST_POLL_INTERVAL} seconds."
