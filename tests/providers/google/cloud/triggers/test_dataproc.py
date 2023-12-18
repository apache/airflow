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
from asyncio import Future
from unittest import mock

import pytest
from google.cloud.dataproc_v1 import Batch, ClusterStatus
from google.rpc.status_pb2 import Status

from airflow.providers.google.cloud.triggers.dataproc import (
    DataprocBatchTrigger,
    DataprocClusterTrigger,
    DataprocWorkflowTrigger,
)
from airflow.triggers.base import TriggerEvent

TEST_PROJECT_ID = "project-id"
TEST_REGION = "region"
TEST_BATCH_ID = "batch-id"
BATCH_CONFIG = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}
TEST_CLUSTER_NAME = "cluster_name"
TEST_POLL_INTERVAL = 5
TEST_GCP_CONN_ID = "google_cloud_default"
TEST_OPERATION_NAME = "name"


@pytest.fixture
def cluster_trigger():
    return DataprocClusterTrigger(
        cluster_name=TEST_CLUSTER_NAME,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
    )


@pytest.fixture
def batch_trigger():
    trigger = DataprocBatchTrigger(
        batch_id=TEST_BATCH_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
    )
    return trigger


@pytest.fixture
def workflow_trigger():
    return DataprocWorkflowTrigger(
        name=TEST_OPERATION_NAME,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
    )


@pytest.fixture()
def async_get_cluster():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = asyncio.Future()
        f.set_result(m)
        return f

    return func


@pytest.fixture()
def async_get_batch():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = Future()
        f.set_result(m)
        return f

    return func


@pytest.fixture()
def async_get_operation():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = Future()
        f.set_result(m)
        return f

    return func


@pytest.mark.db_test
class TestDataprocClusterTrigger:
    def test_async_cluster_trigger_serialization_should_execute_successfully(self, cluster_trigger):
        classpath, kwargs = cluster_trigger.serialize()
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
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_cluster")
    async def test_async_cluster_triggers_on_success_should_execute_successfully(
        self, mock_hook, cluster_trigger, async_get_cluster
    ):
        mock_hook.return_value = async_get_cluster(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.RUNNING),
        )

        generator = cluster_trigger.run()
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
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_cluster")
    async def test_async_cluster_trigger_run_returns_error_event(
        self, mock_hook, cluster_trigger, async_get_cluster
    ):
        mock_hook.return_value = async_get_cluster(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.ERROR),
        )

        actual_event = await cluster_trigger.run().asend(None)
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
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_cluster")
    async def test_cluster_run_loop_is_still_running(
        self, mock_hook, cluster_trigger, caplog, async_get_cluster
    ):
        mock_hook.return_value = async_get_cluster(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.CREATING),
        )

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(cluster_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert f"Current state is: {ClusterStatus.State.CREATING}"
        assert f"Sleeping for {TEST_POLL_INTERVAL} seconds."


@pytest.mark.db_test
class TestDataprocBatchTrigger:
    def test_async_create_batch_trigger_serialization_should_execute_successfully(self, batch_trigger):
        """
        Asserts that the DataprocBatchTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = batch_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.dataproc.DataprocBatchTrigger"
        assert kwargs == {
            "batch_id": TEST_BATCH_ID,
            "project_id": TEST_PROJECT_ID,
            "region": TEST_REGION,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": None,
            "polling_interval_seconds": TEST_POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_batch")
    async def test_async_create_batch_trigger_triggers_on_success_should_execute_successfully(
        self, mock_hook, batch_trigger, async_get_batch
    ):
        """
        Tests the DataprocBatchTrigger only fires once the batch execution reaches a successful state.
        """

        mock_hook.return_value = async_get_batch(state=Batch.State.SUCCEEDED, batch_id=TEST_BATCH_ID)

        expected_event = TriggerEvent(
            {
                "batch_id": TEST_BATCH_ID,
                "batch_state": Batch.State.SUCCEEDED,
            }
        )

        actual_event = await batch_trigger.run().asend(None)
        await asyncio.sleep(0.5)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_batch")
    async def test_async_create_batch_trigger_run_returns_failed_event(
        self, mock_hook, batch_trigger, async_get_batch
    ):
        mock_hook.return_value = async_get_batch(state=Batch.State.FAILED, batch_id=TEST_BATCH_ID)

        expected_event = TriggerEvent({"batch_id": TEST_BATCH_ID, "batch_state": Batch.State.FAILED})

        actual_event = await batch_trigger.run().asend(None)
        await asyncio.sleep(0.5)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_batch")
    async def test_create_batch_run_returns_cancelled_event(self, mock_hook, batch_trigger, async_get_batch):
        mock_hook.return_value = async_get_batch(state=Batch.State.CANCELLED, batch_id=TEST_BATCH_ID)

        expected_event = TriggerEvent({"batch_id": TEST_BATCH_ID, "batch_state": Batch.State.CANCELLED})

        actual_event = await batch_trigger.run().asend(None)
        await asyncio.sleep(0.5)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_batch")
    async def test_create_batch_run_loop_is_still_running(
        self, mock_hook, batch_trigger, caplog, async_get_batch
    ):
        mock_hook.return_value = async_get_batch(state=Batch.State.RUNNING)

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(batch_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert f"Current state is: {Batch.State.RUNNING}"
        assert f"Sleeping for {TEST_POLL_INTERVAL} seconds."


class TestDataprocWorkflowTrigger:
    def test_async_cluster_trigger_serialization_should_execute_successfully(self, workflow_trigger):
        classpath, kwargs = workflow_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.dataproc.DataprocWorkflowTrigger"
        assert kwargs == {
            "name": TEST_OPERATION_NAME,
            "project_id": TEST_PROJECT_ID,
            "region": TEST_REGION,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": None,
            "polling_interval_seconds": TEST_POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBaseTrigger.get_async_hook")
    async def test_async_workflow_triggers_on_success_should_execute_successfully(
        self, mock_hook, workflow_trigger, async_get_operation
    ):
        mock_hook.return_value.get_operation.return_value = async_get_operation(
            name=TEST_OPERATION_NAME, done=True, response={}, error=Status(message="")
        )

        expected_event = TriggerEvent(
            {
                "operation_name": TEST_OPERATION_NAME,
                "operation_done": True,
                "status": "success",
                "message": "Operation is successfully ended.",
            }
        )
        actual_event = await workflow_trigger.run().asend(None)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBaseTrigger.get_async_hook")
    async def test_async_workflow_triggers_on_error(self, mock_hook, workflow_trigger, async_get_operation):
        mock_hook.return_value.get_operation.return_value = async_get_operation(
            name=TEST_OPERATION_NAME, done=True, response={}, error=Status(message="test_error")
        )

        expected_event = TriggerEvent(
            {
                "operation_name": TEST_OPERATION_NAME,
                "operation_done": True,
                "status": "error",
                "message": "test_error",
            }
        )
        actual_event = await workflow_trigger.run().asend(None)
        assert expected_event == actual_event
