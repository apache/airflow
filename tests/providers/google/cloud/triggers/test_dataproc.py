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
from google.cloud.dataproc_v1 import Batch, Cluster, ClusterStatus
from google.protobuf.any_pb2 import Any
from google.rpc.status_pb2 import Status

from airflow.providers.google.cloud.triggers.dataproc import (
    DataprocBatchTrigger,
    DataprocClusterTrigger,
    DataprocOperationTrigger,
)
from airflow.providers.google.cloud.utils.dataproc import DataprocOperationType
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
        delete_on_error=True,
    )
    return trigger


@pytest.fixture
def operation_trigger():
    return DataprocOperationTrigger(
        name=TEST_OPERATION_NAME,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
    )


@pytest.fixture
def diagnose_operation_trigger():
    return DataprocOperationTrigger(
        name=TEST_OPERATION_NAME,
        operation_type=DataprocOperationType.DIAGNOSE.value,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
        delete_on_error=True,
    )


@pytest.fixture
def async_get_cluster():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = asyncio.Future()
        f.set_result(m)
        return f

    return func


@pytest.fixture
def async_get_batch():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = Future()
        f.set_result(m)
        return f

    return func


@pytest.fixture
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
            "delete_on_error": True,
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
    @mock.patch(
        "airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.delete_cluster",
        return_value=asyncio.Future(),
    )
    @mock.patch("google.auth.default")
    async def test_async_cluster_trigger_run_returns_error_event(
        self, mock_auth, mock_delete_cluster, mock_get_cluster, cluster_trigger, async_get_cluster, caplog
    ):
        mock_credentials = mock.MagicMock()
        mock_credentials.universe_domain = "googleapis.com"

        mock_auth.return_value = (mock_credentials, "project-id")

        mock_delete_cluster.return_value = asyncio.Future()
        mock_delete_cluster.return_value.set_result(None)

        mock_get_cluster.return_value = async_get_cluster(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.ERROR),
        )

        caplog.set_level(logging.INFO)

        trigger_event = None
        async for event in cluster_trigger.run():
            trigger_event = event

        assert trigger_event.payload["cluster_name"] == TEST_CLUSTER_NAME
        assert trigger_event.payload["cluster_state"] == ClusterStatus.State.DELETING

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
        assert f"Current state is: {ClusterStatus.State.CREATING}."
        assert f"Sleeping for {TEST_POLL_INTERVAL} seconds."

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.get_cluster")
    async def test_fetch_cluster_status(self, mock_get_cluster, cluster_trigger, async_get_cluster):
        mock_get_cluster.return_value = async_get_cluster(
            status=ClusterStatus(state=ClusterStatus.State.RUNNING)
        )
        cluster = await cluster_trigger.fetch_cluster()

        assert cluster.status.state == ClusterStatus.State.RUNNING, "The cluster state should be RUNNING"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.delete_cluster")
    async def test_delete_when_error_occurred(self, mock_delete_cluster, cluster_trigger):
        mock_cluster = mock.MagicMock(spec=Cluster)
        type(mock_cluster).status = mock.PropertyMock(
            return_value=mock.MagicMock(state=ClusterStatus.State.ERROR)
        )

        mock_delete_future = asyncio.Future()
        mock_delete_future.set_result(None)
        mock_delete_cluster.return_value = mock_delete_future

        cluster_trigger.delete_on_error = True

        await cluster_trigger.delete_when_error_occurred(mock_cluster)

        mock_delete_cluster.assert_called_once_with(
            region=cluster_trigger.region,
            cluster_name=cluster_trigger.cluster_name,
            project_id=cluster_trigger.project_id,
        )

        mock_delete_cluster.reset_mock()
        cluster_trigger.delete_on_error = False

        await cluster_trigger.delete_when_error_occurred(mock_cluster)

        mock_delete_cluster.assert_not_called()


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


class TestDataprocOperationTrigger:
    def test_async_cluster_trigger_serialization_should_execute_successfully(self, operation_trigger):
        classpath, kwargs = operation_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.dataproc.DataprocOperationTrigger"
        assert kwargs == {
            "name": TEST_OPERATION_NAME,
            "project_id": TEST_PROJECT_ID,
            "operation_type": None,
            "region": TEST_REGION,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": None,
            "polling_interval_seconds": TEST_POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBaseTrigger.get_async_hook")
    async def test_async_operation_triggers_on_success_should_execute_successfully(
        self, mock_hook, operation_trigger, async_get_operation
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
        actual_event = await operation_trigger.run().asend(None)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBaseTrigger.get_async_hook")
    async def test_async_diagnose_operation_triggers_on_success_should_execute_successfully(
        self, mock_hook, diagnose_operation_trigger, async_get_operation
    ):
        gcs_uri = "gs://test-tarball-gcs-dir-bucket"
        mock_hook.return_value.get_operation.return_value = async_get_operation(
            name=TEST_OPERATION_NAME,
            done=True,
            response=Any(value=gcs_uri.encode("utf-8")),
            error=Status(message=""),
        )

        expected_event = TriggerEvent(
            {
                "output_uri": gcs_uri,
                "status": "success",
                "message": "Operation is successfully ended.",
            }
        )
        actual_event = await diagnose_operation_trigger.run().asend(None)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBaseTrigger.get_async_hook")
    async def test_async_operation_triggers_on_error(self, mock_hook, operation_trigger, async_get_operation):
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
        actual_event = await operation_trigger.run().asend(None)
        assert expected_event == actual_event
