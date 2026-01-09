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
import contextlib
import logging
from asyncio import CancelledError, Future, sleep
from unittest import mock

import pytest
from google.cloud.dataproc_v1 import Batch, Cluster, ClusterStatus, Job, JobStatus
from google.protobuf.any_pb2 import Any
from google.rpc.status_pb2 import Status

from airflow.providers.google.cloud.triggers.dataproc import (
    DataprocBatchTrigger,
    DataprocClusterTrigger,
    DataprocOperationTrigger,
    DataprocSubmitTrigger,
)
from airflow.providers.google.cloud.utils.dataproc import DataprocOperationType
from airflow.triggers.base import TriggerEvent

TEST_PROJECT_ID = "project-id"
TEST_REGION = "region"
TEST_BATCH_ID = "batch-id"
TEST_BATCH_STATE_MESSAGE = "Test batch state message"
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
TEST_JOB_ID = "test-job-id"
TEST_RUNNING_CLUSTER = Cluster(
    cluster_name=TEST_CLUSTER_NAME,
    status=ClusterStatus(state=ClusterStatus.State.RUNNING),
)
TEST_ERROR_CLUSTER = Cluster(
    cluster_name=TEST_CLUSTER_NAME,
    status=ClusterStatus(state=ClusterStatus.State.ERROR),
)


@pytest.fixture
def cluster_trigger():
    return DataprocClusterTrigger(
        cluster_name=TEST_CLUSTER_NAME,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
        delete_on_error=True,
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
def submit_trigger():
    return DataprocSubmitTrigger(
        job_id=TEST_JOB_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        polling_interval_seconds=TEST_POLL_INTERVAL,
        cancel_on_kill=True,
    )


@pytest.fixture
def async_get_batch():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = Future()
        f.set_result(m)
        return f

    return func


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

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_async_hook")
    async def test_async_cluster_triggers_on_success_should_execute_successfully(
        self, mock_get_async_hook, cluster_trigger
    ):
        future = asyncio.Future()
        future.set_result(TEST_RUNNING_CLUSTER)
        mock_get_async_hook.return_value.get_cluster.return_value = future

        generator = cluster_trigger.run()
        actual_event = await generator.asend(None)

        expected_event = TriggerEvent(
            {
                "cluster_name": TEST_CLUSTER_NAME,
                "cluster_state": ClusterStatus.State(ClusterStatus.State.RUNNING).name,
                "cluster": actual_event.payload["cluster"],
            }
        )
        assert expected_event == actual_event

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.fetch_cluster")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.dataproc.DataprocAsyncHook.delete_cluster",
        return_value=asyncio.Future(),
    )
    @mock.patch("google.auth.default")
    async def test_async_cluster_trigger_run_returns_error_event(
        self, mock_auth, mock_delete_cluster, mock_fetch_cluster, cluster_trigger, async_get_cluster, caplog
    ):
        mock_credentials = mock.MagicMock()
        mock_credentials.universe_domain = "googleapis.com"

        mock_auth.return_value = (mock_credentials, "project-id")

        mock_delete_cluster.return_value = asyncio.Future()
        mock_delete_cluster.return_value.set_result(None)

        mock_fetch_cluster.return_value = TEST_ERROR_CLUSTER

        caplog.set_level(logging.INFO)

        trigger_event = None
        async for event in cluster_trigger.run():
            trigger_event = event

        assert trigger_event.payload["cluster_name"] == TEST_CLUSTER_NAME
        assert (
            trigger_event.payload["cluster_state"] == ClusterStatus.State(ClusterStatus.State.DELETING).name
        )

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_async_hook")
    @mock.patch.object(DataprocClusterTrigger, "log")
    async def test_cluster_run_loop_is_still_running(self, mock_log, mock_get_async_hook, cluster_trigger):
        mock_cluster = mock.MagicMock()
        mock_cluster.status = ClusterStatus(state=ClusterStatus.State.CREATING)

        future = asyncio.Future()
        future.set_result(mock_cluster)
        mock_get_async_hook.return_value.get_cluster.return_value = future

        task = asyncio.create_task(cluster_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        mock_log.info.assert_called()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_async_hook")
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_sync_hook")
    @mock.patch.object(DataprocClusterTrigger, "log")
    async def test_cluster_trigger_cancellation_handling(
        self, mock_log, mock_get_sync_hook, mock_get_async_hook
    ):
        cluster = Cluster(status=ClusterStatus(state=ClusterStatus.State.RUNNING))
        mock_get_async_hook.return_value.get_cluster.return_value = asyncio.Future()
        mock_get_async_hook.return_value.get_cluster.return_value.set_result(cluster)

        mock_delete_cluster = mock.MagicMock()
        mock_get_sync_hook.return_value.delete_cluster = mock_delete_cluster

        cluster_trigger = DataprocClusterTrigger(
            cluster_name="cluster_name",
            project_id="project-id",
            region="region",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
            polling_interval_seconds=5,
            delete_on_error=True,
        )

        cluster_trigger_gen = cluster_trigger.run()

        try:
            await cluster_trigger_gen.__anext__()
            await cluster_trigger_gen.aclose()

        except asyncio.CancelledError:
            # Verify that cancellation was handled as expected
            if cluster_trigger.delete_on_error:
                mock_get_sync_hook.assert_called_once()
                mock_delete_cluster.assert_called_once_with(
                    region=cluster_trigger.region,
                    cluster_name=cluster_trigger.cluster_name,
                    project_id=cluster_trigger.project_id,
                )
                mock_log.info.assert_called()
            else:
                mock_delete_cluster.assert_not_called()
        except Exception as e:
            pytest.fail(f"Unexpected exception raised: {e}")

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_async_hook")
    async def test_fetch_cluster_status(self, mock_get_async_hook, cluster_trigger):
        mock_cluster = mock.MagicMock()
        mock_cluster.status = ClusterStatus(state=ClusterStatus.State.RUNNING)

        future = asyncio.Future()
        future.set_result(mock_cluster)
        mock_get_async_hook.return_value.get_cluster.return_value = future

        cluster = await cluster_trigger.fetch_cluster()

        assert cluster.status.state == ClusterStatus.State.RUNNING, "The cluster state should be RUNNING"

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_async_hook")
    @mock.patch.object(DataprocClusterTrigger, "log")
    async def test_delete_when_error_occurred(self, mock_log, mock_get_async_hook, cluster_trigger):
        mock_cluster = mock.MagicMock(spec=Cluster)
        type(mock_cluster).status = mock.PropertyMock(
            return_value=mock.MagicMock(state=ClusterStatus.State.ERROR)
        )

        mock_delete_future = asyncio.Future()
        mock_delete_future.set_result(None)
        mock_get_async_hook.return_value.delete_cluster.return_value = mock_delete_future

        cluster_trigger.delete_on_error = True

        await cluster_trigger.delete_when_error_occurred(mock_cluster)

        mock_get_async_hook.return_value.delete_cluster.assert_called_once_with(
            region=cluster_trigger.region,
            cluster_name=cluster_trigger.cluster_name,
            project_id=cluster_trigger.project_id,
        )

        mock_get_async_hook.return_value.delete_cluster.reset_mock()
        cluster_trigger.delete_on_error = False

        await cluster_trigger.delete_when_error_occurred(mock_cluster)

        mock_get_async_hook.return_value.delete_cluster.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_async_hook")
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.get_sync_hook")
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger.safe_to_cancel")
    @mock.patch.object(DataprocClusterTrigger, "log")
    async def test_cluster_trigger_run_cancelled_not_safe_to_cancel(
        self, mock_log, mock_safe_to_cancel, mock_get_sync_hook, mock_get_async_hook, cluster_trigger
    ):
        """Test the trigger's cancellation behavior when it is not safe to cancel."""
        mock_safe_to_cancel.return_value = False
        cluster = Cluster(status=ClusterStatus(state=ClusterStatus.State.RUNNING))
        future_cluster = asyncio.Future()
        future_cluster.set_result(cluster)
        mock_get_async_hook.return_value.get_cluster.return_value = future_cluster

        mock_delete_cluster = mock.MagicMock()
        mock_get_sync_hook.return_value.delete_cluster = mock_delete_cluster

        cluster_trigger.delete_on_error = True

        async_gen = cluster_trigger.run()
        task = asyncio.create_task(async_gen.__anext__())
        await sleep(0)
        task.cancel()

        with contextlib.suppress(CancelledError):
            await task

        assert mock_delete_cluster.call_count == 0
        mock_delete_cluster.assert_not_called()


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

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBatchTrigger.get_async_hook")
    async def test_async_create_batch_trigger_triggers_on_success_should_execute_successfully(
        self, mock_get_async_hook, batch_trigger
    ):
        """
        Tests the DataprocBatchTrigger only fires once the batch execution reaches a successful state.
        """

        mock_batch = mock.MagicMock()
        mock_batch.state = Batch.State.SUCCEEDED
        mock_batch.state_message = TEST_BATCH_STATE_MESSAGE

        future = asyncio.Future()
        future.set_result(mock_batch)
        mock_get_async_hook.return_value.get_batch.return_value = future

        expected_event = TriggerEvent(
            {
                "batch_id": TEST_BATCH_ID,
                "batch_state": Batch.State.SUCCEEDED.name,
                "batch_state_message": TEST_BATCH_STATE_MESSAGE,
            }
        )

        actual_event = await batch_trigger.run().asend(None)
        await asyncio.sleep(0.5)
        assert expected_event == actual_event

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBatchTrigger.get_async_hook")
    async def test_async_create_batch_trigger_run_returns_failed_event(
        self, mock_get_async_hook, batch_trigger
    ):
        mock_batch = mock.MagicMock()
        mock_batch.state = Batch.State.FAILED
        mock_batch.state_message = TEST_BATCH_STATE_MESSAGE

        future = asyncio.Future()
        future.set_result(mock_batch)
        mock_get_async_hook.return_value.get_batch.return_value = future

        expected_event = TriggerEvent(
            {
                "batch_id": TEST_BATCH_ID,
                "batch_state": Batch.State.FAILED.name,
                "batch_state_message": TEST_BATCH_STATE_MESSAGE,
            }
        )

        actual_event = await batch_trigger.run().asend(None)
        await asyncio.sleep(0.5)
        assert expected_event == actual_event

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBatchTrigger.get_async_hook")
    async def test_create_batch_run_returns_cancelled_event(self, mock_get_async_hook, batch_trigger):
        mock_batch = mock.MagicMock()
        mock_batch.state = Batch.State.CANCELLED
        mock_batch.state_message = TEST_BATCH_STATE_MESSAGE

        future = asyncio.Future()
        future.set_result(mock_batch)
        mock_get_async_hook.return_value.get_batch.return_value = future

        expected_event = TriggerEvent(
            {
                "batch_id": TEST_BATCH_ID,
                "batch_state": Batch.State.CANCELLED.name,
                "batch_state_message": TEST_BATCH_STATE_MESSAGE,
            }
        )

        actual_event = await batch_trigger.run().asend(None)
        await asyncio.sleep(0.5)
        assert expected_event == actual_event

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocBatchTrigger.get_async_hook")
    @mock.patch.object(DataprocBatchTrigger, "log")
    async def test_create_batch_run_loop_is_still_running(self, mock_log, mock_get_async_hook, batch_trigger):
        mock_batch = mock.MagicMock()
        mock_batch.state = Batch.State.RUNNING

        future = asyncio.Future()
        future.set_result(mock_batch)
        mock_get_async_hook.return_value.get_batch.return_value = future

        task = asyncio.create_task(batch_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        mock_log.info.assert_called()


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
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocOperationTrigger.get_async_hook")
    async def test_async_operation_triggers_on_success_should_execute_successfully(
        self, mock_get_async_hook, operation_trigger
    ):
        mock_operation = mock.MagicMock()
        mock_operation.name = TEST_OPERATION_NAME
        mock_operation.done = True
        mock_operation.response = {}
        mock_operation.error = Status(message="")

        future = asyncio.Future()
        future.set_result(mock_operation)
        mock_get_async_hook.return_value.get_operation.return_value = future

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
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocOperationTrigger.get_async_hook")
    async def test_async_diagnose_operation_triggers_on_success_should_execute_successfully(
        self, mock_get_async_hook, diagnose_operation_trigger
    ):
        gcs_uri = "gs://test-tarball-gcs-dir-bucket"
        mock_operation = mock.MagicMock()
        mock_operation.name = TEST_OPERATION_NAME
        mock_operation.done = True
        mock_operation.response = Any(value=gcs_uri.encode("utf-8"))
        mock_operation.error = Status(message="")

        future = asyncio.Future()
        future.set_result(mock_operation)
        mock_get_async_hook.return_value.get_operation.return_value = future

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
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocOperationTrigger.get_async_hook")
    async def test_async_operation_triggers_on_error(self, mock_get_async_hook, operation_trigger):
        mock_operation = mock.MagicMock()
        mock_operation.name = TEST_OPERATION_NAME
        mock_operation.done = True
        mock_operation.response = {}
        mock_operation.error = Status(message="test_error")

        future = asyncio.Future()
        future.set_result(mock_operation)
        mock_get_async_hook.return_value.get_operation.return_value = future

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


class TestDataprocSubmitTrigger:
    def test_submit_trigger_serialization(self, submit_trigger):
        """Test that the trigger serializes its configuration correctly."""
        classpath, kwargs = submit_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger"
        assert kwargs == {
            "job_id": TEST_JOB_ID,
            "project_id": TEST_PROJECT_ID,
            "region": TEST_REGION,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "polling_interval_seconds": TEST_POLL_INTERVAL,
            "cancel_on_kill": True,
            "impersonation_chain": None,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger.get_async_hook")
    async def test_submit_trigger_run_success(self, mock_get_async_hook, submit_trigger):
        """Test the trigger correctly handles a job completion."""
        mock_job = Job(status=JobStatus(state=JobStatus.State.DONE))
        future = asyncio.Future()
        future.set_result(mock_job)
        mock_get_async_hook.return_value.get_job.return_value = future

        async_gen = submit_trigger.run()
        event = await async_gen.asend(None)
        expected_event = TriggerEvent(
            {"job_id": TEST_JOB_ID, "job_state": JobStatus.State.DONE.name, "job": Job.to_dict(mock_job)}
        )
        assert event.payload == expected_event.payload

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger.get_async_hook")
    async def test_submit_trigger_run_error(self, mock_get_async_hook, submit_trigger):
        """Test the trigger correctly handles a job error."""
        mock_job = Job(status=JobStatus(state=JobStatus.State.ERROR))
        future = asyncio.Future()
        future.set_result(mock_job)
        mock_get_async_hook.return_value.get_job.return_value = future

        async_gen = submit_trigger.run()
        event = await async_gen.asend(None)
        expected_event = TriggerEvent(
            {"job_id": TEST_JOB_ID, "job_state": JobStatus.State.ERROR.name, "job": Job.to_dict(mock_job)}
        )
        assert event.payload == expected_event.payload

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_safe_to_cancel", [True, False])
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger.get_async_hook")
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger.get_sync_hook")
    @mock.patch("airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger.safe_to_cancel")
    async def test_submit_trigger_run_cancelled(
        self, mock_safe_to_cancel, mock_get_sync_hook, mock_get_async_hook, submit_trigger, is_safe_to_cancel
    ):
        """Test the trigger correctly handles an asyncio.CancelledError."""
        mock_safe_to_cancel.return_value = is_safe_to_cancel
        mock_async_hook = mock_get_async_hook.return_value
        mock_async_hook.get_job.side_effect = asyncio.CancelledError

        mock_sync_hook = mock_get_sync_hook.return_value
        mock_sync_hook.cancel_job = mock.MagicMock()

        async_gen = submit_trigger.run()

        try:
            await async_gen.asend(None)
            # Should raise StopAsyncIteration if no more items to yield
            await async_gen.asend(None)
        except asyncio.CancelledError:
            # Handle the cancellation as expected
            pass
        except StopAsyncIteration:
            # The generator should be properly closed after handling the cancellation
            pass
        except Exception as e:
            # Catch any other exceptions that should not occur
            pytest.fail(f"Unexpected exception raised: {e}")

        # Check if cancel_job was correctly called
        if submit_trigger.cancel_on_kill and is_safe_to_cancel:
            mock_sync_hook.cancel_job.assert_called_once_with(
                job_id=submit_trigger.job_id,
                project_id=submit_trigger.project_id,
                region=submit_trigger.region,
            )
        else:
            mock_sync_hook.cancel_job.assert_not_called()

        # Clean up the generator
        await async_gen.aclose()
