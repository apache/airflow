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

from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftCreateClusterSnapshotTrigger,
    RedshiftCreateClusterTrigger,
    RedshiftDeleteClusterTrigger,
    RedshiftPauseClusterTrigger,
    RedshiftResumeClusterTrigger,
)
from airflow.triggers.base import TriggerEvent

TEST_CLUSTER_IDENTIFIER = "test-cluster"
TEST_POLL_INTERVAL = 10
TEST_MAX_ATTEMPT = 10
TEST_AWS_CONN_ID = "test-aws-id"


class TestRedshiftCreateClusterTrigger:
    def test_redshift_create_cluster_trigger_serialize(self):
        redshift_create_cluster_trigger = RedshiftCreateClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempt=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )
        class_path, args = redshift_create_cluster_trigger.serialize()
        assert (
            class_path
            == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterTrigger"
        )
        assert args["cluster_identifier"] == TEST_CLUSTER_IDENTIFIER
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempt"] == str(TEST_MAX_ATTEMPT)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.async_conn")
    async def test_redshift_create_cluster_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        a_mock.get_waiter().wait = AsyncMock()

        redshift_create_cluster_trigger = RedshiftCreateClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempt=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_create_cluster_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Cluster Created"})


class TestRedshiftPauseClusterTrigger:
    def test_redshift_pause_cluster_trigger_serialize(self):
        redshift_pause_cluster_trigger = RedshiftPauseClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )
        class_path, args = redshift_pause_cluster_trigger.serialize()
        assert (
            class_path == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftPauseClusterTrigger"
        )
        assert args["cluster_identifier"] == TEST_CLUSTER_IDENTIFIER
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempts"] == str(TEST_MAX_ATTEMPT)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID

    @pytest.mark.asyncio
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_pause_cluster_trigger_run(self, mock_async_conn, mock_get_waiter):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        mock_get_waiter().wait = AsyncMock()

        redshift_pause_cluster_trigger = RedshiftPauseClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_pause_cluster_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Cluster paused"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_pause_cluster_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_pause_cluster_trigger = RedshiftPauseClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_pause_cluster_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 3
        assert response == TriggerEvent({"status": "success", "message": "Cluster paused"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_pause_cluster_trigger_run_attempts_exceeded(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_pause_cluster_trigger = RedshiftPauseClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=2,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_pause_cluster_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 2
        assert response == TriggerEvent(
            {"status": "failure", "message": "Pause Cluster Failed - max attempts reached."}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_pause_cluster_trigger_run_attempts_failed(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_available = WaiterError(
            name="test_name",
            reason="Max attempts exceeded",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(
            side_effect=[error_available, error_available, error_failed]
        )
        mock_sleep.return_value = True

        redshift_pause_cluster_trigger = RedshiftPauseClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_pause_cluster_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {"status": "failure", "message": f"Pause Cluster Failed: {error_failed}"}
        )


class TestRedshiftCreateClusterSnapshotTrigger:
    def test_redshift_create_cluster_snapshot_trigger_serialize(self):
        redshift_create_cluster_trigger = RedshiftCreateClusterSnapshotTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )
        class_path, args = redshift_create_cluster_trigger.serialize()
        assert (
            class_path
            == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterSnapshotTrigger"
        )
        assert args["cluster_identifier"] == TEST_CLUSTER_IDENTIFIER
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempts"] == str(TEST_MAX_ATTEMPT)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID

    @pytest.mark.asyncio
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_create_cluster_snapshot_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        a_mock.get_waiter().wait = AsyncMock()

        redshift_create_cluster_trigger = RedshiftCreateClusterSnapshotTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_create_cluster_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Cluster Snapshot Created"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_create_cluster_snapshot_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Snapshots": [{"Status": "available"}]},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_create_cluster_snapshot_trigger = RedshiftCreateClusterSnapshotTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_create_cluster_snapshot_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 3
        assert response == TriggerEvent({"status": "success", "message": "Cluster Snapshot Created"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_create_cluster_snapshot_trigger_run_attempts_exceeded(
        self, mock_async_conn, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Snapshots": [{"Status": "available"}]},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_create_cluster_snapshot_trigger = RedshiftCreateClusterSnapshotTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=2,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_create_cluster_snapshot_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 2
        assert response == TriggerEvent(
            {"status": "failure", "message": "Create Cluster Snapshot Cluster Failed - max attempts reached."}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_create_cluster_snapshot_trigger_run_attempts_failed(
        self, mock_async_conn, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_available = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Snapshots": [{"Status": "available"}]},
        )

        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"Snapshots": [{"Status": "available"}]},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(
            side_effect=[error_available, error_available, error_failed]
        )
        mock_sleep.return_value = True

        redshift_create_cluster_snapshot_trigger = RedshiftCreateClusterSnapshotTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_create_cluster_snapshot_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {"status": "failure", "message": f"Create Cluster Snapshot Failed: {error_failed}"}
        )


class TestRedshiftResumeClusterTrigger:
    def test_redshift_resume_cluster_trigger_serialize(self):
        redshift_resume_cluster_trigger = RedshiftResumeClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )
        class_path, args = redshift_resume_cluster_trigger.serialize()
        assert (
            class_path
            == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftResumeClusterTrigger"
        )
        assert args["cluster_identifier"] == TEST_CLUSTER_IDENTIFIER
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempts"] == str(TEST_MAX_ATTEMPT)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID

    @pytest.mark.asyncio
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_resume_cluster_trigger_run(self, mock_async_conn, mock_get_waiter):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        mock_get_waiter().wait = AsyncMock()

        redshift_resume_cluster_trigger = RedshiftResumeClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_resume_cluster_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Cluster resumed"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_resume_cluster_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_resume_cluster_trigger = RedshiftResumeClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_resume_cluster_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 3
        assert response == TriggerEvent({"status": "success", "message": "Cluster resumed"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_resume_cluster_trigger_run_attempts_exceeded(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_resume_cluster_trigger = RedshiftResumeClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=2,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_resume_cluster_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 2
        assert response == TriggerEvent(
            {"status": "failure", "message": "Resume Cluster Failed - max attempts reached."}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_resume_cluster_trigger_run_attempts_failed(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_available = WaiterError(
            name="test_name",
            reason="Max attempts exceeded",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(
            side_effect=[error_available, error_available, error_failed]
        )
        mock_sleep.return_value = True

        redshift_resume_cluster_trigger = RedshiftResumeClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_resume_cluster_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {"status": "failure", "message": f"Resume Cluster Failed: {error_failed}"}
        )


class TestRedshiftDeleteClusterTrigger:
    def test_redshift_delete_cluster_trigger_serialize(self):
        redshift_delete_cluster_trigger = RedshiftDeleteClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )
        class_path, args = redshift_delete_cluster_trigger.serialize()
        assert (
            class_path
            == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftDeleteClusterTrigger"
        )
        assert args["cluster_identifier"] == TEST_CLUSTER_IDENTIFIER
        assert args["poll_interval"] == TEST_POLL_INTERVAL
        assert args["max_attempts"] == TEST_MAX_ATTEMPT
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.async_conn")
    async def test_redshift_delete_cluster_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        a_mock.get_waiter().wait = AsyncMock()

        redshift_delete_cluster_trigger = RedshiftDeleteClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_delete_cluster_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Cluster deleted."})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_delete_cluster_trigger_run_multiple_attempts(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_delete_cluster_trigger = RedshiftDeleteClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_delete_cluster_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 3
        assert response == TriggerEvent({"status": "success", "message": "Cluster deleted."})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_delete_cluster_trigger_run_attempts_exceeded(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Clusters": [{"ClusterStatus": "deleting"}]},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        redshift_delete_cluster_trigger = RedshiftDeleteClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=2,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_delete_cluster_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 2
        assert response == TriggerEvent(
            {"status": "failure", "message": "Delete Cluster Failed - max attempts reached."}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RedshiftHook, "async_conn")
    async def test_redshift_delete_cluster_trigger_run_attempts_failed(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        error_available = WaiterError(
            name="test_name",
            reason="Max attempts exceeded",
            last_response={"Clusters": [{"ClusterStatus": "deleting"}]},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"Clusters": [{"ClusterStatus": "available"}]},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(
            side_effect=[error_available, error_available, error_failed]
        )
        mock_sleep.return_value = True

        redshift_delete_cluster_trigger = RedshiftDeleteClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_delete_cluster_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {"status": "failure", "message": f"Delete Cluster Failed: {error_failed}"}
        )
