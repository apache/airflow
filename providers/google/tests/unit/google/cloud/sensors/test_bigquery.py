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
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud.bigquery.table import Table as BQTable

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryStreamingBufferEmptySensor,
    BigQueryTableExistenceSensor,
    BigQueryTablePartitionExistenceSensor,
)
from airflow.providers.google.cloud.triggers.bigquery import (
    BigQueryStreamingBufferEmptyTrigger,
    BigQueryTableExistenceTrigger,
    BigQueryTablePartitionExistenceTrigger,
)

TEST_PROJECT_ID = "test_project"
TEST_DATASET_ID = "test_dataset"
TEST_TABLE_ID = "test_table"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_PARTITION_ID = "20200101"
TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestBigqueryTableExistenceSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_passing_arguments_to_hook(self, mock_hook):
        task = BigQueryTableExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_exists.return_value = True
        results = task.poke(mock.MagicMock())

        assert results is True

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_exists.assert_called_once_with(
            project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID, table_id=TEST_TABLE_ID
        )

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor.defer")
    def test_table_existence_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        task = BigQueryTableExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        mock_hook.return_value.table_exists.return_value = True
        task.execute(mock.MagicMock())
        assert not mock_defer.called

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_execute_deferred(self, mock_hook):
        """
        Asserts that a task is deferred and a BigQueryTableExistenceTrigger will be fired
        when the BigQueryTableExistenceSensor is executed.
        """
        task = BigQueryTableExistenceSensor(
            task_id="check_table_exists",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            deferrable=True,
        )
        mock_hook.return_value.table_exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(mock.MagicMock())
        assert isinstance(exc.value.trigger, BigQueryTableExistenceTrigger), (
            "Trigger is not a BigQueryTableExistenceTrigger"
        )

    def test_execute_deferred_failure(self):
        """Tests that an expected exception is raised in case of error event"""
        task = BigQueryTableExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context={}, event={"status": "error", "message": "test failure message"})

    def test_execute_complete(self):
        """Asserts that logging occurs as expected"""
        task = BigQueryTableExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            deferrable=True,
        )
        table_uri = f"{TEST_PROJECT_ID}:{TEST_DATASET_ID}.{TEST_TABLE_ID}"
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context={}, event={"status": "success", "message": "Job completed"})
        mock_log_info.assert_called_with("Sensor checks existence of table: %s", table_uri)

    def test_execute_defered_complete_event_none(self):
        """Asserts that logging occurs as expected"""
        task = BigQueryTableExistenceSensor(
            task_id="task-id", project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID, table_id=TEST_TABLE_ID
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context={}, event=None)


class TestBigqueryTablePartitionExistenceSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_passing_arguments_to_hook(self, mock_hook):
        task = BigQueryTablePartitionExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_partition_exists.return_value = True
        results = task.poke(mock.MagicMock())

        assert results is True

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_partition_exists.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
        )

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryTablePartitionExistenceSensor.defer")
    def test_table_partition_existence_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        """
        Asserts that a task is deferred and a BigQueryTablePartitionExistenceTrigger will be fired
        when the BigQueryTablePartitionExistenceSensor is executed and deferrable is set to True.
        """
        task = BigQueryTablePartitionExistenceSensor(
            task_id="test_task_id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
            deferrable=True,
        )
        mock_hook.return_value.table_partition_exists.return_value = True
        task.execute(mock.MagicMock())
        assert not mock_defer.called

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_execute_with_deferrable_mode(self, mock_hook):
        """
        Asserts that a task is deferred and a BigQueryTablePartitionExistenceTrigger will be fired
        when the BigQueryTablePartitionExistenceSensor is executed and deferrable is set to True.
        """
        task = BigQueryTablePartitionExistenceSensor(
            task_id="test_task_id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
            deferrable=True,
        )
        mock_hook.return_value.table_partition_exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context={})
        assert isinstance(exc.value.trigger, BigQueryTablePartitionExistenceTrigger), (
            "Trigger is not a BigQueryTablePartitionExistenceTrigger"
        )

    def test_execute_with_deferrable_mode_execute_failure(self):
        """Tests that an AirflowException is raised in case of error event"""
        task = BigQueryTablePartitionExistenceSensor(
            task_id="test_task_id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context={}, event={"status": "error", "message": "test failure message"})

    def test_execute_complete_event_none(self):
        """Asserts that logging occurs as expected"""
        task = BigQueryTablePartitionExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
            deferrable=True,
        )
        with pytest.raises(AirflowException, match="No event received in trigger callback"):
            task.execute_complete(context={}, event=None)

    def test_execute_complete(self):
        """Asserts that logging occurs as expected"""
        task = BigQueryTablePartitionExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
            deferrable=True,
        )
        table_uri = f"{TEST_PROJECT_ID}:{TEST_DATASET_ID}.{TEST_TABLE_ID}"
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context={}, event={"status": "success", "message": "test"})
        mock_log_info.assert_called_with(
            'Sensor checks existence of partition: "%s" in table: %s', TEST_PARTITION_ID, table_uri
        )


# BigQuery REST API Tables.get response fixtures used to build real SDK objects
# via google.cloud.bigquery.table.Table.from_api_repr().
# Captured from a real BigQuery table (student-00343:test.Customer) on 2026-02-16.
# See https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource-table
BQ_TABLE_RESOURCE_NO_BUFFER: dict = {
    "kind": "bigquery#table",
    "etag": "HBcs4StKiCVYjqarES8vAg==",
    "id": f"{TEST_PROJECT_ID}:{TEST_DATASET_ID}.{TEST_TABLE_ID}",
    "selfLink": (
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{TEST_PROJECT_ID}"
        f"/datasets/{TEST_DATASET_ID}/tables/{TEST_TABLE_ID}"
    ),
    "tableReference": {
        "projectId": TEST_PROJECT_ID,
        "datasetId": TEST_DATASET_ID,
        "tableId": TEST_TABLE_ID,
    },
    "schema": {
        "fields": [
            {"name": "customor_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        ]
    },
    "numBytes": "0",
    "numLongTermBytes": "0",
    "numRows": "0",
    "creationTime": "1771225583467",
    "lastModifiedTime": "1771225583534",
    "type": "TABLE",
    "location": "US",
    "numTotalLogicalBytes": "0",
    "numActiveLogicalBytes": "0",
    "numLongTermLogicalBytes": "0",
}

BQ_TABLE_RESOURCE_WITH_BUFFER: dict = {
    **BQ_TABLE_RESOURCE_NO_BUFFER,
    "streamingBuffer": {
        "estimatedBytes": "302",
        "estimatedRows": "2",
        "oldestEntryTime": "1771225672524",
    },
}


class TestBigQueryStreamingBufferEmptySensor:
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_poke_buffer_present(self, mock_hook):
        """Streaming buffer still present → poke returns False."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        # Return a real google.cloud.bigquery.table.Table with a StreamingBuffer
        bq_table = BQTable.from_api_repr(BQ_TABLE_RESOURCE_WITH_BUFFER)
        mock_hook.return_value.get_client.return_value.get_table.return_value = bq_table

        result = task.poke(mock.MagicMock())

        assert result is False
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_client.assert_called_once_with(project_id=TEST_PROJECT_ID)
        mock_hook.return_value.get_client.return_value.get_table.assert_called_once_with(
            f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}"
        )

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_poke_buffer_empty(self, mock_hook):
        """Streaming buffer is None → poke returns True."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        # Return a real google.cloud.bigquery.table.Table without streamingBuffer
        bq_table = BQTable.from_api_repr(BQ_TABLE_RESOURCE_NO_BUFFER)
        mock_hook.return_value.get_client.return_value.get_table.return_value = bq_table

        result = task.poke(mock.MagicMock())

        assert result is True

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_poke_table_not_found_raises_value_error(self, mock_hook):
        """google.api_core.exceptions.NotFound (404) is wrapped into ValueError."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        # google.cloud.bigquery.Client.get_table raises google.api_core.exceptions.NotFound
        # when the table does not exist.  Its string repr contains "not found".
        mock_hook.return_value.get_client.return_value.get_table.side_effect = NotFound(
            f"Not found: Table {TEST_PROJECT_ID}:{TEST_DATASET_ID}.{TEST_TABLE_ID}"
        )

        with pytest.raises(ValueError, match="not found"):
            task.poke(mock.MagicMock())

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_poke_unexpected_error_reraised(self, mock_hook):
        """google.api_core.exceptions.Forbidden (403) is re-raised as-is."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        # google.cloud.bigquery.Client.get_table raises google.api_core.exceptions.Forbidden
        # when the caller lacks bigquery.tables.get permission.
        mock_hook.return_value.get_client.return_value.get_table.side_effect = Forbidden(
            f"Access Denied: Table {TEST_PROJECT_ID}:{TEST_DATASET_ID}.{TEST_TABLE_ID}: "
            "Permission bigquery.tables.get denied"
        )

        with pytest.raises(Forbidden):
            task.poke(mock.MagicMock())

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_execute_non_deferrable_calls_super(self, mock_hook):
        """Non-deferrable execute delegates to BaseSensorOperator.execute (poke loop)."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            deferrable=False,
        )
        bq_table = BQTable.from_api_repr(BQ_TABLE_RESOURCE_NO_BUFFER)
        mock_hook.return_value.get_client.return_value.get_table.return_value = bq_table

        # super().execute() will call poke, which returns True → sensor completes
        task.execute(mock.MagicMock())

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryStreamingBufferEmptySensor.defer")
    def test_execute_deferrable_already_empty(self, mock_defer, mock_hook):
        """Deferrable execute: if poke returns True immediately, defer is NOT called."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            deferrable=True,
        )
        bq_table = BQTable.from_api_repr(BQ_TABLE_RESOURCE_NO_BUFFER)
        mock_hook.return_value.get_client.return_value.get_table.return_value = bq_table

        task.execute(mock.MagicMock())
        assert not mock_defer.called

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_execute_deferrable_triggers_defer(self, mock_hook):
        """Deferrable execute: if buffer present, task defers with correct trigger."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            deferrable=True,
        )
        # Return a real google.cloud.bigquery.table.Table with a StreamingBuffer
        bq_table = BQTable.from_api_repr(BQ_TABLE_RESOURCE_WITH_BUFFER)
        mock_hook.return_value.get_client.return_value.get_table.return_value = bq_table

        with pytest.raises(TaskDeferred) as exc:
            task.execute(mock.MagicMock())
        assert isinstance(exc.value.trigger, BigQueryStreamingBufferEmptyTrigger)

    def test_execute_complete_success(self):
        """execute_complete with success event returns the message."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )
        result = task.execute_complete(context={}, event={"status": "success", "message": "Buffer is empty"})
        assert result == "Buffer is empty"

    def test_execute_complete_error_raises_runtime_error(self):
        """execute_complete with error event raises RuntimeError."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )
        with pytest.raises(RuntimeError, match="something went wrong"):
            task.execute_complete(context={}, event={"status": "error", "message": "something went wrong"})

    def test_execute_complete_none_event_raises_runtime_error(self):
        """execute_complete with None event raises RuntimeError."""
        task = BigQueryStreamingBufferEmptySensor(
            task_id="test-streaming-buffer",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )
        with pytest.raises(RuntimeError, match="No event received in trigger callback"):
            task.execute_complete(context={}, event=None)
