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

from airflow.exceptions import (
    AirflowException,
    TaskDeferred,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
    BigQueryTablePartitionExistenceSensor,
    BigQueryTableStreamingBufferEmptySensor,
)
from airflow.providers.google.cloud.triggers.bigquery import (
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


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    return context


class TestBigQueryTableStreamingBufferEmptySensor:
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_poke_when_no_streaming_buffer(self, mock_hook):
        """Test sensor returns True when table has no streaming buffer."""
        task = BigQueryTableStreamingBufferEmptySensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

        # Mock table with no streaming buffer
        mock_table = mock.MagicMock()
        mock_table.streaming_buffer = None
        mock_hook.return_value.get_client.return_value.get_table.return_value = mock_table

        result = task.poke(mock.MagicMock())

        assert result is True
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_poke_when_streaming_buffer_exists(self, mock_hook):
        """Test sensor returns False when table has active streaming buffer."""
        task = BigQueryTableStreamingBufferEmptySensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )

        # Mock table with streaming buffer
        mock_table = mock.MagicMock()
        mock_streaming_buffer = mock.MagicMock()
        mock_streaming_buffer.estimated_rows = 1000
        mock_table.streaming_buffer = mock_streaming_buffer
        mock_hook.return_value.get_client.return_value.get_table.return_value = mock_table

        result = task.poke(mock.MagicMock())

        assert result is False

    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_sensor_with_correct_table_reference(self, mock_hook):
        """Test sensor constructs correct table reference."""
        task = BigQueryTableStreamingBufferEmptySensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )

        mock_table = mock.MagicMock()
        mock_table.streaming_buffer = None
        mock_hook.return_value.get_client.return_value.get_table.return_value = mock_table

        task.poke(mock.MagicMock())

        # Verify correct table reference format
        mock_hook.return_value.get_client.return_value.get_table.assert_called_once_with(
            f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}"
        )

