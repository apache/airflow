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

from unittest import TestCase, mock

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceAsyncSensor,
    BigQueryTableExistenceSensor,
    BigQueryTablePartitionExistenceSensor,
)
from airflow.providers.google.cloud.triggers.bigquery import BigQueryTableExistenceTrigger

TEST_PROJECT_ID = "test_project"
TEST_DATASET_ID = "test_dataset"
TEST_TABLE_ID = "test_table"
TEST_DELEGATE_TO = "test_delegate_to"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_PARTITION_ID = "20200101"
TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestBigqueryTableExistenceSensor(TestCase):
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_passing_arguments_to_hook(self, mock_hook):
        task = BigQueryTableExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_exists.return_value = True
        results = task.poke(mock.MagicMock())

        assert results is True

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_exists.assert_called_once_with(
            project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID, table_id=TEST_TABLE_ID
        )


class TestBigqueryTablePartitionExistenceSensor(TestCase):
    @mock.patch("airflow.providers.google.cloud.sensors.bigquery.BigQueryHook")
    def test_passing_arguments_to_hook(self, mock_hook):
        task = BigQueryTablePartitionExistenceSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_partition_exists.return_value = True
        results = task.poke(mock.MagicMock())

        assert results is True

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.table_partition_exists.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            partition_id=TEST_PARTITION_ID,
        )


@pytest.fixture()
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


class TestBigQueryTableExistenceAsyncSensor(TestCase):
    def test_big_query_table_existence_sensor_async(self):
        """
        Asserts that a task is deferred and a BigQueryTableExistenceTrigger will be fired
        when the BigQueryTableExistenceAsyncSensor is executed.
        """
        task = BigQueryTableExistenceAsyncSensor(
            task_id="check_table_exists",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context={})
        assert isinstance(
            exc.value.trigger, BigQueryTableExistenceTrigger
        ), "Trigger is not a BigQueryTableExistenceTrigger"

    def test_big_query_table_existence_sensor_async_execute_failure(self):
        """Tests that an AirflowException is raised in case of error event"""
        task = BigQueryTableExistenceAsyncSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context={}, event={"status": "error", "message": "test failure message"})

    def test_big_query_table_existence_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""
        task = BigQueryTableExistenceAsyncSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )
        table_uri = f"{TEST_PROJECT_ID}:{TEST_DATASET_ID}.{TEST_TABLE_ID}"
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context={}, event={"status": "success", "message": "Job completed"})
        mock_log_info.assert_called_with("Sensor checks existence of table: %s", table_uri)

    def test_big_query_sensor_async_execute_complete_event_none(self):
        """Asserts that logging occurs as expected"""
        task = BigQueryTableExistenceAsyncSensor(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context={}, event=None)
