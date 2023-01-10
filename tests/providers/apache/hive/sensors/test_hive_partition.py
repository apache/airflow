#
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

import os
from unittest import mock
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.apache.hive.sensors.hive_partition import HivePartitionAsyncSensor, HivePartitionSensor
from airflow.providers.apache.hive.triggers.hive_partition import HivePartitionTrigger
from tests.providers.apache.hive import DEFAULT_DATE, MockHiveMetastoreHook, TestHiveEnvironment

TEST_TABLE = "test_table"
TEST_PARTITION = "state='FL'"
TEST_METASTORE_CONN_ID = "test_metastore_default"


@pytest.mark.skipif(
    "AIRFLOW_RUNALL_TESTS" not in os.environ, reason="Skipped because AIRFLOW_RUNALL_TESTS is not set"
)
@patch(
    "airflow.providers.apache.hive.sensors.hive_partition.HiveMetastoreHook",
    side_effect=MockHiveMetastoreHook,
)
class TestHivePartitionSensor(TestHiveEnvironment):
    def test_hive_partition_sensor(self, mock_hive_metastore_hook):
        op = HivePartitionSensor(
            task_id="hive_partition_check", table="airflow.static_babynames_partitioned", dag=self.dag
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


@pytest.fixture()
def context():
    """Creates an empty context."""
    context = {}
    yield context


def test_hive_partition_sensor_async():
    """
    Asserts that a task is deferred and a HivePartitionTrigger will be fired
    when the HivePartitionAsyncSensor is executed.
    """
    task = HivePartitionAsyncSensor(
        task_id="task-id",
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, HivePartitionTrigger), "Trigger is not a HivePartitionTrigger"


def test_hive_partition_sensor_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = HivePartitionAsyncSensor(
        task_id="task-id",
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_hive_partition_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = HivePartitionAsyncSensor(
        task_id="task-id",
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
    mock_log_info.assert_called_with(
        "Success criteria met. Found partition %s in table: %s", TEST_PARTITION, TEST_TABLE
    )


def test_hive_partition_sensor_async_execute_failure_no_event(context):
    """Tests that an AirflowException is raised in case of no event"""
    task = HivePartitionAsyncSensor(
        task_id="task-id",
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event=None)
