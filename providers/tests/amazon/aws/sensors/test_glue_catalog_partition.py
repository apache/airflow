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

from unittest import mock

import pytest
from moto import mock_aws

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.amazon.aws.sensors.glue_catalog_partition import (
    GlueCatalogPartitionSensor,
)


class TestGlueCatalogPartitionSensor:
    task_id = "test_glue_catalog_partition_sensor"

    @mock_aws
    @mock.patch.object(GlueCatalogHook, "check_for_partition")
    def test_poke(self, mock_check_for_partition):
        mock_check_for_partition.return_value = True
        op = GlueCatalogPartitionSensor(task_id=self.task_id, table_name="tbl")
        assert op.poke({})

    @mock_aws
    @mock.patch.object(GlueCatalogHook, "check_for_partition")
    def test_poke_false(self, mock_check_for_partition):
        mock_check_for_partition.return_value = False
        op = GlueCatalogPartitionSensor(task_id=self.task_id, table_name="tbl")
        assert not op.poke({})

    @mock_aws
    @mock.patch.object(GlueCatalogHook, "check_for_partition")
    def test_poke_default_args(self, mock_check_for_partition):
        table_name = "test_glue_catalog_partition_sensor_tbl"
        op = GlueCatalogPartitionSensor(task_id=self.task_id, table_name=table_name)
        op.poke({})

        assert op.hook.region_name is None
        assert op.hook.aws_conn_id == "aws_default"
        mock_check_for_partition.assert_called_once_with(
            "default", table_name, "ds='{{ ds }}'"
        )

    @mock_aws
    @mock.patch.object(GlueCatalogHook, "check_for_partition")
    def test_poke_nondefault_args(self, mock_check_for_partition):
        table_name = "my_table"
        expression = "col=val"
        aws_conn_id = "my_aws_conn_id"
        region_name = "us-west-2"
        database_name = "my_db"
        poke_interval = 2
        timeout = 3
        op = GlueCatalogPartitionSensor(
            task_id=self.task_id,
            table_name=table_name,
            expression=expression,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            database_name=database_name,
            poke_interval=poke_interval,
            timeout=timeout,
        )
        # We're mocking all actual AWS calls and don't need a connection. This
        # avoids an Airflow warning about connection cannot be found.
        op.hook.get_connection = lambda _: None
        op.poke({})

        assert op.hook.region_name == region_name
        assert op.hook.aws_conn_id == aws_conn_id
        assert op.poke_interval == poke_interval
        assert op.timeout == timeout
        mock_check_for_partition.assert_called_once_with(
            database_name, table_name, expression
        )

    @mock_aws
    @mock.patch.object(GlueCatalogHook, "check_for_partition")
    def test_dot_notation(self, mock_check_for_partition):
        db_table = "my_db.my_tbl"
        op = GlueCatalogPartitionSensor(task_id=self.task_id, table_name=db_table)
        op.poke({})

        mock_check_for_partition.assert_called_once_with(
            "my_db", "my_tbl", "ds='{{ ds }}'"
        )

    def test_deferrable_mode_raises_task_deferred(self):
        op = GlueCatalogPartitionSensor(
            task_id=self.task_id, table_name="tbl", deferrable=True
        )
        with pytest.raises(TaskDeferred):
            op.execute({})

    def test_execute_complete_fails_if_status_is_not_success(self):
        op = GlueCatalogPartitionSensor(
            task_id=self.task_id, table_name="tbl", deferrable=True
        )
        event = {"status": "FAILED"}
        with pytest.raises(AirflowException):
            op.execute_complete(context={}, event=event)

    def test_execute_complete_succeeds_if_status_is_success(self, caplog):
        op = GlueCatalogPartitionSensor(
            task_id=self.task_id, table_name="tbl", deferrable=True
        )
        event = {"status": "success"}
        op.execute_complete(context={}, event=event)
        assert "Partition exists in the Glue Catalog" in caplog.messages

    def test_fail_execute_complete(self):
        op = GlueCatalogPartitionSensor(
            task_id=self.task_id, table_name="tbl", deferrable=True
        )
        event = {"status": "Failed"}
        message = f"Trigger error: event is {event}"
        with pytest.raises(AirflowException, match=message):
            op.execute_complete(context={}, event=event)

    def test_init(self):
        default_op_kwargs = {
            "task_id": "test_task",
            "table_name": "test_table",
        }

        sensor = GlueCatalogPartitionSensor(**default_op_kwargs)
        assert sensor.hook.aws_conn_id == "aws_default"
        assert sensor.hook._region_name is None
        assert sensor.hook._verify is None
        assert sensor.hook._config is None

        sensor = GlueCatalogPartitionSensor(
            **default_op_kwargs,
            aws_conn_id=None,
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert sensor.hook.aws_conn_id is None
        assert sensor.hook._region_name == "eu-west-2"
        assert sensor.hook._verify is True
        assert sensor.hook._config is not None
        assert sensor.hook._config.read_timeout == 42
