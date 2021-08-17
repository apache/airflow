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

import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.glue_catalog import AwsGlueCatalogHook
from airflow.providers.amazon.aws.sensors.glue_catalog_partition import AwsGlueCatalogPartitionSensor

try:
    from moto import mock_glue
except ImportError:
    mock_glue = None


@unittest.skipIf(mock_glue is None, "Skipping test because moto.mock_glue is not available")
class TestAwsGlueCatalogPartitionSensor(unittest.TestCase):

    task_id = 'test_glue_catalog_partition_sensor'

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'check_for_partition')
    def test_poke(self, mock_check_for_partition):
        mock_check_for_partition.return_value = True
        op = AwsGlueCatalogPartitionSensor(task_id=self.task_id, table_name='tbl')
        assert op.poke(None)

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'check_for_partition')
    def test_poke_false(self, mock_check_for_partition):
        mock_check_for_partition.return_value = False
        op = AwsGlueCatalogPartitionSensor(task_id=self.task_id, table_name='tbl')
        assert not op.poke(None)

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'check_for_partition')
    def test_poke_default_args(self, mock_check_for_partition):
        table_name = 'test_glue_catalog_partition_sensor_tbl'
        op = AwsGlueCatalogPartitionSensor(task_id=self.task_id, table_name=table_name)
        op.poke(None)

        assert op.hook.region_name is None
        assert op.hook.aws_conn_id == 'aws_default'
        mock_check_for_partition.assert_called_once_with('default', table_name, "ds='{{ ds }}'")

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'check_for_partition')
    def test_poke_nondefault_args(self, mock_check_for_partition):
        table_name = 'my_table'
        expression = 'col=val'
        aws_conn_id = 'my_aws_conn_id'
        region_name = 'us-west-2'
        database_name = 'my_db'
        poke_interval = 2
        timeout = 3
        op = AwsGlueCatalogPartitionSensor(
            task_id=self.task_id,
            table_name=table_name,
            expression=expression,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            database_name=database_name,
            poke_interval=poke_interval,
            timeout=timeout,
        )
        op.poke(None)

        assert op.hook.region_name == region_name
        assert op.hook.aws_conn_id == aws_conn_id
        assert op.poke_interval == poke_interval
        assert op.timeout == timeout
        mock_check_for_partition.assert_called_once_with(database_name, table_name, expression)

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'check_for_partition')
    def test_dot_notation(self, mock_check_for_partition):
        db_table = 'my_db.my_tbl'
        op = AwsGlueCatalogPartitionSensor(task_id=self.task_id, table_name=db_table)
        op.poke(None)

        mock_check_for_partition.assert_called_once_with('my_db', 'my_tbl', "ds='{{ ds }}'")
