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
#
import unittest
from unittest import mock
from airflow.providers.databricks.sensors.databricks import  \
    (DatabricksDeltaTableChangeSensor, DatabricksPartitionTableSensor)
from databricks.sql.types import Row


DEFAULT_CONN_ID = 'databricks_default'


class TestDatabricksDeltaTableChangeSensor(unittest.TestCase):

    @mock.patch('airflow.providers.databricks.sensors.databricks.DatabricksSqlHook')
    def test_exec_success_sensor_find_new_events(self, db_mock_class):
        """
        Test the sensor when find a new event from the Databricks Delta Table
        """

        sensor = DatabricksDeltaTableChangeSensor(
            task_id='run_now',
            databricks_conn_id='databricks_default',
            table='table_test',
            timestamp='2022-05-07 21:24:58.680358'
        )

        db_mock = db_mock_class.return_value
        mock_schema = [('new_events',)]
        mock_results = [Row(new_events=1)]
        db_mock.run.return_value = (mock_schema, mock_results)

        results = sensor.poke(None)

        assert results

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
        )
        sql = 'SELECT COUNT(1) as new_events from ' \
              '(DESCRIBE HISTORY default.table_test) ' \
              'WHERE timestamp > "2022-05-07 21:24:58.680358"'
        db_mock.run.assert_called_once_with(sql)

    @mock.patch('airflow.providers.databricks.sensors.databricks.DatabricksSqlHook')
    def test_exec_success_sensor_not_find_new_events(self, db_mock_class):
        """
        Test the sensor when does not find a new event from the Databricks Delta Table
        """

        sensor = DatabricksDeltaTableChangeSensor(
            task_id='run_now',
            databricks_conn_id='databricks_default',
            table='table_test_2',
            timestamp='2022-05-07 21:24:58.680358'
        )

        db_mock = db_mock_class.return_value
        mock_schema = [('new_events',)]
        mock_results = [Row(new_events=0)]
        db_mock.run.return_value = (mock_schema, mock_results)

        results = sensor.poke(None)

        assert not results

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
        )
        sql = 'SELECT COUNT(1) as new_events from ' \
              '(DESCRIBE HISTORY default.table_test_2) ' \
              'WHERE timestamp > "2022-05-07 21:24:58.680358"'
        db_mock.run.assert_called_once_with(sql)


class TestDatabricksPartitionTableSensor(unittest.TestCase):

    @mock.patch('airflow.providers.databricks.sensors.databricks.DatabricksSqlHook')
    def test_exec_success_sensor_find_partition(self, db_mock_class):
        """
        Test the sensor when find the partition from the Databricks Delta Table
        """

        sensor = DatabricksPartitionTableSensor(
            task_id='run_now',
            databricks_conn_id='databricks_default',
            table='table_test',
            partition='partition_test'
        )

        db_mock = db_mock_class.return_value
        mock_schema = [('partition_test',)]
        mock_results = [Row(partition_test='test_value')]
        db_mock.run.return_value = (mock_schema, mock_results)

        results = sensor.poke(None)

        assert results

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
        )
        sql = 'SHOW PARTITIONS default.table_test'
        db_mock.run.assert_called_once_with(sql)

    @mock.patch('airflow.providers.databricks.sensors.databricks.DatabricksSqlHook')
    def test_exec_success_sensor_not_find_partition(self, db_mock_class):
        """
        Test the sensor when does not find the partition from the Databricks Delta Table
        """

        sensor = DatabricksPartitionTableSensor(
            task_id='run_now',
            databricks_conn_id='databricks_default',
            table='table_test',
            partition='partition_test'
        )

        db_mock = db_mock_class.return_value
        mock_schema = [('partition_test_2',)]
        mock_results = [Row(partition_test_2='test_value')]
        db_mock.run.return_value = (mock_schema, mock_results)

        results = sensor.poke(None)

        assert not results

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
        )
        sql = 'SHOW PARTITIONS default.table_test'
        db_mock.run.assert_called_once_with(sql)
