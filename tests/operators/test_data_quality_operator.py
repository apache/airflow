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
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from airflow import DAG, AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Connection, TaskInstance
from airflow.operators.data_quality_operator import (
    BaseDataQualityOperator, DataQualityThresholdCheckOperator, DataQualityThresholdSQLCheckOperator,
)


@patch.object(PostgresHook, "get_records")
@patch.object(BaseHook, 'get_connection')
class TestBaseDataQualityOperator(unittest.TestCase):

    def test_get_sql_value_single_result(self, connection_mock, records_mock):
        task = BaseDataQualityOperator(
            task_id="one_result_task",
            conn_id='test_id',
            sql='SELECT COUNT(1) FROM test;'
        )

        connection_mock.return_value = Connection(conn_id='test_id', conn_type='postgres')
        records_mock.return_value = [(10,)]

        result = task.get_sql_value(
            conn_id=task.conn_id,
            sql=task.sql
        )

        assert result == 10

    def test_get_sql_value_multiple_rows(self, connection_mock, records_mock):

        task = BaseDataQualityOperator(
            task_id="one_result_task",
            conn_id='test_id',
            sql='SELECT COUNT(1) FROM test;'
        )

        connection_mock.return_value = Connection(conn_id='test_id', conn_type='postgres')
        records_mock.return_value = [(10,), (10,)]

        with pytest.raises(ValueError) as exec_info:
            task.get_sql_value(
                conn_id=task.conn_id,
                sql=task.sql
            )

        self.assertEqual(str(exec_info.value), "Result from sql query contains more than 1 entry")

    def test_get_sql_value_multiple_values(self, connection_mock, records_mock):

        task = BaseDataQualityOperator(
            task_id="one_result_task",
            conn_id='test_id',
            sql='SELECT COUNT(1) FROM test;'
        )

        connection_mock.return_value = Connection(conn_id='test_id', conn_type='postgres')
        records_mock.return_value = [(10, 100)]

        with pytest.raises(ValueError) as exec_info:
            task.get_sql_value(
                conn_id=task.conn_id,
                sql=task.sql
            )

        self.assertEqual(str(exec_info.value), "Result from sql query does not contain exactly 1 column")

    def test_get_sql_value_no_result(self, connection_mock, records_mock):

        task = BaseDataQualityOperator(
            task_id="one_result_task",
            conn_id='test_id',
            sql='SELECT COUNT(1) FROM test;'
        )

        connection_mock.return_value = Connection(conn_id='test_id', conn_type='postgres')
        records_mock.return_value = []

        with pytest.raises(ValueError) as exec_info:
            task.get_sql_value(
                conn_id=task.conn_id,
                sql=task.sql
            )

        self.assertEqual(str(exec_info.value), "No result returned from sql query")


class TestDataQualityThresholdCheckOperator(unittest.TestCase):

    def test_inside_threshold(self):
        min_threshold, max_threshold = 10, 15
        sql = "SELECT MIN(value) FROM test;"

        task = DataQualityThresholdCheckOperator(
            task_id="test_inside_threshold_values",
            conn_id="postgres",
            sql=sql,
            min_threshold=min_threshold,
            max_threshold=max_threshold
        )

        task.get_sql_value = Mock(return_value=12.5)
        task_instance = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(context={
            "execution_date": datetime.now(),
            'ti': task_instance
        })

        self.assertEqual(len(result), 7)
        self.assertTrue(result["within_threshold"])

    @patch.object(TaskInstance, "xcom_push")
    def test_outside_threshold(self, task_mock):
        min_threshold, max_threshold = 10, 15
        sql = "SELECT MIN(value) FROM test;"

        task = DataQualityThresholdCheckOperator(
            task_id="test_inside_threshold_values",
            conn_id="postgres",
            sql=sql,
            min_threshold=min_threshold,
            max_threshold=max_threshold,
            dag=DAG('test_dag', start_date=datetime(2018, 1, 1))
        )

        task.get_sql_value = Mock(return_value=50)
        task_instance = TaskInstance(task=task, execution_date=datetime.now())

        with pytest.raises(AirflowException) as exec_info:
            task.execute(context={
                "execution_date": datetime.now(),
                'ti': task_instance
            })

        self.assertTrue(task_mock.called)
        self.assertTrue("Result: 50 is not within thresholds 10 and 15" in str(exec_info.value))


class TestDataQualityThresholdSQLCheckOperator(unittest.TestCase):

    def test_inside_threshold(self):
        min_threshold_sql, max_threshold_sql = "SELECT 10", "SELECT 50"
        sql = "SELECT 40"

        task = DataQualityThresholdSQLCheckOperator(
            task_id="test_inside_threshold_values",
            conn_id="postgres",
            threshold_conn_id='postgres',
            sql=sql,
            min_threshold_sql=min_threshold_sql,
            max_threshold_sql=max_threshold_sql
        )

        task.get_sql_value = Mock(side_effect=lambda _, sql: int(sql.split()[1]))
        task_instance = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(context={
            "execution_date": datetime.now(),
            'ti': task_instance
        })

        self.assertEqual(len(result), 7)
        self.assertTrue(result["within_threshold"])

    @patch.object(TaskInstance, "xcom_push")
    def test_outside_threshold(self, task_mock):
        min_threshold_sql, max_threshold_sql = "SELECT 10", "SELECT 50"
        sql = "SELECT 100"

        task = DataQualityThresholdSQLCheckOperator(
            task_id="test_inside_threshold_values",
            conn_id="postgres",
            threshold_conn_id='postgres',
            sql=sql,
            min_threshold_sql=min_threshold_sql,
            max_threshold_sql=max_threshold_sql
        )

        task.get_sql_value = Mock(side_effect=lambda _, sql: int(sql.split()[1]))
        task_instance = TaskInstance(task=task, execution_date=datetime.now())

        with pytest.raises(AirflowException) as exec_info:
            task.execute(context={
                "execution_date": datetime.now(),
                'ti': task_instance
            })
        self.assertTrue(task_mock.called)
        self.assertTrue("Result: 100 is not within thresholds 10 and 50" in str(exec_info.value))


if __name__ == '__main__':
    unittest.main()
