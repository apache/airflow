# -*- coding: utf-8 -*-
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

from airflow import DAG
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_sql_dag'


class TestSqlSensor(unittest.TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(TEST_DAG_ID, default_args=args)

    def test_unsupported_conn_type(self):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='redis_default',
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag
        )

        with self.assertRaises(AirflowException):
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @unittest.skipUnless(
        'mysql' in conf.get('core', 'sql_alchemy_conn'), "this is a mysql test")
    def test_sql_sensor_mysql(self):
        t1 = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='mysql_default',
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag
        )
        t1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        t2 = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='mysql_default',
            sql="SELECT count(%s) FROM INFORMATION_SCHEMA.TABLES",
            parameters=["table_name"],
            dag=self.dag
        )
        t2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @unittest.skipUnless(
        'postgresql' in conf.get('core', 'sql_alchemy_conn'), "this is a postgres test")
    def test_sql_sensor_postgres(self):
        t1 = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag
        )
        t1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        t2 = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT count(%s) FROM INFORMATION_SCHEMA.TABLES",
            parameters=["table_name"],
            dag=self.dag
        )
        t2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        self.assertFalse(t.poke(None))

        mock_get_records.return_value = [['1']]
        self.assertTrue(t.poke(None))

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke_fail_on_empty(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
            fail_on_empty=True
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        self.assertRaises(AirflowException, t.poke, None)

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke_success(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
            success=lambda x: x in [1]
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        self.assertFalse(t.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertTrue(t.poke(None))

        mock_get_records.return_value = [['1']]
        self.assertFalse(t.poke(None))

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke_failure(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
            failure=lambda x: x in [1]
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        self.assertFalse(t.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, t.poke, None)

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke_failure_success(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
            failure=lambda x: x in [1],
            success=lambda x: x in [2]
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        self.assertFalse(t.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, t.poke, None)

        mock_get_records.return_value = [[2]]
        self.assertTrue(t.poke(None))

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke_failure_success_same(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
            failure=lambda x: x in [1],
            success=lambda x: x in [1]
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        self.assertFalse(t.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, t.poke, None)

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke_invalid_failure(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
            failure=[1],
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, t.poke, None)

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_poke_invalid_success(self, mock_hook):
        t = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT 1",
            success=[1],
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, t.poke, None)

    @mock.patch('airflow.sensors.sql_sensor.BaseHook')
    def test_sql_sensor_postgres_check_allow_null(self, mock_hook):
        t1 = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT NULL",
            allow_null=True
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[None]]
        self.assertTrue(t1.poke(None))

        t2 = SqlSensor(
            task_id='sql_sensor_check',
            conn_id='postgres_default',
            sql="SELECT NULL",
            allow_null=False
        )

        mock_hook.get_connection('postgres_default').conn_type = "postgres"
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[None]]
        self.assertFalse(t2.poke(None))
