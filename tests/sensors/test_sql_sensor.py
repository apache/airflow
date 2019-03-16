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
import mock
import unittest

from airflow import DAG
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.timezone import datetime

configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_sql_dag'


class SqlSensorTests(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
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
        'mysql' in configuration.conf.get('core', 'sql_alchemy_conn'), "this is a mysql test")
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
        'postgresql' in configuration.conf.get('core', 'sql_alchemy_conn'), "this is a postgres test")
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
