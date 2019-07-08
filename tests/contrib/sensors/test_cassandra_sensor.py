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

from unittest.mock import patch

from airflow import DAG
from airflow.contrib.sensors.cassandra_record_sensor import CassandraRecordSensor
from airflow.contrib.sensors.cassandra_table_sensor import CassandraTableSensor
from airflow.utils import timezone


DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestCassandraRecordSensor(unittest.TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG('test_dag_id', default_args=args)
        self.sensor = CassandraRecordSensor(
            task_id='test_task',
            cassandra_conn_id='cassandra_default',
            dag=self.dag,
            table='t',
            keys={'foo': 'bar'}
        )

    @patch("airflow.contrib.hooks.cassandra_hook.CassandraHook.record_exists")
    def test_poke(self, mock_record_exists):
        self.sensor.poke(None)
        mock_record_exists.assert_called_once_with('t', {'foo': 'bar'})


class TestCassandraTableSensor(unittest.TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG('test_dag_id', default_args=args)
        self.sensor = CassandraTableSensor(
            task_id='test_task',
            cassandra_conn_id='cassandra_default',
            dag=self.dag,
            table='t',
        )

    @patch("airflow.contrib.hooks.cassandra_hook.CassandraHook.table_exists")
    def test_poke(self, mock_table_exists):
        self.sensor.poke(None)
        mock_table_exists.assert_called_once_with('t')


if __name__ == '__main__':
    unittest.main()
