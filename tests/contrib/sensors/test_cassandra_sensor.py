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

from mock import patch

from airflow.contrib.sensors.cassandra_record_sensor import CassandraRecordSensor
from airflow.contrib.sensors.cassandra_table_sensor import CassandraTableSensor


class TestCassandraRecordSensor(unittest.TestCase):
    @patch("airflow.contrib.sensors.cassandra_record_sensor.CassandraHook")
    def test_poke(self, mock_hook):
        sensor = CassandraRecordSensor(
            task_id='test_task',
            cassandra_conn_id='cassandra_default',
            table='t',
            keys={'foo': 'bar'}
        )
        sensor.poke(None)
        mock_hook.return_value.record_exists.assert_called_once_with('t', {'foo': 'bar'})


class TestCassandraTableSensor(unittest.TestCase):
    @patch("airflow.contrib.sensors.cassandra_table_sensor.CassandraHook")
    def test_poke(self, mock_hook):
        sensor = CassandraTableSensor(
            task_id='test_task',
            cassandra_conn_id='cassandra_default',
            table='t',
        )
        sensor.poke(None)
        mock_hook.return_value.table_exists.assert_called_once_with('t')


if __name__ == '__main__':
    unittest.main()
