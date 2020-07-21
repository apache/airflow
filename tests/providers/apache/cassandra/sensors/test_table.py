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

from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor

TEST_CASSANDRA_CONN_ID = 'cassandra_default'
TEST_CASSANDRA_TABLE = 't'
TEST_CASSANDRA_TABLE_WITH_KEYSPACE = 'keyspacename.tablename'


class TestCassandraTableSensor(unittest.TestCase):
    @patch("airflow.providers.apache.cassandra.sensors.table.CassandraHook")
    def test_poke(self, mock_hook):
        sensor = CassandraTableSensor(
            task_id='test_task',
            cassandra_conn_id=TEST_CASSANDRA_CONN_ID,
            table=TEST_CASSANDRA_TABLE,
        )
        exists = sensor.poke(dict())

        self.assertTrue(exists)

        mock_hook.return_value.table_exists.assert_called_once_with(TEST_CASSANDRA_TABLE)
        mock_hook.assert_called_once_with(TEST_CASSANDRA_CONN_ID)

    @patch("airflow.providers.apache.cassandra.sensors.table.CassandraHook")
    def test_poke_should_return_false_for_non_existing_table(self, mock_hook):
        mock_hook.return_value.table_exists.return_value = False

        sensor = CassandraTableSensor(
            task_id='test_task',
            cassandra_conn_id=TEST_CASSANDRA_CONN_ID,
            table=TEST_CASSANDRA_TABLE,
        )
        exists = sensor.poke(dict())

        self.assertFalse(exists)

        mock_hook.return_value.table_exists.assert_called_once_with(TEST_CASSANDRA_TABLE)
        mock_hook.assert_called_once_with(TEST_CASSANDRA_CONN_ID)

    @patch("airflow.providers.apache.cassandra.sensors.table.CassandraHook")
    def test_poke_should_succeed_for_table_with_mentioned_keyspace(self, mock_hook):
        sensor = CassandraTableSensor(
            task_id='test_task',
            cassandra_conn_id=TEST_CASSANDRA_CONN_ID,
            table=TEST_CASSANDRA_TABLE_WITH_KEYSPACE,
        )
        exists = sensor.poke(dict())

        self.assertTrue(exists)

        mock_hook.return_value.table_exists.assert_called_once_with(TEST_CASSANDRA_TABLE_WITH_KEYSPACE)
        mock_hook.assert_called_once_with(TEST_CASSANDRA_CONN_ID)
