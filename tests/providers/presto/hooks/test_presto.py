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
from unittest.mock import patch

from prestodb.transaction import IsolationLevel

from airflow.models import Connection
from airflow.providers.presto.hooks.presto import PrestoHook


class TestPrestoHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(login='login', password='password', host='host', schema='hive',)

        class UnitTestPrestoHook(PrestoHook):
            conn_name_attr = 'presto_conn_id'

        self.db_hook = UnitTestPrestoHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch('airflow.providers.presto.hooks.presto.prestodb.auth.BasicAuthentication')
    @patch('airflow.providers.presto.hooks.presto.prestodb.dbapi.connect')
    def test_get_conn(self, mock_connect, mock_basic_auth):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            catalog='hive',
            host='host',
            port=None,
            http_scheme='http',
            schema='hive',
            source='airflow',
            user='login',
            isolation_level=0,
            auth=mock_basic_auth('login', 'password'),
        )


class TestPrestoHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestPrestoHook(PrestoHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

            def get_isolation_level(self):
                return IsolationLevel.READ_COMMITTED

        self.db_hook = UnitTestPrestoHook()

    @patch('airflow.hooks.dbapi_hook.DbApiHook.insert_rows')
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",), ("world",)]
        target_fields = None
        commit_every = 10
        self.db_hook.insert_rows(table, rows, target_fields, commit_every)
        mock_insert_rows.assert_called_once_with(table, rows, None, 10)

    def test_get_first_record(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchone.return_value = result_sets[0]

        self.assertEqual(result_sets[0], self.db_hook.get_first(statement))
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchall.return_value = result_sets

        self.assertEqual(result_sets, self.db_hook.get_records(statement))
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_pandas_df(self):
        statement = 'SQL'
        column = 'col'
        result_sets = [('row1',), ('row2',)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_pandas_df(statement)

        self.assertEqual(column, df.columns[0])

        self.assertEqual(result_sets[0][0], df.values.tolist()[0][0])
        self.assertEqual(result_sets[1][0], df.values.tolist()[1][0])

        self.cur.execute.assert_called_once_with(statement, None)
