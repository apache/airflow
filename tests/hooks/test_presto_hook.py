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
#

from unittest import mock
import unittest

from airflow.hooks.presto_hook import PrestoHook
from airflow.models import Connection


class TestPrestoHookConn(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.connection = Connection(
            host='host',
            login='airflow',
            password=None,
            schema='schema'
        )
        self.db_hook = PrestoHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('airflow.hooks.presto_hook.presto.connect')
    def test_get_conn_default(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(catalog='hive', host='host',
                                             poll_interval=1, port=None,
                                             protocol='http', schema='default',
                                             requests_kwargs=None, source='airflow',
                                             username='airflow')

    @mock.patch('airflow.hooks.presto_hook.presto.connect')
    def test_get_conn_port(self, mock_connect):
        self.connection.port = 1520
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(catalog='hive', host='host',
                                             poll_interval=1, port=1520,
                                             protocol='http', schema='default',
                                             requests_kwargs=None, source='airflow',
                                             username='airflow')

    @mock.patch('airflow.hooks.presto_hook.presto.connect')
    def test_get_conn_catalog(self, mock_connect):
        self.connection.catalog = 'db_catalog'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(catalog='db_catalog', host='host',
                                             poll_interval=1, port=None,
                                             protocol='http', schema='default',
                                             requests_kwargs=None, source='airflow',
                                             username='airflow')

    @mock.patch('airflow.hooks.presto_hook.presto.connect')
    def test_get_conn_poll_interval(self, mock_connect):
        self.connection.poll_interval = 5
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(catalog='hive', host='host',
                                             poll_interval=5, port=None,
                                             protocol='http', schema='default',
                                             requests_kwargs=None, source='airflow',
                                             username='airflow')

    @mock.patch('airflow.hooks.presto_hook.presto.connect')
    def test_get_conn_schema(self, mock_connect):
        self.connection.schema = 'myschema'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(catalog='hive', host='host',
                                             poll_interval=1, port=None,
                                             protocol='http', schema='myschema',
                                             requests_kwargs=None, source='airflow',
                                             username='airflow')


class TestPrestoHook(unittest.TestCase):

    def setUp(self):
        cursor_patch = mock.patch("pyhive.presto.Cursor", autospec=True)

        self.db_hook = PrestoHook()
        self.execute_args = ["SELECT * FROM users", None]
        self.mock_cursor = cursor_patch.start().return_value

        self.addCleanup(cursor_patch.stop)

    @mock.patch("airflow.hooks.dbapi_hook.DbApiHook.insert_rows")
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",),
                ("world",)]
        target_fields = None
        self.db_hook.insert_rows(table, rows, target_fields)
        mock_insert_rows.assert_called_once_with(table, rows, None, 0)
