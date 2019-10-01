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
import unittest
from unittest import mock

from airflow.hooks.drill_hook import DrillHook
from airflow.models import Connection


class TestDrillHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.connection = Connection(
            login='login',
            password='password',
            host='host',
            port=8047
        )

        self.db_hook = DrillHook()
        self.db_hook_get_connection = mock.Mock()
        self.db_hook_get_connection.return_value = self.connection

    @mock.patch('airflow.hooks.drill_hook.PyDrill')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            host='localhost',
            port=8047,
            auth=None,
            user=None,
            password=None
        )

    def test_get_uri(self):
        """
        Test on getting a drill connection uri
        """
        db_hook = self.db_hook
        self.assertEqual(db_hook.get_uri(), 'drill+sadrill://localhost:8047/None')

    @mock.patch('airflow.hooks.drill_hook.PyDrill.query')
    def test_get_records(self, mock_connect):
        sql = 'SQL'
        self.db_hook.get_records(sql=sql)

        mock_connect.assert_called_once_with(sql=sql)

    @mock.patch('airflow.hooks.drill_hook.PyDrill.query')
    def test_get_pandas_df(self, mock_connect):
        sql = 'SQL'
        self.db_hook.get_pandas_df(sql=sql)
        mock_connect.assert_called_once_with(sql=sql)
