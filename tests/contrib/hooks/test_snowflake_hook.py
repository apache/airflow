
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

import mock
import unittest

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook


class TestSnowflakeHook(unittest.TestCase):

    def setUp(self):
        super(TestSnowflakeHook, self).setUp()

        self.cur = mock.MagicMock()
        self.conn = conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur

        self.conn.login = 'user'
        self.conn.password = 'pw'
        self.conn.schema = 'public'
        self.conn.extra_dejson = {'database': 'db',
                                  'account': 'airflow',
                                  'warehouse': 'af_wh'}

        class UnitTestSnowflakeHook(SnowflakeHook):
            conn_name_attr = 'snowflake_conn_id'

            def get_conn(self):
                return conn

            def get_connection(self, connection_id):
                return conn

        self.db_hook = UnitTestSnowflakeHook()

    def test_get_uri(self):
        uri_shouldbe = 'snowflake://user:pw@airflow/db/public?warehouse=af_wh'
        self.assertEqual(uri_shouldbe, self.db_hook.get_uri())

    def test_get_conn_params(self):
        conn_params_shouldbe = {'user': 'user',
                                'password': 'pw',
                                'schema': 'public',
                                'database': 'db',
                                'account': 'airflow',
                                'warehouse': 'af_wh'}
        self.assertEqual(conn_params_shouldbe, self.db_hook._get_conn_params())

    def test_get_conn(self):
        self.assertEqual(self.db_hook.get_conn(), self.conn)
