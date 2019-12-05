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

from airflow.contrib.hooks.pinot_hook import PinotDbApiHook


class TestPinotDbApiHook(unittest.TestCase):

    def setUp(self):
        super(TestPinotDbApiHook, self).setUp()
        self.conn = conn = mock.MagicMock()
        self.conn.host = 'host'
        self.conn.port = '1000'
        self.conn.conn_type = 'http'
        self.conn.extra_dejson = {'endpoint': 'pql'}
        self.cur = mock.MagicMock()
        self.conn.__enter__.return_value = self.cur
        self.conn.__exit__.return_value = None

        class TestPinotDBApiHook(PinotDbApiHook):
            def get_conn(self):
                return conn

            def get_connection(self, conn_id):
                return conn

        self.db_hook = TestPinotDBApiHook

    def test_get_uri(self):
        """
        Test on getting a pinot connection uri
        """
        db_hook = self.db_hook()
        self.assertEqual(db_hook.get_uri(), 'http://host:1000/pql')

    def test_get_conn(self):
        """
        Test on getting a pinot connection
        """
        conn = self.db_hook().get_conn()
        self.assertEqual(conn.host, 'host')
        self.assertEqual(conn.port, '1000')
        self.assertEqual(conn.conn_type, 'http')
        self.assertEqual(conn.extra_dejson.get('endpoint'), 'pql')

    def test_get_records(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchall.return_value = result_sets
        self.assertEqual(result_sets, self.db_hook().get_records(statement))

    def test_get_first(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchone.return_value = result_sets[0]
        self.assertEqual(result_sets[0], self.db_hook().get_first(statement))
