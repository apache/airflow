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
import time

from mock import patch

import pyhive.presto

from airflow.hooks.presto_hook import PrestoHook


class TestPrestoHook(unittest.TestCase):

    def setUp(self):
        super(TestPrestoHook, self).setUp()

        self.cur = mock.MagicMock()
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestPrestoHook(PrestoHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = UnitTestPrestoHook()

    @patch('airflow.hooks.dbapi_hook.DbApiHook.insert_rows')
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",),
                ("world",)]
        target_fields = None
        self.db_hook.insert_rows(table, rows, target_fields)
        mock_insert_rows.assert_called_once_with(table, rows, None, 0)

    @patch("time.sleep")
    @patch("pyhive.presto.Cursor", autospec=True)
    def test_run_does_not_sleep_by_default(self, mock_cursor, mock_sleep):
        hook = PrestoHook()
        hook.run(sql="")
        mock_sleep.assert_not_called()

    @patch("time.sleep")
    @patch("pyhive.presto.Cursor", autospec=True)
    def test_run_optionally_sleeps_while_statement_executes(self, mock_cursor, mock_sleep):
        POLL_INTERVAL = 0.01
        ERROR_MSG = "would have blocked"
        mock_sleep.side_effect = RuntimeError(ERROR_MSG)
        hook = PrestoHook()

        with self.assertRaises(RuntimeError, msg=ERROR_MSG):
            hook.run(sql="", poll_interval=POLL_INTERVAL)
            mock_sleep.assert_called_once_with(POLL_INTERVAL)

