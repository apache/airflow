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

from requests.exceptions import RequestException

from airflow.hooks.presto_hook import PrestoHook


class TestPrestoHook(unittest.TestCase):

    def setUp(self):
        cursor_patch = mock.patch("pyhive.presto.Cursor", autospec=True)
        sleep_patch = mock.patch("time.sleep")

        self.hook = PrestoHook()
        self.execute_args = ["SELECT * FROM users", None]
        self.mock_cursor = cursor_patch.start().return_value
        self.mock_sleep = sleep_patch.start()

        self.addCleanup(cursor_patch.stop)
        self.addCleanup(sleep_patch.stop)

    @mock.patch("airflow.hooks.dbapi_hook.DbApiHook.insert_rows")
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",),
                ("world",)]
        target_fields = None
        self.hook.insert_rows(table, rows, target_fields)
        mock_insert_rows.assert_called_once_with(table, rows, None, 0)

    def test_run_does_not_block_by_default(self):
        self.hook.run(*self.execute_args)

        self.mock_cursor.execute.assert_called_once_with(*self.execute_args)
        self.mock_sleep.assert_not_called()

    def test_run_optionally_blocks_while_statement_executes(self):
        poll_interval = 1
        error_message = "would have slept"

        self.mock_cursor.poll.return_value = "execution unfinished"
        self.mock_sleep.side_effect = RuntimeError(error_message)

        with self.assertRaises(RuntimeError, msg=error_message):
            run_args = self.execute_args + [poll_interval]
            self.hook.run(*run_args)

        self.mock_cursor.execute.assert_called_once_with(*self.execute_args)
        self.mock_cursor.poll.assert_called_once()
        self.mock_sleep.assert_called_once_with(poll_interval)

    def test_run_continues_polling_if_presto_unreachable(self):
        poll_interval = 1
        error_message = "would have slept"

        self.mock_cursor.poll.side_effect = RequestException("network partition")
        self.mock_sleep.side_effect = RuntimeError(error_message)

        with self.assertRaises(RuntimeError, msg=error_message):
            run_args = self.execute_args + [poll_interval]
            self.hook.run(*run_args)

        self.mock_cursor.execute.assert_called_once_with(*self.execute_args)
        self.mock_cursor.poll.assert_called_once()
        self.mock_sleep.assert_called_once_with(poll_interval)
