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

from airflow.hooks.dbapi_hook import DbApiHook


class TestDbApiHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestDbApiHook(DbApiHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = UnitTestDbApiHook()

    def test_get_records(self):
        statement = "SQL"
        rows = [("hello",),
                ("world",)]

        self.cur.fetchall.return_value = rows

        self.assertEqual(rows, self.db_hook.get_records(statement))

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records_parameters(self):
        statement = "SQL"
        parameters = ["X", "Y", "Z"]
        rows = [("hello",),
                ("world",)]

        self.cur.fetchall.return_value = rows

        self.assertEqual(rows, self.db_hook.get_records(statement, parameters))

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement, parameters)

    def test_get_records_exception(self):
        statement = "SQL"
        self.cur.fetchall.side_effect = RuntimeError('Great Problems')

        with self.assertRaises(RuntimeError):
            self.db_hook.get_records(statement)

        self.conn.close.call_count == 1
        self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_insert_rows(self):
        table = "table"
        rows = [("hello",),
                ("world",)]

        self.db_hook.insert_rows(table, rows)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "INSERT INTO {}  VALUES (%s)".format(table)
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_replace(self):
        table = "table"
        rows = [("hello",),
                ("world",)]

        self.db_hook.insert_rows(table, rows, replace=True)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "REPLACE INTO {}  VALUES (%s)".format(table)
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_target_fields(self):
        table = "table"
        rows = [("hello",),
                ("world",)]
        target_fields = ["field"]

        self.db_hook.insert_rows(table, rows, target_fields)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "INSERT INTO {} ({}) VALUES (%s)".format(table, target_fields[0])
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_commit_every(self):
        table = "table"
        rows = [("hello",),
                ("world",)]
        commit_every = 1

        self.db_hook.insert_rows(table, rows, commit_every=commit_every)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2 + divmod(len(rows), commit_every)[0]
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "INSERT INTO {}  VALUES (%s)".format(table)
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)
