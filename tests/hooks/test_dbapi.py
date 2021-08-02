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

import pytest
from parameterized import parameterized

from airflow.hooks.dbapi import DbApiHook
from airflow.models import Connection


class TestDbApiHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestDbApiHook(DbApiHook):
            conn_name_attr = 'test_conn_id'
            log = mock.MagicMock()

            def get_conn(self):
                return conn

        self.db_hook = UnitTestDbApiHook()
        self.db_hook_schema_override = UnitTestDbApiHook(schema='schema-override')

    def test_get_records(self):
        statement = "SQL"
        rows = [("hello",), ("world",)]

        self.cur.fetchall.return_value = rows

        assert rows == self.db_hook.get_records(statement)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records_parameters(self):
        statement = "SQL"
        parameters = ["X", "Y", "Z"]
        rows = [("hello",), ("world",)]

        self.cur.fetchall.return_value = rows

        assert rows == self.db_hook.get_records(statement, parameters)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement, parameters)

    def test_get_records_exception(self):
        statement = "SQL"
        self.cur.fetchall.side_effect = RuntimeError('Great Problems')

        with pytest.raises(RuntimeError):
            self.db_hook.get_records(statement)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_insert_rows(self):
        table = "table"
        rows = [("hello",), ("world",)]

        self.db_hook.insert_rows(table, rows)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == self.conn.commit.call_count

        sql = f"INSERT INTO {table}  VALUES (%s)"
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_replace(self):
        table = "table"
        rows = [("hello",), ("world",)]

        self.db_hook.insert_rows(table, rows, replace=True)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == self.conn.commit.call_count

        sql = f"REPLACE INTO {table}  VALUES (%s)"
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_target_fields(self):
        table = "table"
        rows = [("hello",), ("world",)]
        target_fields = ["field"]

        self.db_hook.insert_rows(table, rows, target_fields)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == self.conn.commit.call_count

        sql = f"INSERT INTO {table} ({target_fields[0]}) VALUES (%s)"
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_commit_every(self):
        table = "table"
        rows = [("hello",), ("world",)]
        commit_every = 1

        self.db_hook.insert_rows(table, rows, commit_every=commit_every)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2 + divmod(len(rows), commit_every)[0]
        assert commit_count == self.conn.commit.call_count

        sql = f"INSERT INTO {table}  VALUES (%s)"
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_get_uri_schema_not_none(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn_type",
                host="host",
                login="login",
                password="password",
                schema="schema",
                port=1,
            )
        )
        assert "conn_type://login:password@host:1/schema" == self.db_hook.get_uri()

    def test_get_uri_schema_override(self):
        self.db_hook_schema_override.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn_type",
                host="host",
                login="login",
                password="password",
                schema="schema",
                port=1,
            )
        )
        assert "conn_type://login:password@host:1/schema-override" == self.db_hook_schema_override.get_uri()

    def test_get_uri_schema_none(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn_type", host="host", login="login", password="password", schema=None, port=1
            )
        )
        assert "conn_type://login:password@host:1" == self.db_hook.get_uri()

    def test_get_uri_special_characters(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn_type",
                host="host",
                login="logi#! n",
                password="pass*! word",
                schema="schema",
                port=1,
            )
        )
        assert "conn_type://logi%23%21+n:pass%2A%21+word@host:1/schema" == self.db_hook.get_uri()

    def test_run_log(self):
        statement = 'SQL'
        self.db_hook.run(statement)
        assert self.db_hook.log.info.call_count == 2

    def test_run_with_handler(self):
        sql = 'SQL'
        param = ('p1', 'p2')
        called = 0
        obj = object()

        def handler(cur):
            cur.execute.assert_called_once_with(sql, param)
            nonlocal called
            called += 1
            return obj

        result = self.db_hook.run(sql, parameters=param, handler=handler)
        assert called == 1
        assert self.conn.commit.called
        assert result == obj

    def test_run_with_handler_multiple(self):
        sql = ['SQL', 'SQL']
        param = ('p1', 'p2')
        called = 0
        obj = object()

        def handler(cur):
            cur.execute.assert_called_with(sql[0], param)
            nonlocal called
            called += 1
            return obj

        result = self.db_hook.run(sql, parameters=param, handler=handler)
        assert called == 2
        assert self.conn.commit.called
        assert result == [obj, obj]

    @parameterized.expand(
        [
            (['SQL1; SQL2;', 'SQL3'], 3),
            (['SQL1; SQL2;', 'SQL3;'], 3),
            (['SQL1; SQL2; SQL3;'], 3),
            ('SQL1; SQL2; SQL3;', 3),
            (['--this is a comment', 'SQL1; SQL2;'], 2),
            (['SQL1;', 'SQL2'], 2),
            (['SQL1;', 'SQL2;'], 2),
            (['SQL1; SQL2;'], 2),
            ('SQL1; SQL2;', 2),
            (['SQL1'], 1),
            (['SQL1;'], 1),
            ('SQL1;', 1),
            ('SQL1', 1),
        ]
    )
    @patch('airflow.hooks.dbapi.DbApiHook._run_command')
    def test_run_with_multiple_statements_and_split(self, sql, run_count, _run_command):
        self.db_hook.run(sql, split_statements=True)
        assert _run_command.call_count == run_count

    @parameterized.expand([(['SQL1; SQL2;', 'SQL3'], ['SQL1;', 'SQL2;', 'SQL3'])])
    def test_split_statement_with_no_semicolon_strip(self, sql, expected_splitted_sql):
        assert self.db_hook._split_sql_statements(sql, strip_semicolon=False) == expected_splitted_sql

    @parameterized.expand(
        [(['--this is a comment', 'SQL2;', 'SQL3'], ['--this is a comment', 'SQL2', 'SQL3'])]
    )
    def test_split_statement_with_no_comment_strip(self, sql, expected_splitted_sql):
        assert self.db_hook._split_sql_statements(sql, strip_comments=False) == expected_splitted_sql

    @parameterized.expand(
        [
            (['SQL1; SQL2;', 'SQL3'], ['SQL1', 'SQL2', 'SQL3']),
            (['SELECT 1;SELECT 2;SELECT 3;--SELECT 4 commented'], ['SELECT 1', 'SELECT 2', 'SELECT 3']),
            (
                [
                    """
                SELECT 1;
                SELECT 2;
                SELECT COUNT(1) FROM tbl WHERE 1 = 1 AND tbl.country = "Chile";
                SELECT COUNT(1) FROM tbl WHERE 1 = 1 AND tbl.char = ";";
                SELECT A.*, B.id2 FROM tbl A LEFT JOIN tbl2 B ON A.id = B.id2 b WHERE 1
                """
                ],
                [
                    'SELECT 1',
                    'SELECT 2',
                    'SELECT COUNT(1) FROM tbl WHERE 1 = 1 AND tbl.country = "Chile"',
                    'SELECT COUNT(1) FROM tbl WHERE 1 = 1 AND tbl.char = ";"',
                    'SELECT A.*, B.id2 FROM tbl A LEFT JOIN tbl2 B ON A.id = B.id2 b WHERE 1',
                ],
            ),
        ]
    )
    def test_split_statement(self, sql, expected_splitted_sql):
        assert self.db_hook._split_sql_statements(sql) == expected_splitted_sql
