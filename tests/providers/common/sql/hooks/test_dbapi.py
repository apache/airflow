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
from __future__ import annotations

import json
import unittest
from unittest import mock

import pytest

from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook, fetch_all_handler


class DbApiHookInProvider(DbApiHook):
    conn_name_attr = 'test_conn_id'


class NonDbApiHook(BaseHook):
    pass


class TestDbApiHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock(
            rowcount=0, spec=["description", "rowcount", "execute", "fetchall", "close"]
        )
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestDbApiHook(DbApiHook):
            conn_name_attr = 'test_conn_id'
            log = mock.MagicMock()

            def get_conn(self):
                return conn

        self.db_hook = UnitTestDbApiHook()
        self.db_hook_no_log_sql = UnitTestDbApiHook(log_sql=False)
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
                conn_type="conn-type",
                host="host",
                login="login",
                password="password",
                schema="schema",
                port=1,
            )
        )
        assert "conn-type://login:password@host:1/schema" == self.db_hook.get_uri()

    def test_get_uri_schema_override(self):
        self.db_hook_schema_override.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login="login",
                password="password",
                schema="schema",
                port=1,
            )
        )
        assert "conn-type://login:password@host:1/schema-override" == self.db_hook_schema_override.get_uri()

    def test_get_uri_schema_none(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type", host="host", login="login", password="password", schema=None, port=1
            )
        )
        assert "conn-type://login:password@host:1" == self.db_hook.get_uri()

    def test_get_uri_special_characters(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host/",
                login="lo/gi#! n",
                password="pass*! word/",
                schema="schema/",
                port=1,
            )
        )
        assert (
            "conn-type://lo%2Fgi%23%21%20n:pass%2A%21%20word%2F@host%2F:1/schema%2F" == self.db_hook.get_uri()
        )

    def test_get_uri_login_none(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login=None,
                password="password",
                schema="schema",
                port=1,
            )
        )
        assert "conn-type://:password@host:1/schema" == self.db_hook.get_uri()

    def test_get_uri_password_none(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login="login",
                password=None,
                schema="schema",
                port=1,
            )
        )
        assert "conn-type://login@host:1/schema" == self.db_hook.get_uri()

    def test_get_uri_authority_none(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login=None,
                password=None,
                schema="schema",
                port=1,
            )
        )
        assert "conn-type://host:1/schema" == self.db_hook.get_uri()

    def test_get_uri_extra(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login='login',
                password='password',
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://login:password@host/?charset=utf-8"

    def test_get_uri_extra_with_schema(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login='login',
                password='password',
                schema="schema",
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://login:password@host/schema?charset=utf-8"

    def test_get_uri_extra_with_port(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login='login',
                password='password',
                port=3306,
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://login:password@host:3306/?charset=utf-8"

    def test_get_uri_extra_with_port_and_empty_host(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                login='login',
                password='password',
                port=3306,
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://login:password@:3306/?charset=utf-8"

    def test_get_uri_extra_with_port_and_schema(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login='login',
                password='password',
                schema="schema",
                port=3306,
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://login:password@host:3306/schema?charset=utf-8"

    def test_get_uri_without_password(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login='login',
                password=None,
                schema="schema",
                port=3306,
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://login@host:3306/schema?charset=utf-8"

    def test_get_uri_without_auth(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                host="host",
                login=None,
                password=None,
                schema="schema",
                port=3306,
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://host:3306/schema?charset=utf-8"

    def test_get_uri_without_auth_and_empty_host(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="conn-type",
                login=None,
                password=None,
                schema="schema",
                port=3306,
                extra=json.dumps({'charset': 'utf-8'}),
            )
        )
        assert self.db_hook.get_uri() == "conn-type://@:3306/schema?charset=utf-8"

    def test_run_log(self):
        statement = 'SQL'
        self.db_hook.run(statement)
        assert self.db_hook.log.info.call_count == 2

    def test_run_no_log(self):
        statement = 'SQL'
        self.db_hook_no_log_sql.run(statement)
        assert self.db_hook_no_log_sql.log.info.call_count == 1

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

    def test_run_no_queries(self):
        with pytest.raises(ValueError) as err:
            self.db_hook.run(sql=[])
        assert err.value.args[0] == "List of SQL statements is empty"

    def test_instance_check_works_for_provider_derived_hook(self):
        assert isinstance(DbApiHookInProvider(), DbApiHook)

    def test_instance_check_works_for_non_db_api_hook(self):
        assert not isinstance(NonDbApiHook(), DbApiHook)

    def test_instance_check_works_for_legacy_db_api_hook(self):
        from airflow.hooks.dbapi import DbApiHook as LegacyDbApiHook

        assert isinstance(DbApiHookInProvider(), LegacyDbApiHook)

    def test_run_fetch_all_handler_select_1(self):
        self.cur.rowcount = -1  # can be -1 according to pep249
        self.cur.description = (tuple([None] * 7),)
        query = "SELECT 1"
        rows = [[1]]

        self.cur.fetchall.return_value = rows
        assert rows == self.db_hook.run(sql=query, handler=fetch_all_handler)

    def test_run_fetch_all_handler_print(self):
        self.cur.rowcount = -1
        self.cur.description = None
        query = "PRINT('Hello World !')"
        rows = None

        self.cur.fetchall.side_effect = Exception("Should not get called !")
        assert rows == self.db_hook.run(sql=query, handler=fetch_all_handler)
