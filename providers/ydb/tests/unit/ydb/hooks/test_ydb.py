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

import pytest

ydb = pytest.importorskip("ydb")

from unittest.mock import PropertyMock, patch

from airflow.models import Connection
from airflow.providers.ydb.hooks.ydb import YDBHook

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"


class FakeDriver:
    def wait(*args, **kwargs):
        pass


class FakeSessionPool:
    def __init__(self, driver):
        self._driver = driver


class FakeYDBCursor:
    def __init__(self, *args, **kwargs):
        self.description = True

    def execute(self, operation, parameters=None):
        return True

    def fetchone(self):
        return 1, 2

    def fetchmany(self, size=None):
        return [(1, 2), (2, 3), (3, 4)][0:size]

    def fetchall(self):
        return [(1, 2), (2, 3), (3, 4)]

    def close(self):
        pass

    @property
    def rowcount(self):
        return 1


@patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
@patch("ydb.Driver")
@patch("ydb.QuerySessionPool")
@patch("ydb_dbapi.Connection._cursor_cls", new_callable=PropertyMock)
def test_execute(cursor_class, mock_session_pool, mock_driver, mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_type="ydb",
        host="grpc://localhost",
        port=2135,
        login="my_user",
        password="my_pwd",
        extra={"database": "/my_db1"},
    )
    driver_instance = FakeDriver()

    cursor_class.return_value = FakeYDBCursor
    mock_driver.return_value = driver_instance
    mock_session_pool.return_value = FakeSessionPool(driver_instance)

    hook = YDBHook()
    assert hook.get_uri() == "ydb://grpc://my_user:my_pwd@localhost:2135/?database=%2Fmy_db1"
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            assert cur.execute("INSERT INTO table VALUES ('aaa'), ('bbbb')")
            conn.commit()
            assert cur.execute("SELECT * FROM table")
            assert cur.fetchone() == (1, 2)
            assert cur.fetchmany(2) == [(1, 2), (2, 3)]
            assert cur.fetchall() == [(1, 2), (2, 3), (3, 4)]


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
@patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
@patch("ydb.Driver")
@patch("ydb.QuerySessionPool")
@patch("ydb_dbapi.Connection._cursor_cls", new_callable=PropertyMock)
def test_run_hook_lineage(
    cursor_class, mock_session_pool, mock_driver, mock_get_connection, mock_send_lineage
):
    mock_get_connection.return_value = Connection(
        conn_type="ydb",
        host="grpc://localhost",
        port=2135,
        login="my_user",
        password="my_pwd",
        extra={"database": "/my_db1"},
    )
    driver_instance = FakeDriver()

    cursor_class.return_value = FakeYDBCursor
    mock_driver.return_value = driver_instance
    mock_session_pool.return_value = FakeSessionPool(driver_instance)

    hook = YDBHook()
    sql = "SELECT 1"
    hook.run(sql)

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is hook
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] is None


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
@patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df")
@patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
@patch("ydb.Driver")
@patch("ydb.QuerySessionPool")
@patch("ydb_dbapi.Connection._cursor_cls", new_callable=PropertyMock)
def test_get_df_hook_lineage(
    cursor_class, mock_session_pool, mock_driver, mock_get_connection, mock_get_pandas_df, mock_send_lineage
):
    mock_get_connection.return_value = Connection(
        conn_type="ydb",
        host="grpc://localhost",
        port=2135,
        login="my_user",
        password="my_pwd",
        extra={"database": "/my_db1"},
    )
    driver_instance = FakeDriver()

    cursor_class.return_value = FakeYDBCursor
    mock_driver.return_value = driver_instance
    mock_session_pool.return_value = FakeSessionPool(driver_instance)

    hook = YDBHook()
    sql = "SELECT 1"
    hook.get_df(sql, df_type="pandas")

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is hook
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] is None


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
@patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df_by_chunks")
@patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
@patch("ydb.Driver")
@patch("ydb.QuerySessionPool")
@patch("ydb_dbapi.Connection._cursor_cls", new_callable=PropertyMock)
def test_get_df_by_chunks_hook_lineage(
    cursor_class,
    mock_session_pool,
    mock_driver,
    mock_get_connection,
    mock_get_pandas_df_by_chunks,
    mock_send_lineage,
):
    mock_get_connection.return_value = Connection(
        conn_type="ydb",
        host="grpc://localhost",
        port=2135,
        login="my_user",
        password="my_pwd",
        extra={"database": "/my_db1"},
    )
    driver_instance = FakeDriver()

    cursor_class.return_value = FakeYDBCursor
    mock_driver.return_value = driver_instance
    mock_session_pool.return_value = FakeSessionPool(driver_instance)

    hook = YDBHook()
    sql = "SELECT 1"
    parameters = ("x",)
    hook.get_df_by_chunks(sql, parameters=parameters, chunksize=1)

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is hook
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] == parameters
