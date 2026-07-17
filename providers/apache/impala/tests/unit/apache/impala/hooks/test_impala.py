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

from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.apache.impala.hooks.impala import ImpalaHook


@pytest.fixture
def impala_hook_fixture() -> ImpalaHook:
    hook = ImpalaHook()
    mock_get_conn = MagicMock()
    mock_get_conn.return_value.cursor = MagicMock()
    mock_get_conn.return_value.cursor.return_value.rowcount = 2
    hook.get_conn = mock_get_conn  # type:ignore[method-assign]
    hook.get_connection = MagicMock(return_value=Connection(conn_type="impala"))  # type:ignore[method-assign]

    return hook


@patch("airflow.providers.apache.impala.hooks.impala.connect", autospec=True)
def test_get_conn(mock_connect):
    hook = ImpalaHook()
    hook.get_connection = MagicMock(
        return_value=Connection(
            login="login",
            password="password",
            host="host",
            port=21050,
            schema="test",
            extra={"use_ssl": True},
        )
    )
    hook.get_conn()
    mock_connect.assert_called_once_with(
        host="host", port=21050, user="login", password="password", database="test", use_ssl=True
    )


@patch("airflow.providers.apache.impala.hooks.impala.connect", autospec=True)
def test_get_conn_kerberos(mock_connect):
    hook = ImpalaHook()
    hook.get_connection = MagicMock(
        return_value=Connection(
            login="login",
            password="password",
            host="host",
            port=21050,
            schema="test",
            extra={"auth_mechanism": "GSSAPI", "use_ssl": True},
        )
    )
    hook.get_conn()
    mock_connect.assert_called_once_with(
        host="host",
        port=21050,
        user="login",
        password="password",
        database="test",
        use_ssl=True,
        auth_mechanism="GSSAPI",
    )


@patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
def test_insert_rows(mock_insert_rows, impala_hook_fixture):
    table = "table"
    rows = [("hello",), ("world",)]
    target_fields = None
    commit_every = 10
    impala_hook_fixture.insert_rows(table, rows, target_fields, commit_every)
    mock_insert_rows.assert_called_once_with(table, rows, None, 10)


def test_get_first_record(impala_hook_fixture):
    statement = "SQL"
    result_sets = [("row1",), ("row2",)]
    impala_hook_fixture.get_conn.return_value.cursor.return_value.fetchone.return_value = result_sets[0]

    assert result_sets[0] == impala_hook_fixture.get_first(statement)
    impala_hook_fixture.get_conn.return_value.cursor.return_value.execute.assert_called_once_with(statement)


def test_get_records(impala_hook_fixture):
    statement = "SQL"
    result_sets = [("row1",), ("row2",)]
    impala_hook_fixture.get_conn.return_value.cursor.return_value.fetchall.return_value = result_sets

    assert result_sets == impala_hook_fixture.get_records(statement)
    impala_hook_fixture.get_conn.return_value.cursor.return_value.execute.assert_called_once_with(statement)


def test_get_df(impala_hook_fixture):
    statement = "SQL"
    column = "col"
    result_sets = [("row1",), ("row2",)]
    impala_hook_fixture.get_conn.return_value.cursor.return_value.description = [(column,)]
    impala_hook_fixture.get_conn.return_value.cursor.return_value.fetchall.return_value = result_sets
    df = impala_hook_fixture.get_df(statement, df_type="pandas")

    assert column == df.columns[0]

    assert result_sets[0][0] == df.values.tolist()[0][0]
    assert result_sets[1][0] == df.values.tolist()[1][0]

    impala_hook_fixture.get_conn.return_value.cursor.return_value.execute.assert_called_once_with(statement)


def test_get_df_polars(impala_hook_fixture):
    statement = "SQL"
    column = "col"
    result_sets = [("row1",), ("row2",)]
    mock_execute = MagicMock()
    mock_execute.description = [(column, None, None, None, None, None, None)]
    mock_execute.fetchall.return_value = result_sets
    impala_hook_fixture.get_conn.return_value.cursor.return_value.execute.return_value = mock_execute

    df = impala_hook_fixture.get_df(statement, df_type="polars")
    assert column == df.columns[0]
    assert result_sets[0][0] == df.row(0)[0]
    assert result_sets[1][0] == df.row(1)[0]


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
def test_run_hook_lineage(mock_send_lineage, impala_hook_fixture):
    sql = "SELECT 1"
    impala_hook_fixture.run(sql)

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is impala_hook_fixture
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] is None
    assert call_kw["cur"] is impala_hook_fixture.get_conn.return_value.cursor.return_value


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
def test_insert_rows_hook_lineage(mock_send_lineage, impala_hook_fixture):
    table = "table"
    rows = [("hello",), ("world",)]
    impala_hook_fixture.insert_rows(table, rows)

    mock_send_lineage.assert_called()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is impala_hook_fixture
    assert call_kw["sql"] == "INSERT INTO table  VALUES (%s)"
    assert call_kw["row_count"] == 2


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
@patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df")
def test_get_df_hook_lineage(mock_get_pandas_df, mock_send_lineage, impala_hook_fixture):
    sql = "SELECT 1"
    impala_hook_fixture.get_df(sql, df_type="pandas")

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is impala_hook_fixture
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] is None


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
@patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df_by_chunks")
def test_get_df_by_chunks_hook_lineage(mock_get_pandas_df_by_chunks, mock_send_lineage, impala_hook_fixture):
    sql = "SELECT 1"
    parameters = ("x",)
    impala_hook_fixture.get_df_by_chunks(sql, parameters=parameters, chunksize=1)

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is impala_hook_fixture
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] == parameters
