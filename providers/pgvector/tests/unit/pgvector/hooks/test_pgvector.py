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

from unittest.mock import MagicMock, Mock, patch

import pytest

from airflow.models import Connection
from airflow.providers.pgvector.hooks.pgvector import PgVectorHook


@pytest.fixture
def pg_vector_hook():
    return PgVectorHook(postgres_conn_id="your_postgres_conn_id")


@pytest.fixture
def pgvector_hook_setup():
    """Set up mock PgVectorHook for testing (follows the postgres test pattern)."""
    cur = MagicMock(rowcount=0)
    conn = MagicMock()
    conn.cursor.return_value = cur

    class UnitTestPgVectorHook(PgVectorHook):
        conn_name_attr = "test_conn_id"

        def get_conn(self):
            return conn

    db_hook = UnitTestPgVectorHook()
    db_hook.get_connection = MagicMock(return_value=Connection(conn_type="postgres"))
    return MagicMock(cur=cur, conn=conn, db_hook=db_hook)


def test_create_table(pg_vector_hook):
    pg_vector_hook.run = Mock()
    table_name = "my_table"
    columns = ["id SERIAL PRIMARY KEY", "name VARCHAR(255)", "value INTEGER"]
    pg_vector_hook.create_table(table_name, columns, if_not_exists=True)
    pg_vector_hook.run.assert_called_with(
        "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY, name VARCHAR(255), value INTEGER)"
    )


def test_create_extension(pg_vector_hook):
    pg_vector_hook.run = Mock()
    extension_name = "timescaledb"
    pg_vector_hook.create_extension(extension_name, if_not_exists=True)
    pg_vector_hook.run.assert_called_with("CREATE EXTENSION IF NOT EXISTS timescaledb")


def test_drop_table(pg_vector_hook):
    pg_vector_hook.run = Mock()
    table_name = "my_table"
    pg_vector_hook.drop_table(table_name, if_exists=True)
    pg_vector_hook.run.assert_called_with("DROP TABLE IF EXISTS my_table")


def test_truncate_table(pg_vector_hook):
    pg_vector_hook.run = Mock()
    table_name = "my_table"
    pg_vector_hook.truncate_table(table_name, restart_identity=True)
    pg_vector_hook.run.assert_called_with("TRUNCATE TABLE my_table RESTART IDENTITY")


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
def test_run_hook_lineage(mock_send_lineage, pgvector_hook_setup):
    setup = pgvector_hook_setup
    sql = "SELECT 1"
    setup.db_hook.run(sql)

    mock_send_lineage.assert_called()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is setup.db_hook
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] is None
    assert call_kw["cur"] is setup.cur


@patch("airflow.providers.postgres.hooks.postgres.send_sql_hook_lineage")
@patch("airflow.providers.postgres.hooks.postgres.PostgresHook._get_polars_df")
def test_get_df_hook_lineage(mock_get_polars_df, mock_send_lineage, pgvector_hook_setup):
    setup = pgvector_hook_setup
    sql = "SELECT 1"
    parameters = ("x",)
    setup.db_hook.get_df(sql, parameters=parameters, df_type="polars")

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is setup.db_hook
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] == parameters


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
@patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df_by_chunks")
def test_get_df_by_chunks_hook_lineage(mock_get_pandas_df_by_chunks, mock_send_lineage, pgvector_hook_setup):
    setup = pgvector_hook_setup
    sql = "SELECT 1"
    parameters = ("x",)
    setup.db_hook.get_df_by_chunks(sql, parameters=parameters, chunksize=1)

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is setup.db_hook
    assert call_kw["sql"] == sql
    assert call_kw["sql_parameters"] == parameters


@patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
def test_insert_rows_hook_lineage(mock_send_lineage, pgvector_hook_setup):
    setup = pgvector_hook_setup
    table = "table"
    rows = [("hello",), ("world",)]

    setup.db_hook.insert_rows(table, rows)

    mock_send_lineage.assert_called_once()
    call_kw = mock_send_lineage.call_args.kwargs
    assert call_kw["context"] is setup.db_hook
    assert call_kw["sql"] == f"INSERT INTO {table}  VALUES (%s)"
    assert call_kw["row_count"] == 2
