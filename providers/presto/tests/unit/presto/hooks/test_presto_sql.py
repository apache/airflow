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
from collections import namedtuple
from unittest import mock
from unittest.mock import patch, MagicMock, PropertyMock
from typing import Union
import pytest
from sqlalchemy.engine import URL

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.presto.hooks.presto import PrestoHook

SerializableRow = namedtuple("SerializableRow", ["id", "value"])

DEFAULT_CONN_ID = "presto_default"
DEFAULT_HOST = "test_host"
DEFAULT_PORT = 8080
DEFAULT_LOGIN = "test"
DEFAULT_EXTRA_JSON = None
DEFAULT_PASSWORD = "test_pass"


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    """Convert field names into cursor.description tuples."""
    return [(field,) for field in fields]


@pytest.fixture(autouse=True)
def mock_connection(create_connection_without_db):
    """Create a mocked Airflow connection for Presto."""
    conn = Connection(
        conn_id=DEFAULT_CONN_ID,
        conn_type="presto",
        host=DEFAULT_HOST,
        login=DEFAULT_LOGIN,
        password=DEFAULT_PASSWORD,
        port=DEFAULT_PORT,
        extra=DEFAULT_EXTRA_JSON,
        schema="presto_db",
    )
    create_connection_without_db(conn)
    return conn



@pytest.fixture
def presto_hook():
    """Fixture for PrestoHook with mocked connection."""
    return PrestoHook(presto_conn_id=DEFAULT_CONN_ID)


@pytest.fixture
def mock_get_conn():
    """Fixture to mock get_conn method of PrestoHook."""
    with patch.object(PrestoHook, "get_conn", autospec=True) as mock:
        yield mock


@pytest.fixture
def mock_cursor(mock_get_conn: Union[MagicMock, mock.AsyncMock]):
    """Fixture to mock cursor returned by get_conn."""
    cursor = MagicMock()
    type(cursor).rowcount = PropertyMock(return_value=1)
    cursor.fetchall.return_value = [("1", "row1")]
    cursor.description = get_cursor_descriptions(["id", "value"])
    cursor.nextset.side_effect = [False]
    mock_get_conn.return_value.cursor.return_value = cursor
    return cursor


@pytest.mark.parametrize(
    "custom_extra, expected_catalog, expected_protocol, expected_source, conn_schema_override","expected_query_schema",
    [
        pytest.param(
            {"catalog": "reporting_db", "protocol": "https"},
            "reporting_db",
            "https",
            "airflow",
            "data_schema",
            id="custom_catalog_and_protocol",
        ),
        pytest.param(
            {"source": "my_dag_run"},
            "hive",
            "http",
            "my_dag_run",
            "test_schema",
            "test_schema",
            id="custom_source_only",
        ),
        pytest.param(
            {"catalog": "logs"},
            "logs",
            "http",
            "airflow",
            None,
            None,
            id="empty_schema_in_connection",
        ),
        pytest.param(
            {},
            "hive",
            "http",
            "airflow",
            "default_schema",
            "default_schema",
            id="all_defaults",
        ),
    ],
)
def test_sqlalchemy_url_property(
    presto_hook,
    mock_connection,
    custom_extra,
    expected_catalog,
    expected_protocol,
    expected_source,
    conn_schema_override,
    expected_query_schema,
):
    """Tests various custom configuration passed via the 'extra' field to ensure the hook"""

    mock_connection.extra = json.dumps(custom_extra)
    mock_connection.schema = conn_schema_override

    expected_database = expected_catalog
    
    url_string = presto_hook.sqlalchemy_url.render_as_string(hide_password=False)
    expected_base_path = (
        f"presto://{DEFAULT_LOGIN}:{DEFAULT_PASSWORD}@{DEFAULT_HOST}:{DEFAULT_PORT}/{expected_database}"
    )

    assert url_string.startswith(expected_base_path), (
        f"Base URL prefix is incorrect. Expected: {expected_base_path}"
    )

    parsed_url = URL(url_string)

    assert parsed_url.host == DEFAULT_HOST
    assert parsed_url.database == expected_database
    assert parsed_url.query.get("protocol") == expected_protocol
    assert parsed_url.query.get("source") == expected_source
    assert parsed_url.query.get("schema") == expected_query_schema

@pytest.mark.parametrize(
    "return_last, split_statements, sql, expected_calls, cursor_results, expected_result",
    [
        pytest.param(
            True,
            False,
            "SELECT * FROM table_A",
            ["SELECT * FROM table_A"],
            [("A", 1), ("B", 2)],
            [SerializableRow("A", 1), SerializableRow("B", 2)],
            id="single_query_return_all",
        ),
        pytest.param(
            True,
            True,
            "SELECT * FROM table1; SELECT 1;",
            ["SELECT * FROM table1", "SELECT 1"],
            [[("Result1", 1)], [("Result2", 2)]],
            [SerializableRow("Result2", 2)],
            id="multi_query_return_last",
        ),
    ],
)

def test_run_single_query(
    presto_hook,
    mock_cursor,
    return_last,
    split_statements,
    sql,
    expected_calls,
    cursor_results,
    expected_result,
):
    """Tests various execution paths for PrestoHook.run"""

    if split_statements:
        mock_cursor.fetchall.side_effect = cursor_results
        mock_cursor.nextset.return_value = False
    else:
        mock_cursor.fetchall.return_value = cursor_results
        mock_cursor.nextset.side_effect = lambda: False

    result = presto_hook.run(
        sql, return_last=return_last, handler=lambda cur: cur.fetchall(), split_statements=split_statements
    )

    mock_cursor.execute.assert_has_calls([mock.call(sql_statement) for sql_statement in expected_calls])

    assert [SerializableRow(*row) for row in result] == expected_result
    


def test_get_sqlalchemy_engine(presto_hook, mock_connection, mocker):
    """Test that get_sqlalchemy_engine returns a SQLAlchemy engine with the correct URL."""
    mock_create_engine = mocker.patch("airflow.providers.presto.hooks.presto.create_engine", autospec=True)
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    engine = presto_hook.get_sqlalchemy_engine()

    assert engine is mock_engine, "Returned engine does not match the mocked engine."
    mock_create_engine.assert_called_once()

    call_args = mock_create_engine.call_args[1]
    actual_url = call_args["url"]

    assert actual_url.drivername == "presto"
    assert actual_url.host == DEFAULT_HOST
    assert actual_url.password == DEFAULT_PASSWORD
    assert actual_url.port == DEFAULT_PORT
    assert actual_url.username == DEFAULT_LOGIN
    assert actual_url.database == "hive"
    assert actual_url.query.get("protocol") == "http"
    assert actual_url.query.get("source") == "airflow"
    assert actual_url.query.get("schema") == "presto_db"



def test_run_with_multiple_statements(presto_hook, mock_cursor, mock_get_conn):
    """Test execution of a single string containing multiple queries."""

    mock_cursor.fetchall.side_effect = [[(1,)], [(2,)]]
    mock_cursor.nextset.return_value = False
    sql = "SELECT 1; SELECT 2;"

    results = presto_hook.run(
        sql,
        return_last=True,
        handler=lambda cur: cur.fetchall(),
        split_statements=True,
    )

    mock_cursor.execute.assert_has_calls(
        [
            mock.call("SELECT 1"),
            mock.call("SELECT 2"),
        ],
        any_order=False
    )
    mock_get_conn.return_value.cursor.assert_called_once()

    assert results == [(2,)]


def test_get_uri(presto_hook, mock_connection):
    """Test that get_uri returns the correct connection URI."""

    mock_connection.extra = json.dumps({"catalog": "hive", "protocol": "https", "source": "airflow"})
    expected_protocol = "https"
    expected_catalog = "hive"
    expected_schema = "presto_db"
    expected_source = "airflow"
    expected_database = expected_catalog

    uri = presto_hook.get_uri()
    parsed_uri = URL(uri)

    assert parsed_uri.host == DEFAULT_HOST
    assert parsed_uri.database == expected_database
    assert parsed_uri.query.get("protocol") == expected_protocol
    assert parsed_uri.query.get("source") == expected_source
    assert parsed_uri.query.get("schema") == expected_schema

    assert len(parsed_uri.query) == 3

@pytest.mark.parametrize("sql", ["", "\n", " "])
def test_run_with_empty_sql(presto_hook, sql):
    """Test that running with empty SQL raises an AirflowException."""
    with pytest.raises(ValueError, match="List of SQL statements is empty"):
        presto_hook.run(sql)



def test_with_invalid_names(presto_hook, mock_cursor):
    """Ensure PrestoHook.run handles queries returning reserved/invalid column names without raising errors."""
    invalid_names = ["1_2_3", "select", "from"]

    mock_cursor.description = get_cursor_descriptions(invalid_names)

    expected_data = [(1, "row1", "bar")]
    mock_cursor.fetchall.return_value = expected_data

    test_sql = "SELECT 1 as id, 'row1' as value, 'bar' as foo"

    result = presto_hook.run(test_sql, handler=lambda cur: cur.fetchall())

    mock_cursor.execute.assert_called_once_with(test_sql)

    assert result == expected_data
    assert result[0][0] == 1
    assert result[0][1] == "row1"
    assert result[0][2] == "bar"


@pytest.fixture
def presto_hook_timeout(create_connection_without_db):
    """Fixture for PrestoHook with a connection that has a timeout set."""
    create_connection_without_db(
        Connection(
            conn_id="presto_with_timeout",
            conn_type="presto",
            host=DEFAULT_HOST,
            login=DEFAULT_LOGIN,
            password=DEFAULT_PASSWORD,
            port=DEFAULT_PORT,
            schema="presto_db",
            extra=json.dumps({"catalog": "hive", "protocol": "http", "source": "airflow", "timeout": 10}),
        )
    )
    return PrestoHook(presto_conn_id="presto_with_timeout")


def test_execution_timeout_exceeded(presto_hook_timeout):
    """Test that a query exceeding the execution timeout raises an AirflowException."""
    test_sql = "SELECT large_data FROM slow_table"

    with patch(
        "airflow.providers.common.sql.hooks.sql.DbApiHook.run",
        side_effect=AirflowException("Query exceeded execution timeout"),
    ) as mock_parent_run:
        with pytest.raises(AirflowException, match="Query exceeded execution timeout"):
            presto_hook_timeout.run(sql=test_sql)

        mock_parent_run.assert_called_once()
