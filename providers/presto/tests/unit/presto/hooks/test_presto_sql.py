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
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from sqlalchemy.engine.url import make_url

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
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
def mock_connection(create_connection_without_db) -> Connection:
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
def presto_hook() -> PrestoHook:
    """Fixture for PrestoHook with mocked connection."""
    return PrestoHook(presto_conn_id=DEFAULT_CONN_ID)


@pytest.fixture
def mock_get_conn():
    """Fixture to mock get_conn method of PrestoHook."""
    with patch.object(PrestoHook, "get_conn", autospec=True) as mock:
        yield mock


@pytest.fixture
def mock_cursor(mock_get_conn: MagicMock | mock.AsyncMock):
    """Fixture to mock cursor returned by get_conn."""
    cursor = MagicMock()
    type(cursor).rowcount = PropertyMock(return_value=1)
    cursor.fetchall.return_value = [("1", "row1")]
    cursor.description = get_cursor_descriptions(["id", "value"])
    cursor.nextset.side_effect = [False]
    mock_get_conn.return_value.cursor.return_value = cursor
    return cursor


@pytest.mark.parametrize(
    (
        "custom_extra",
        "expected_catalog",
        "expected_protocol",
        "expected_source",
        "conn_schema_override",
        "expected_schema",
    ),
    [
        pytest.param(
            {"catalog": "reporting_db", "protocol": "https", "source": "airflow"},
            "reporting_db",
            "https",
            "airflow",
            "data_schema",
            "data_schema",
            id="custom_catalog_and_protocol",
        ),
        pytest.param(
            {"source": "my_dag_run"},
            None,
            "http",
            "my_dag_run",
            "test_schema",
            "test_schema",
            id="missing_protocol_should_default_http",
        ),
        pytest.param(
            {"protocol": None, "catalog": "logs"},
            "logs",
            "http",
            "airflow",
            None,
            None,
            id="explicit_protocol_none_should_default_http",
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
    create_connection_without_db,
    custom_extra,
    expected_catalog,
    expected_protocol,
    expected_source,
    conn_schema_override,
    expected_schema,
):
    """Tests various custom configurations passed via the 'extra' field."""

    # Create a real Airflow connection
    temp_conn = Connection(
        conn_id=DEFAULT_CONN_ID,
        conn_type="presto",
        host=DEFAULT_HOST,
        login=DEFAULT_LOGIN,
        password=DEFAULT_PASSWORD,
        port=DEFAULT_PORT,
        schema=conn_schema_override or "",
        extra=json.dumps(custom_extra) if custom_extra else None,
    )
    create_connection_without_db(temp_conn)

    with patch.object(presto_hook, "get_connection", return_value=temp_conn):
        url = presto_hook.sqlalchemy_url

        assert url.host == DEFAULT_HOST
        assert url.port == DEFAULT_PORT
        assert url.username == DEFAULT_LOGIN
        assert url.password == DEFAULT_PASSWORD
        assert url.database == custom_extra.get("catalog")

        query = url.query

        assert query.get("protocol") == custom_extra.get("protocol")
        assert query.get("source") == custom_extra.get("source")
        assert query.get("schema") == temp_conn.schema


@pytest.mark.parametrize(
    ("return_last", "split_statements", "sql", "expected_calls", "cursor_results", "expected_result"),
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

    mock_create_engine = mocker.patch("airflow.providers.common.sql.hooks.sql.create_engine", autospec=True)
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    with patch.object(presto_hook, "get_connection") as mock_get_connection:
        mock_get_connection.return_value = mock_connection
    engine = presto_hook.get_sqlalchemy_engine()

    assert engine is mock_engine, "Returned engine does not match the mocked engine."
    mock_create_engine.assert_called_once()

    call_args = mock_create_engine.call_args[1]
    actual_url = call_args["url"]
    extra_dict = json.loads(mock_connection.extra or "{}")

    assert actual_url.drivername == "presto"
    assert actual_url.host == str(DEFAULT_HOST)
    assert actual_url.password == (DEFAULT_PASSWORD or "")
    assert actual_url.port == DEFAULT_PORT
    assert actual_url.username == DEFAULT_LOGIN
    assert actual_url.database == extra_dict.get("catalog")
    assert actual_url.query.get("protocol") == extra_dict.get("protocol")
    assert actual_url.query.get("source") == extra_dict.get("source")
    assert actual_url.query.get("schema") == mock_connection.schema


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
        any_order=False,
    )
    mock_get_conn.return_value.cursor.assert_called_once()

    assert results == [(2,)]


def test_get_uri(presto_hook, mock_connection):
    """Test that get_uri returns the correct connection URI with debug prints."""

    # Ensure all connection attributes are explicitly set
    mock_connection.host = DEFAULT_HOST
    mock_connection.port = DEFAULT_PORT
    mock_connection.login = DEFAULT_LOGIN
    mock_connection.password = DEFAULT_PASSWORD
    mock_connection.extra = json.dumps({"catalog": "hive", "protocol": "https", "source": "airflow"})
    mock_connection.schema = "presto_db"

    expected_uri = (
        "presto://test:test_pass@test_host:8080/hive?protocol=https&source=airflow&schema=presto_db"
    )

    with patch.object(presto_hook, "get_connection", return_value=mock_connection):
        uri = presto_hook.get_uri()
    parsed = make_url(uri)
    expected = make_url(expected_uri)

    assert parsed.drivername == expected.drivername
    assert parsed.username == expected.username
    assert parsed.password == expected.password
    assert parsed.host == expected.host
    assert parsed.port == expected.port
    assert parsed.database == expected.database
    for key, value in expected.query.items():
        assert parsed.query.get(key) == value


@pytest.mark.parametrize("sql", ["", "\n", " "])
def test_run_with_empty_sql(presto_hook, sql):
    """Test that running with empty SQL raises an ValueError."""
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
def presto_hook_with_timeout(create_connection_without_db):
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


def test_execution_timeout_exceeded(presto_hook_with_timeout):
    """Test that a query exceeding the execution timeout raises an AirflowException."""
    test_sql = "SELECT large_data FROM slow_table"

    with patch(
        "airflow.providers.common.sql.hooks.sql.DbApiHook.run",
        side_effect=AirflowException("Query exceeded execution timeout"),
    ) as mock_parent_run:
        with pytest.raises(AirflowException, match="Query exceeded execution timeout"):
            presto_hook_with_timeout.run(sql=test_sql)

        mock_parent_run.assert_called_once()
