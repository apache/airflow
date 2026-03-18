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
from sqlalchemy.engine import Engine

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.vertica.hooks.vertica import VerticaHook

DEFAULT_CONN_ID = "vertica_default"
HOST = "vertica.cloud.com"
PORT = 5433
USER = "user"
PASSWORD = "pass"
DATABASE = "test_db"

SerializableRow = namedtuple("SerializableRow", ["id", "value"])


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    """Convert field names into cursor.description tuples."""
    return [(field,) for field in fields]


@pytest.fixture(autouse=True)
def create_connection(create_connection_without_db):
    """Create a mocked Airflow connection for Vertica."""
    create_connection_without_db(
        Connection(
            conn_id=DEFAULT_CONN_ID,
            conn_type="vertica",
            host=HOST,
            login=USER,
            password=PASSWORD,
            schema=DATABASE,
        )
    )


@pytest.fixture
def vertica_hook():
    return VerticaHook(vertica_conn_id=DEFAULT_CONN_ID)


@pytest.fixture
def mock_get_conn():
    with patch("airflow.providers.vertica.hooks.vertica.VerticaHook.get_conn") as mock_conn:
        yield mock_conn


@pytest.fixture
def mock_cursor(mock_get_conn):
    cursor = MagicMock()
    type(cursor).rowcount = PropertyMock(return_value=1)
    cursor.fetchall.return_value = [("1", "row1")]
    cursor.description = get_cursor_descriptions(["id", "value"])
    cursor.nextset.side_effect = [False]
    mock_get_conn.return_value.cursor.return_value = cursor
    return cursor


def test_sqlalchemy_url_property(vertica_hook):
    url = vertica_hook.sqlalchemy_url.render_as_string(hide_password=False)
    expected_url = f"vertica-python://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    assert url.startswith(expected_url)


@pytest.mark.parametrize(
    ("return_last", "split_statements", "sql", "expected_calls", "cursor_results", "expected_result"),
    [
        pytest.param(
            True,
            False,
            "SELECT * FROM table",
            ["SELECT * FROM table"],
            [("1", "row1"), ("2", "row2")],
            [SerializableRow("1", "row1"), SerializableRow("2", "row2")],
            id="Single query, return_last=True",
        ),
        pytest.param(
            False,
            False,
            "SELECT * FROM table",
            ["SELECT * FROM table"],
            [("1", "row1"), ("2", "row2")],
            [SerializableRow("1", "row1"), SerializableRow("2", "row2")],
            id="Single query, return_last=False",
        ),
        pytest.param(
            True,
            True,
            "SELECT * FROM table1; SELECT * FROM table2;",
            ["SELECT * FROM table1;", "SELECT * FROM table2;"],
            [[("1", "row1"), ("2", "row2")], [("3", "row3"), ("4", "row4")]],
            [SerializableRow("3", "row3"), SerializableRow("4", "row4")],
            id="Multiple queries, split_statements=True, return_last=True",
        ),
        pytest.param(
            True,
            False,
            "SELECT * FROM table1; SELECT * FROM table2;",
            ["SELECT * FROM table1; SELECT * FROM table2;"],
            [("1", "row1"), ("2", "row2")],
            [SerializableRow("1", "row1"), SerializableRow("2", "row2")],
            id="Multiple queries, split_statements=False",
        ),
        pytest.param(
            True,
            False,
            "SELECT * FROM empty",
            ["SELECT * FROM empty"],
            [],
            [],
            id="Empty result",
        ),
    ],
)
def test_vertica_run_queries(
    vertica_hook,
    mock_cursor,
    return_last,
    split_statements,
    sql,
    expected_calls,
    cursor_results,
    expected_result,
):
    if split_statements:
        mock_cursor.fetchall.side_effect = cursor_results
        mock_cursor.nextset.side_effect = [True] * (len(cursor_results) - 1) + [False]
    else:
        mock_cursor.fetchall.return_value = cursor_results
        mock_cursor.nextset.side_effect = lambda: False

    result = vertica_hook.run(
        sql,
        handler=lambda cur: cur.fetchall(),
        split_statements=split_statements,
        return_last=return_last,
    )

    expected_mock_calls = [mock.call(sql_call) for sql_call in expected_calls]
    mock_cursor.execute.assert_has_calls(expected_mock_calls)
    assert [SerializableRow(*row) for row in result] == expected_result


def test_run_with_multiple_statements(vertica_hook, mock_cursor):
    mock_cursor.fetchall.side_effect = [[(1,)], [(2,)]]

    mock_cursor.nextset.side_effect = [True, False]

    sql = "SELECT 1; SELECT 2;"

    results = vertica_hook.run(sql, handler=lambda cur: cur.fetchall(), split_statements=True)

    mock_cursor.execute.assert_has_calls(
        [
            mock.call("SELECT 1;"),
            mock.call("SELECT 2;"),
        ]
    )

    assert results == [(2,)]


def test_get_uri(vertica_hook):
    """
    Test that the get_uri() method returns the correct connection string.
    """
    assert vertica_hook.get_uri() == "vertica-python://user:pass@vertica.cloud.com:5433/test_db"


def test_get_sqlalchemy_engine(vertica_hook):
    """
    Test that the get_sqlalchemy_engine() method returns a valid SQLAlchemy engine.
    """

    with patch("airflow.providers.common.sql.hooks.sql.create_engine") as mock_create_engine:
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        engine = vertica_hook.get_sqlalchemy_engine()

        assert engine is mock_engine

        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert "url" in call_args

        actual_url = call_args["url"]

        assert actual_url.drivername == "vertica-python"
        assert actual_url.username == "user"
        assert actual_url.password == "pass"
        assert actual_url.host == "vertica.cloud.com"
        assert actual_url.port == 5433
        assert actual_url.database == "test_db"


@pytest.mark.parametrize("sql", ["", "\n", " "])
def test_run_with_no_query(vertica_hook, sql):
    """
    Test that running with no SQL query raises a ValueError.
    """
    with pytest.raises(ValueError, match="List of SQL statements is empty"):
        vertica_hook.run(sql)


def test_run_with_invalid_column_names(vertica_hook, mock_cursor):
    invalid_names = [("1_2_3",), ("select",), ("from",)]
    mock_cursor.description = invalid_names
    mock_cursor.fetchall.return_value = [(1, "row1", "bar")]

    result = vertica_hook.run(sql="SELECT * FROM table", handler=lambda cur: cur.fetchall())

    assert result[0][0] == 1
    assert result[0][1] == "row1"
    assert result[0][2] == "bar"


@pytest.fixture
def vertica_hook_with_timeout(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id="vertica_timeout",
            conn_type="vertica",
            host="vertica.cloud.com",
            login="user",
            password="pass",
            schema="test_db",
            extra=json.dumps({"execution_timeout": 1}),
        )
    )
    return VerticaHook(vertica_conn_id="vertica_timeout")


def test_execution_timeout_exceeded(vertica_hook_with_timeout, mock_cursor):
    mock_error_response = MagicMock()
    mock_error_response.error_message.return_value = "Mock error message for test."

    with patch(
        "airflow.providers.common.sql.hooks.sql.DbApiHook.run",
        side_effect=AirflowException("Query exceeded execution timeout"),
    ):
        with pytest.raises(AirflowException, match="Query exceeded execution timeout"):
            vertica_hook_with_timeout.run(sql="SELECT * FROM table1")
