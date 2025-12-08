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
from __future__ import annotations

import threading
from collections import namedtuple
from datetime import timedelta
from unittest import mock
from unittest.mock import PropertyMock, patch
from urllib.parse import quote_plus

import pandas as pd
import polars as pl
import pytest
from databricks.sql.types import Row

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook, create_timeout_thread

TASK_ID = "databricks-sql-operator"
DEFAULT_CONN_ID = "databricks_default"
HOST = "xx.cloud.databricks.com"
HOST_WITH_SCHEME = "https://xx.cloud.databricks.com"
TOKEN = "token"
HTTP_PATH = "sql/protocolv1/o/1234567890123456/0123-456789-abcd123"
SCHEMA = "test_schema"
CATALOG = "test_catalog"


@pytest.fixture(autouse=True)
def create_connection(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id=DEFAULT_CONN_ID,
            conn_type="databricks",
            host=HOST,
            login=None,
            password=TOKEN,
            extra=None,
        )
    )


@pytest.fixture
def databricks_hook():
    return DatabricksSqlHook(sql_endpoint_name="Test")


@pytest.fixture
def mock_get_conn():
    # Start the patcher
    mock_patch = patch("airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook.get_conn")
    mock_conn = mock_patch.start()
    # Use yield to provide the mock object
    yield mock_conn
    # Stop the patcher
    mock_patch.stop()


@pytest.fixture
def mock_get_requests():
    # Start the patcher
    mock_patch = patch("airflow.providers.databricks.hooks.databricks_base.requests")
    mock_requests = mock_patch.start()

    # Configure the mock object
    mock_requests.codes.ok = 200
    mock_requests.get.return_value.json.return_value = {
        "endpoints": [
            {
                "id": "1264e5078741679a",
                "name": "Test",
                "odbc_params": {
                    "hostname": "xx.cloud.databricks.com",
                    "path": "/sql/1.0/endpoints/1264e5078741679a",
                },
            }
        ]
    }
    status_code_mock = PropertyMock(return_value=200)
    type(mock_requests.get.return_value).status_code = status_code_mock

    # Yield the mock object
    yield mock_requests

    # Stop the patcher after the test
    mock_patch.stop()


@pytest.fixture
def mock_timer():
    with patch("threading.Timer") as mock_timer:
        yield mock_timer


def test_sqlachemy_url_property():
    hook = DatabricksSqlHook(
        databricks_conn_id=DEFAULT_CONN_ID, http_path=HTTP_PATH, catalog=CATALOG, schema=SCHEMA
    )
    url = hook.sqlalchemy_url.render_as_string(hide_password=False)
    expected_url = (
        f"databricks://token:{TOKEN}@{HOST}?"
        f"catalog={CATALOG}&http_path={quote_plus(HTTP_PATH)}&schema={SCHEMA}"
    )
    assert url == expected_url


def test_get_sqlalchemy_engine():
    hook = DatabricksSqlHook(
        databricks_conn_id=DEFAULT_CONN_ID, http_path=HTTP_PATH, catalog=CATALOG, schema=SCHEMA
    )
    engine = hook.get_sqlalchemy_engine()
    assert engine.url.render_as_string(hide_password=False) == (
        f"databricks://token:{TOKEN}@{HOST}?"
        f"catalog={CATALOG}&http_path={quote_plus(HTTP_PATH)}&schema={SCHEMA}"
    )


def test_get_uri():
    hook = DatabricksSqlHook(
        databricks_conn_id=DEFAULT_CONN_ID, http_path=HTTP_PATH, catalog=CATALOG, schema=SCHEMA
    )
    uri = hook.get_uri()
    expected_uri = (
        f"databricks://token:{TOKEN}@{HOST}?"
        f"catalog={CATALOG}&http_path={quote_plus(HTTP_PATH)}&schema={SCHEMA}"
    )
    assert uri == expected_uri


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    return [(field,) for field in fields]


# Serializable Row object similar to the one returned by the Hook
SerializableRow = namedtuple("Row", ["id", "value"])  # type: ignore[name-match]


@pytest.mark.parametrize(
    (
        "return_last",
        "split_statements",
        "sql",
        "execution_timeout",
        "cursor_calls",
        "cursor_descriptions",
        "cursor_results",
        "hook_descriptions",
        "hook_results",
    ),
    [
        pytest.param(
            True,
            False,
            "select * from test.test",
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row(id=1, value=2), Row(id=11, value=12)],),
            [[("id",), ("value",)]],
            [Row(id=1, value=2), Row(id=11, value=12)],
            id="The return_last set and no split statements set on single query in string",
        ),
        pytest.param(
            False,
            False,
            "select * from test.test;",
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row(id=1, value=2), Row(id=11, value=12)],),
            [[("id",), ("value",)]],
            [Row(id=1, value=2), Row(id=11, value=12)],
            id="The return_last not set and no split statements set on single query in string",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;",
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row(id=1, value=2), Row(id=11, value=12)],),
            [[("id",), ("value",)]],
            [Row(id=1, value=2), Row(id=11, value=12)],
            id="The return_last set and split statements set on single query in string",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;",
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row(id=1, value=2), Row(id=11, value=12)],),
            [[("id",), ("value",)]],
            [[Row(id=1, value=2), Row(id=11, value=12)]],
            id="The return_last not set and split statements set on single query in string",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;select * from test.test2;",
            None,
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            [[Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]],
            [[("id2",), ("value2",)]],
            [Row(id=3, value=4), Row(id=13, value=14)],
            id="The return_last set and split statements set on multiple queries in string",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            None,
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            [[Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                [Row(id=1, value=2), Row(id=11, value=12)],
                [Row(id=3, value=4), Row(id=13, value=14)],
            ],
            id="The return_last not set and split statements set on multiple queries in string",
        ),
        pytest.param(
            True,
            True,
            ["select * from test.test;"],
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row(id=1, value=2), Row(id=11, value=12)],),
            [[("id",), ("value",)]],
            [[Row(id=1, value=2), Row(id=11, value=12)]],
            id="The return_last set on single query in list",
        ),
        pytest.param(
            False,
            True,
            ["select * from test.test;"],
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row(id=1, value=2), Row(id=11, value=12)],),
            [[("id",), ("value",)]],
            [[Row(id=1, value=2), Row(id=11, value=12)]],
            id="The return_last not set on single query in list",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;select * from test.test2;",
            None,
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            [[Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]],
            [[("id2",), ("value2",)]],
            [Row(id=3, value=4), Row(id=13, value=14)],
            id="The return_last set on multiple queries in list",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            None,
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            [[Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                [Row(id=1, value=2), Row(id=11, value=12)],
                [Row(id=3, value=4), Row(id=13, value=14)],
            ],
            id="The return_last not set on multiple queries not set",
        ),
        pytest.param(
            True,
            False,
            "select * from test.test",
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row("id", "value")(1, 2)],),
            [[("id",), ("value",)]],
            [SerializableRow(1, 2)],
            id="Return a serializable row (tuple) from a row instance created in two step",
        ),
        pytest.param(
            True,
            False,
            "select * from test.test",
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([Row(id=1, value=2)],),
            [[("id",), ("value",)]],
            [SerializableRow(1, 2)],
            id="Return a serializable row (tuple) from a row instance created in one step",
        ),
        pytest.param(
            True,
            False,
            "select * from test.test",
            None,
            ["select * from test.test"],
            [["id", "value"]],
            ([],),
            [[("id",), ("value",)]],
            [],
            id="Empty list",
        ),
    ],
)
def test_query(
    return_last,
    split_statements,
    sql,
    execution_timeout,
    cursor_calls,
    cursor_descriptions,
    cursor_results,
    hook_descriptions,
    hook_results,
    mock_get_conn,
    mock_get_requests,
):
    connections = []
    cursors = []

    for index, cursor_description in enumerate(cursor_descriptions):
        conn = mock.MagicMock()
        cur = mock.MagicMock(
            rowcount=len(cursor_results[index]),
            description=get_cursor_descriptions(cursor_description),
        )
        cur.fetchall.return_value = cursor_results[index]
        conn.cursor.return_value = cur
        cursors.append(cur)
        connections.append(conn)
    mock_get_conn.side_effect = connections

    databricks_hook = DatabricksSqlHook(sql_endpoint_name="Test")
    results = databricks_hook.run(
        sql=sql, handler=fetch_all_handler, return_last=return_last, split_statements=split_statements
    )

    assert databricks_hook.descriptions == hook_descriptions
    assert databricks_hook.last_description == hook_descriptions[-1]
    assert results == hook_results

    for index, cur in enumerate(cursors):
        cur.execute.assert_has_calls([mock.call(cursor_calls[index])])
    cur.close.assert_called()


@pytest.mark.parametrize(
    "empty_statement",
    [
        pytest.param([], id="Empty list"),
        pytest.param("", id="Empty string"),
        pytest.param("\n", id="Only EOL"),
    ],
)
def test_no_query(databricks_hook, empty_statement):
    with pytest.raises(ValueError, match="List of SQL statements is empty"):
        databricks_hook.run(sql=empty_statement)


@pytest.mark.parametrize(
    ("row_objects", "fields_names"),
    [
        pytest.param(Row("count(1)")(9714), ("_0",)),
        pytest.param(Row("1//@:()")("data"), ("_0",)),
        pytest.param(Row("class")("data"), ("_0",)),
        pytest.param(Row("1_wrong", "2_wrong")(1, 2), ("_0", "_1")),
    ],
)
def test_incorrect_column_names(row_objects, fields_names):
    """Ensure that column names can be used as namedtuple attribute.

    namedtuple do not accept special characters and reserved python keywords
    as column name. This test ensure that such columns are renamed.
    """
    result = DatabricksSqlHook()._make_common_data_structure(row_objects)
    assert result._fields == fields_names


@pytest.mark.parametrize(
    ("sql", "execution_timeout", "cursor_descriptions", "cursor_results"),
    [
        (
            "select * from test.test",
            timedelta(microseconds=0),
            ("id", "value"),
            (Row(id=1, value=2), Row(id=11, value=12)),
        )
    ],
)
def test_execution_timeout_exceeded(
    mock_get_conn,
    mock_get_requests,
    sql,
    execution_timeout,
    cursor_descriptions,
    cursor_results,
):
    with (
        patch(
            "airflow.providers.databricks.hooks.databricks_sql.create_timeout_thread"
        ) as mock_create_timeout_thread,
        patch.object(DatabricksSqlHook, "_run_command") as mock_run_command,
    ):
        conn = mock.MagicMock()
        cur = mock.MagicMock(
            rowcount=len(cursor_results),
            description=get_cursor_descriptions(cursor_descriptions),
        )

        # Simulate a timeout
        mock_create_timeout_thread.return_value = threading.Timer(cur, execution_timeout)

        mock_run_command.side_effect = Exception("Mocked exception")

        cur.fetchall.return_value = cursor_results
        conn.cursor.return_value = cur
        mock_get_conn.side_effect = [conn]

        with pytest.raises(AirflowException) as exc_info:
            DatabricksSqlHook(sql_endpoint_name="Test").run(
                sql=sql,
                execution_timeout=execution_timeout,
                handler=fetch_all_handler,
            )

        assert "Timeout threshold exceeded" in str(exc_info.value)


@pytest.mark.parametrize(
    "cursor_descriptions",
    [(("id", "value"),)],
)
def test_create_timeout_thread(
    mock_get_conn,
    mock_get_requests,
    mock_timer,
    cursor_descriptions,
):
    cur = mock.MagicMock(
        rowcount=1,
        description=get_cursor_descriptions(cursor_descriptions),
    )
    timeout = timedelta(seconds=1)
    thread = create_timeout_thread(cur=cur, execution_timeout=timeout)
    mock_timer.assert_called_once_with(timeout.total_seconds(), cur.connection.cancel)
    assert thread is not None


@pytest.mark.parametrize(
    "cursor_descriptions",
    [(("id", "value"),)],
)
def test_create_timeout_thread_no_timeout(
    mock_get_conn,
    mock_get_requests,
    mock_timer,
    cursor_descriptions,
):
    cur = mock.MagicMock(
        rowcount=1,
        description=get_cursor_descriptions(cursor_descriptions),
    )
    thread = create_timeout_thread(cur=cur, execution_timeout=None)
    mock_timer.assert_not_called()
    assert thread is None


def test_get_openlineage_default_schema_with_no_schema_set():
    hook = DatabricksSqlHook()
    assert hook.get_openlineage_default_schema() == "default"


def test_get_openlineage_default_schema_with_schema_set():
    hook = DatabricksSqlHook(schema="my-schema")
    assert hook.get_openlineage_default_schema() == "my-schema"


def test_get_openlineage_database_specific_lineage_with_no_query_id():
    hook = DatabricksSqlHook()
    hook.query_ids = []

    result = hook.get_openlineage_database_specific_lineage(None)
    assert result is None


@mock.patch("airflow.providers.databricks.utils.openlineage.emit_openlineage_events_for_databricks_queries")
def test_get_openlineage_database_specific_lineage_with_single_query_id(mock_emit):
    from airflow.providers.common.compat.openlineage.facet import ExternalQueryRunFacet
    from airflow.providers.openlineage.extractors import OperatorLineage

    hook = DatabricksSqlHook()
    hook.query_ids = ["query1"]
    hook.get_connection = mock.MagicMock()
    hook.get_openlineage_database_info = lambda x: mock.MagicMock(authority="auth", scheme="scheme")

    ti = mock.MagicMock()

    result = hook.get_openlineage_database_specific_lineage(ti)
    mock_emit.assert_called_once_with(
        **{
            "hook": hook,
            "query_ids": ["query1"],
            "query_source_namespace": "scheme://auth",
            "task_instance": ti,
            "query_for_extra_metadata": True,
        }
    )
    assert result == OperatorLineage(
        run_facets={"externalQuery": ExternalQueryRunFacet(externalQueryId="query1", source="scheme://auth")}
    )


@mock.patch("airflow.providers.databricks.utils.openlineage.emit_openlineage_events_for_databricks_queries")
def test_get_openlineage_database_specific_lineage_with_multiple_query_ids(mock_emit):
    hook = DatabricksSqlHook()
    hook.query_ids = ["query1", "query2"]
    hook.get_connection = mock.MagicMock()
    hook.get_openlineage_database_info = lambda x: mock.MagicMock(authority="auth", scheme="scheme")

    ti = mock.MagicMock()

    result = hook.get_openlineage_database_specific_lineage(ti)
    mock_emit.assert_called_once_with(
        **{
            "hook": hook,
            "query_ids": ["query1", "query2"],
            "query_source_namespace": "scheme://auth",
            "task_instance": ti,
            "query_for_extra_metadata": True,
        }
    )
    assert result is None


@mock.patch("importlib.metadata.version", return_value="1.99.0")
def test_get_openlineage_database_specific_lineage_with_old_openlineage_provider(mock_version):
    hook = DatabricksSqlHook()
    hook.query_ids = ["query1", "query2"]
    hook.get_connection = mock.MagicMock()
    hook.get_openlineage_database_info = lambda x: mock.MagicMock(authority="auth", scheme="scheme")

    expected_err = (
        "OpenLineage provider version `1.99.0` is lower than required `2.3.0`, "
        "skipping function `emit_openlineage_events_for_databricks_queries` execution"
    )
    with pytest.raises(AirflowOptionalProviderFeatureException, match=expected_err):
        hook.get_openlineage_database_specific_lineage(mock.MagicMock())


@pytest.mark.parametrize(
    ("df_type", "df_class", "description"),
    [
        pytest.param("pandas", pd.DataFrame, [(("col",))], id="pandas-dataframe"),
        pytest.param(
            "polars",
            pl.DataFrame,
            [(("col", None, None, None, None, None, None))],
            id="polars-dataframe",
        ),
    ],
)
def test_get_df(df_type, df_class, description):
    hook = DatabricksSqlHook()
    statement = "SQL"
    column = "col"
    result_sets = [("row1",), ("row2",)]

    with mock.patch(
        "airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook.get_conn"
    ) as mock_get_conn:
        if df_type == "pandas":
            # Setup for pandas test case
            mock_cursor = mock.MagicMock()
            mock_cursor.description = description
            mock_cursor.fetchall.return_value = result_sets
            mock_get_conn.return_value.cursor.return_value = mock_cursor
        else:
            # Setup for polars test case
            mock_execute = mock.MagicMock()
            mock_execute.description = description
            mock_execute.fetchall.return_value = result_sets

            mock_cursor = mock.MagicMock()
            mock_cursor.execute.return_value = mock_execute
            mock_get_conn.return_value.cursor.return_value = mock_cursor

        df = hook.get_df(statement, df_type=df_type)
        mock_cursor.execute.assert_called_once_with(statement)

        if df_type == "pandas":
            mock_cursor.fetchall.assert_called_once_with()
            assert df.columns[0] == column
            assert df.iloc[0, 0] == "row1"
            assert df.iloc[1, 0] == "row2"
        else:
            mock_execute.fetchall.assert_called_once_with()
            assert df.columns[0] == column
            assert df.row(0)[0] == result_sets[0][0]
            assert df.row(1)[0] == result_sets[1][0]

        assert isinstance(df, df_class)
