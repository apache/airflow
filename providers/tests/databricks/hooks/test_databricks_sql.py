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

from collections import namedtuple
from unittest import mock
from unittest.mock import patch

import pytest
from databricks.sql.types import Row

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.utils.session import provide_session

pytestmark = pytest.mark.db_test

TASK_ID = "databricks-sql-operator"
DEFAULT_CONN_ID = "databricks_default"
HOST = "xx.cloud.databricks.com"
HOST_WITH_SCHEME = "https://xx.cloud.databricks.com"
TOKEN = "token"


@provide_session
@pytest.fixture(autouse=True)
def create_connection(session):
    conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
    conn.host = HOST
    conn.login = None
    conn.password = TOKEN
    conn.extra = None
    session.commit()


@pytest.fixture
def databricks_hook():
    return DatabricksSqlHook(sql_endpoint_name="Test", return_tuple=True)


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    return [(field,) for field in fields]


# Serializable Row object similar to the one returned by the Hook
SerializableRow = namedtuple("Row", ["id", "value"])  # type: ignore[name-match]


@pytest.mark.parametrize(
    "return_last, split_statements, sql, cursor_calls, return_tuple,"
    "cursor_descriptions, cursor_results, hook_descriptions, hook_results, ",
    [
        pytest.param(
            True,
            False,
            "select * from test.test",
            ["select * from test.test"],
            False,
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
            ["select * from test.test"],
            False,
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
            ["select * from test.test"],
            False,
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
            ["select * from test.test"],
            False,
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
            ["select * from test.test", "select * from test.test2"],
            False,
            [["id", "value"], ["id2", "value2"]],
            ([Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]),
            [[("id2",), ("value2",)]],
            [Row(id=3, value=4), Row(id=13, value=14)],
            id="The return_last set and split statements set on multiple queries in string",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            False,
            [["id", "value"], ["id2", "value2"]],
            ([Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]),
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
            ["select * from test.test"],
            False,
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
            ["select * from test.test"],
            False,
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
            ["select * from test.test", "select * from test.test2"],
            False,
            [["id", "value"], ["id2", "value2"]],
            ([Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]),
            [[("id2",), ("value2",)]],
            [Row(id=3, value=4), Row(id=13, value=14)],
            id="The return_last set on multiple queries in list",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            False,
            [["id", "value"], ["id2", "value2"]],
            ([Row(id=1, value=2), Row(id=11, value=12)], [Row(id=3, value=4), Row(id=13, value=14)]),
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
            ["select * from test.test"],
            True,
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
            ["select * from test.test"],
            True,
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
            ["select * from test.test"],
            True,
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
    cursor_calls,
    return_tuple,
    cursor_descriptions,
    cursor_results,
    hook_descriptions,
    hook_results,
):
    with (
        patch("airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook.get_conn") as mock_conn,
        patch("airflow.providers.databricks.hooks.databricks_base.requests") as mock_requests,
    ):
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
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.get.return_value).status_code = status_code_mock
        connections = []
        cursors = []
        for index in range(len(cursor_descriptions)):
            conn = mock.MagicMock()
            cur = mock.MagicMock(
                rowcount=len(cursor_results[index]),
                description=get_cursor_descriptions(cursor_descriptions[index]),
            )
            cur.fetchall.return_value = cursor_results[index]
            conn.cursor.return_value = cur
            cursors.append(cur)
            connections.append(conn)
        mock_conn.side_effect = connections

        if not return_tuple:
            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="""Returning a raw `databricks.sql.Row` object is deprecated. A namedtuple will be
                returned instead in a future release of the databricks provider. Set `return_tuple=True` to
                enable this behavior.""",
            ):
                databricks_hook = DatabricksSqlHook(sql_endpoint_name="Test", return_tuple=return_tuple)
        else:
            databricks_hook = DatabricksSqlHook(sql_endpoint_name="Test", return_tuple=return_tuple)
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
    with pytest.raises(ValueError) as err:
        databricks_hook.run(sql=empty_statement)
    assert err.value.args[0] == "List of SQL statements is empty"


@pytest.mark.parametrize(
    "row_objects, fields_names",
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
    result = DatabricksSqlHook(return_tuple=True)._make_common_data_structure(row_objects)
    assert result._fields == fields_names
