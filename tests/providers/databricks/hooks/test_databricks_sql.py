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

from unittest import mock
from unittest.mock import patch

import pytest

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
    return DatabricksSqlHook(sql_endpoint_name="Test")


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    return [(field,) for field in fields]


@pytest.mark.parametrize(
    "return_last, split_statements, sql, cursor_calls,"
    "cursor_descriptions, cursor_results, hook_descriptions, hook_results, ",
    [
        pytest.param(
            True,
            False,
            "select * from test.test",
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[1, 2], [11, 12]],
            id="The return_last set and no split statements set on single query in string",
        ),
        pytest.param(
            False,
            False,
            "select * from test.test;",
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[1, 2], [11, 12]],
            id="The return_last not set and no split statements set on single query in string",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;",
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[1, 2], [11, 12]],
            id="The return_last set and split statements set on single query in string",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;",
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[[1, 2], [11, 12]]],
            id="The return_last not set and split statements set on single query in string",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id2",), ("value2",)]],
            [[3, 4], [13, 14]],
            id="The return_last set and split statements set on multiple queries in string",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [[[1, 2], [11, 12]], [[3, 4], [13, 14]]],
            id="The return_last not set and split statements set on multiple queries in string",
        ),
        pytest.param(
            True,
            True,
            ["select * from test.test;"],
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[[1, 2], [11, 12]]],
            id="The return_last set on single query in list",
        ),
        pytest.param(
            False,
            True,
            ["select * from test.test;"],
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[[1, 2], [11, 12]]],
            id="The return_last not set on single query in list",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id2",), ("value2",)]],
            [[3, 4], [13, 14]],
            id="The return_last set on multiple queries in list",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [[[1, 2], [11, 12]], [[3, 4], [13, 14]]],
            id="The return_last not set on multiple queries not set",
        ),
    ],
)
def test_query(
    databricks_hook,
    return_last,
    split_statements,
    sql,
    cursor_calls,
    cursor_descriptions,
    cursor_results,
    hook_descriptions,
    hook_results,
):
    with patch(
        "airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook.get_conn"
    ) as mock_conn, patch("airflow.providers.databricks.hooks.databricks_base.requests") as mock_requests:
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
