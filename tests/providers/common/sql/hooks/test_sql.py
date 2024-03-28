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

import warnings
from typing import Any
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook, fetch_all_handler
from airflow.utils.session import provide_session

TASK_ID = "sql-operator"
HOST = "host"
DEFAULT_CONN_ID = "sqlite_default"
PASSWORD = "password"


class DBApiHookForTests(DbApiHook):
    conn_name_attr = "conn_id"
    get_conn = MagicMock(name="conn")


@provide_session
@pytest.fixture(autouse=True)
def create_connection(session):
    conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
    conn.host = HOST
    conn.login = None
    conn.password = PASSWORD
    conn.extra = None
    session.commit()


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    return [(field,) for field in fields]


index = 0


@pytest.mark.db_test
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
            ["select * from test.test;"],
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
            ["select * from test.test;"],
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
            ["select * from test.test;"],
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
            ["select * from test.test;", "select * from test.test2;"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id2",), ("value2",)]],
            [[3, 4], [13, 14]],
            id="The return_last set and split statements set on multiple queries in string",
        ),  # Failing
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test;", "select * from test.test2;"],
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
    return_last,
    split_statements,
    sql,
    cursor_calls,
    cursor_descriptions,
    cursor_results,
    hook_descriptions,
    hook_results,
):
    modified_descriptions = [
        get_cursor_descriptions(cursor_description) for cursor_description in cursor_descriptions
    ]
    dbapi_hook = DBApiHookForTests()
    dbapi_hook.get_conn.return_value.cursor.return_value.rowcount = 2
    dbapi_hook.get_conn.return_value.cursor.return_value._description_index = 0

    def mock_execute(*args, **kwargs):
        # the run method accesses description property directly, and we need to modify it after
        # every execute, to make sure that different descriptions are returned. I could not find easier
        # method with mocking
        dbapi_hook.get_conn.return_value.cursor.return_value.description = modified_descriptions[
            dbapi_hook.get_conn.return_value.cursor.return_value._description_index
        ]
        dbapi_hook.get_conn.return_value.cursor.return_value._description_index += 1

    dbapi_hook.get_conn.return_value.cursor.return_value.execute = mock_execute
    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = cursor_results
    results = dbapi_hook.run(
        sql=sql, handler=fetch_all_handler, return_last=return_last, split_statements=split_statements
    )

    assert dbapi_hook.descriptions == hook_descriptions
    assert dbapi_hook.last_description == hook_descriptions[-1]
    assert results == hook_results

    dbapi_hook.get_conn.return_value.cursor.return_value.close.assert_called()


@pytest.mark.db_test
@pytest.mark.parametrize(
    "empty_statement",
    [
        pytest.param([], id="Empty list"),
        pytest.param("", id="Empty string"),
        pytest.param("\n", id="Only EOL"),
    ],
)
def test_no_query(empty_statement):
    dbapi_hook = DBApiHookForTests()
    dbapi_hook.get_conn.return_value.cursor.rowcount = 0
    with pytest.raises(ValueError) as err:
        dbapi_hook.run(sql=empty_statement)
    assert err.value.args[0] == "List of SQL statements is empty"


@pytest.mark.db_test
def test_make_common_data_structure_hook_has_deprecated_method():
    """If hook implements ``_make_serializable`` warning should be raised on call."""

    class DBApiHookForMakeSerializableTests(DBApiHookForTests):
        def _make_serializable(self, result: Any):
            return result

    hook = DBApiHookForMakeSerializableTests()
    with pytest.warns(AirflowProviderDeprecationWarning, match="`_make_serializable` method is deprecated"):
        hook._make_common_data_structure(["foo", "bar", "baz"])


@pytest.mark.db_test
def test_make_common_data_structure_no_deprecated_method():
    """If hook not implements ``_make_serializable`` there is no warning should be raised on call."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", AirflowProviderDeprecationWarning)
        DBApiHookForTests()._make_common_data_structure(["foo", "bar", "baz"])
