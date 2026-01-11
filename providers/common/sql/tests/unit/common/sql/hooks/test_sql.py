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

import inspect
import logging
from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.common.sql.dialects.dialect import Dialect
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook, resolve_dialects

from tests_common.test_utils.common_sql import mock_db_hook
from tests_common.test_utils.providers import get_provider_min_airflow_version

TASK_ID = "sql-operator"
HOST = "host"
DEFAULT_CONN_ID = "sqlite_default"
PASSWORD = "password"


class DBApiHookForTests(DbApiHook):
    conn_name_attr = "conn_id"
    get_conn = MagicMock(name="conn")


@pytest.fixture(autouse=True)
def create_connection(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id=DEFAULT_CONN_ID,
            conn_type="sqlite",
            host=HOST,
            login=None,
            password=PASSWORD,
            extra=None,
        )
    )


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    return [(field,) for field in fields]


index = 0


@pytest.mark.db_test
@pytest.mark.parametrize(
    (
        "return_last",
        "split_statements",
        "sql",
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


class TestDbApiHook:
    def setup_method(self, **kwargs):
        logging.root.disabled = True

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "empty_statement",
        [
            pytest.param([], id="Empty list"),
            pytest.param("", id="Empty string"),
            pytest.param("\n", id="Only EOL"),
        ],
    )
    def test_no_query(self, empty_statement):
        dbapi_hook = mock_db_hook(DbApiHook)
        with pytest.raises(ValueError, match="List of SQL statements is empty"):
            dbapi_hook.run(sql=empty_statement)

    @pytest.mark.db_test
    def test_placeholder_config_from_extra(self):
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"extra": {"placeholder": "?"}})
        assert dbapi_hook.placeholder == "?"

    @pytest.mark.db_test
    def test_placeholder_config_from_extra_when_not_in_default_sql_placeholders(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow.providers.common.sql.hooks.test_sql"):
            dbapi_hook = mock_db_hook(DbApiHook, conn_params={"extra": {"placeholder": "!"}})
            assert dbapi_hook.placeholder == "%s"
            assert (
                "Placeholder '!' defined in Connection 'default_conn_id' is not listed in 'DEFAULT_SQL_PLACEHOLDERS' "
                f"and got ignored. Falling back to the default placeholder '{DbApiHook._placeholder}'."
                in caplog.text
            )

    @pytest.mark.db_test
    def test_placeholder_multiple_times_and_make_sure_connection_is_only_invoked_once(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        for _ in range(10):
            assert dbapi_hook.placeholder == "%s"
        assert dbapi_hook.connection_invocations == 1

    @pytest.mark.db_test
    def test_escape_column_names(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert not dbapi_hook.escape_column_names

    @pytest.mark.db_test
    def test_dialect_name(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert dbapi_hook.dialect_name == "default"

    @pytest.mark.db_test
    def test_dialect(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert isinstance(dbapi_hook.dialect, Dialect)

    @pytest.mark.db_test
    def test_when_provider_min_airflow_version_is_3_0_or_higher_remove_obsolete_code(self):
        """
        Once this test starts failing due to the fact that the minimum Airflow version is now 3.0.0 or higher
        for this provider, you should remove the obsolete code in the get_dialects method of the DbApiHook
        and remove this test.  This test was added to make sure to not forget to remove the fallback code
        for backward compatibility with Airflow 2.8.x which isn't need anymore once this provider depends on
        Airflow 3.0.0 or higher.
        """
        min_airflow_version = get_provider_min_airflow_version("apache-airflow-providers-common-sql")

        # Check if the current Airflow version is 3.0.0 or higher
        if min_airflow_version[0] >= 3:
            method_source = inspect.getsource(resolve_dialects)
            raise AirflowProviderDeprecationWarning(
                f"Check TODO's to remove obsolete code in resolve_dialects method:\n\r\n\r\t\t\t{method_source}"
            )

    @pytest.mark.db_test
    def test_uri(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert dbapi_hook.get_uri() == "//login:password@host:1234/schema"

    @pytest.mark.db_test
    def test_uri_with_schema(self):
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"schema": "other_schema"})
        assert dbapi_hook.get_uri() == "//login:password@host:1234/other_schema"

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("df_type", "expected_type"),
        [
            ("test_default_df_type", pd.DataFrame),
            ("pandas", pd.DataFrame),
            ("polars", pl.DataFrame),
        ],
    )
    def test_get_df_with_df_type(db, df_type, expected_type):
        dbapi_hook = mock_db_hook(DbApiHook)
        if df_type == "test_default_df_type":
            df = dbapi_hook.get_df("SQL")
            assert isinstance(df, pd.DataFrame)
        else:
            df = dbapi_hook.get_df("SQL", df_type=df_type)
            assert isinstance(df, expected_type)
