#
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

from collections.abc import Sequence
from typing import Any, NamedTuple
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import TaskDeferred
from airflow.models import Connection
from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SQLJobFacet,
)
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.operators import read_only_guard
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.triggers.sql import SQLExecuteQueryTrigger
from airflow.providers.openlineage.extractors.base import OperatorLineage

DATE = "2017-04-20"
TASK_ID = "sql-operator"


class Row(NamedTuple):
    id: str
    value: str


class Row2(NamedTuple):
    id2: str
    value2: str


@pytest.mark.parametrize(
    ("sql", "return_last", "split_statement", "hook_results", "hook_descriptions", "expected_results"),
    [
        pytest.param(
            "select * from dummy",
            True,
            True,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            id="Scalar: Single SQL statement, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy;select * from dummy2",
            True,
            True,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            id="Scalar: Multiple SQL statements, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy",
            False,
            False,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            id="Scalar: Single SQL statements, no return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            "select * from dummy",
            True,
            False,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            id="Scalar: Single SQL statements, return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy"],
            False,
            False,
            [[Row(id="1", value="value1"), Row(id="2", value="value2")]],
            [[("id",), ("value",)]],
            [[Row(id="1", value="value1"), Row(id="2", value="value2")]],
            id="Non-Scalar: Single SQL statements in list, no return_last, no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            False,
            False,
            [
                [Row(id="1", value="value1"), Row(id="2", value="value2")],
                [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                [Row(id="1", value="value1"), Row(id="2", value="value2")],
                [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")],
            ],
            id="Non-Scalar: Multiple SQL statements in list, no return_last (no matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            True,
            False,
            [
                [Row(id="1", value="value1"), Row(id="2", value="value2")],
                [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                [Row(id="1", value="value1"), Row(id="2", value="value2")],
                [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")],
            ],
            id="Non-Scalar: Multiple SQL statements in list, return_last (no matter), no split statement",
        ),
    ],
)
def test_exec_success(sql, return_last, split_statement, hook_results, hook_descriptions, expected_results):
    """
    Test the execute function in case where SQL query was successful.
    """

    class SQLExecuteQueryOperatorForTest(SQLExecuteQueryOperator):
        _mock_db_api_hook = MagicMock()

        def get_db_hook(self):
            return self._mock_db_api_hook

    op = SQLExecuteQueryOperatorForTest(
        task_id=TASK_ID,
        sql=sql,
        do_xcom_push=True,
        return_last=return_last,
        split_statements=split_statement,
    )

    op._mock_db_api_hook.run.return_value = hook_results
    op._mock_db_api_hook.descriptions = hook_descriptions

    execute_results = op.execute(None)

    assert execute_results == expected_results
    op._mock_db_api_hook.run.assert_called_once_with(
        sql=sql,
        parameters=None,
        handler=fetch_all_handler,
        autocommit=False,
        return_last=return_last,
        split_statements=split_statement,
    )


@pytest.mark.parametrize(
    ("sql", "return_last", "split_statement", "hook_results", "hook_descriptions", "expected_results"),
    [
        pytest.param(
            "select * from dummy",
            True,
            True,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id="1", value="value1"), Row(id="2", value="value2")]),
            id="Scalar: Single SQL statement, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy;select * from dummy2",
            True,
            True,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id="1", value="value1"), Row(id="2", value="value2")]),
            id="Scalar: Multiple SQL statements, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy",
            False,
            False,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id="1", value="value1"), Row(id="2", value="value2")]),
            id="Scalar: Single SQL statements, no return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            "select * from dummy",
            True,
            False,
            [Row(id="1", value="value1"), Row(id="2", value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id="1", value="value1"), Row(id="2", value="value2")]),
            id="Scalar: Single SQL statements, return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy"],
            False,
            False,
            [[Row(id="1", value="value1"), Row(id="2", value="value2")]],
            [[("id",), ("value",)]],
            [([("id",), ("value",)], [Row(id="1", value="value1"), Row(id="2", value="value2")])],
            id="Non-Scalar: Single SQL statements in list, no return_last, no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            False,
            False,
            [
                [Row(id="1", value="value1"), Row(id="2", value="value2")],
                [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([("id",), ("value",)], [Row(id="1", value="value1"), Row(id="2", value="value2")]),
                ([("id2",), ("value2",)], [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")]),
            ],
            id="Non-Scalar: Multiple SQL statements in list, no return_last (no matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            True,
            False,
            [
                [Row(id="1", value="value1"), Row(id="2", value="value2")],
                [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([("id",), ("value",)], [Row(id="1", value="value1"), Row(id="2", value="value2")]),
                ([("id2",), ("value2",)], [Row2(id2="1", value2="value1"), Row2(id2="2", value2="value2")]),
            ],
            id="Non-Scalar: Multiple SQL statements in list, return_last (no matter), no split statement",
        ),
    ],
)
def test_exec_success_with_process_output(
    sql, return_last, split_statement, hook_results, hook_descriptions, expected_results
):
    """
    Test the execute function in case where SQL query was successful.
    """

    class SQLExecuteQueryOperatorForTestWithProcessOutput(SQLExecuteQueryOperator):
        _mock_db_api_hook = MagicMock()

        def get_db_hook(self):
            return self._mock_db_api_hook

        def _process_output(
            self, results: list[Any], descriptions: list[Sequence[Sequence] | None]
        ) -> list[Any]:
            return list(zip(descriptions, results))

    op = SQLExecuteQueryOperatorForTestWithProcessOutput(
        task_id=TASK_ID,
        sql=sql,
        do_xcom_push=True,
        return_last=return_last,
        split_statements=split_statement,
    )

    op._mock_db_api_hook.run.return_value = hook_results
    op._mock_db_api_hook.descriptions = hook_descriptions

    execute_results = op.execute(None)

    assert execute_results == expected_results
    op._mock_db_api_hook.run.assert_called_once_with(
        sql=sql,
        parameters=None,
        handler=fetch_all_handler,
        autocommit=False,
        return_last=return_last,
        split_statements=split_statement,
    )


@pytest.mark.parametrize(
    ("connection_port", "default_port", "expected_port"),
    [(None, 4321, 4321), (1234, None, 1234), (1234, 4321, 1234)],
)
def test_execute_openlineage_events(connection_port, default_port, expected_port):
    class DBApiHookForTests(DbApiHook):
        conn_name_attr = "sql_default"
        get_conn = MagicMock(name="conn")
        get_connection = MagicMock()

        def get_openlineage_database_info(self, connection):
            from airflow.providers.openlineage.sqlparser import DatabaseInfo

            return DatabaseInfo(
                scheme="sqlscheme",
                authority=DbApiHook.get_openlineage_authority_part(connection, default_port=default_port),
            )

        def get_openlineage_database_specific_lineage(self, task_instance):
            return OperatorLineage(run_facets={"completed": True})

    dbapi_hook = DBApiHookForTests()

    class SQLExecuteQueryOperatorForTest(SQLExecuteQueryOperator):
        def get_db_hook(self):
            return dbapi_hook

    sql = """CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
        order_day_of_week VARCHAR(64) NOT NULL,
        order_placed_on   TIMESTAMP NOT NULL,
        orders_placed     INTEGER NOT NULL
    );
FORGOT TO COMMENT"""
    op = SQLExecuteQueryOperatorForTest(task_id=TASK_ID, sql=sql)
    DB_SCHEMA_NAME = "PUBLIC"
    rows = [
        (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_day_of_week", 1, "varchar"),
        (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_placed_on", 2, "timestamp"),
        (DB_SCHEMA_NAME, "popular_orders_day_of_week", "orders_placed", 3, "int4"),
    ]
    dbapi_hook.get_connection.return_value = Connection(
        conn_id="sql_default", conn_type="postgresql", host="host", port=connection_port
    )
    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    lineage = op.get_openlineage_facets_on_start()
    assert len(lineage.inputs) == 0
    assert lineage.outputs == [
        Dataset(
            namespace=f"sqlscheme://host:{expected_port}",
            name="PUBLIC.popular_orders_day_of_week",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="order_day_of_week", type="varchar"),
                        SchemaDatasetFacetFields(name="order_placed_on", type="timestamp"),
                        SchemaDatasetFacetFields(name="orders_placed", type="int4"),
                    ]
                )
            },
        )
    ]

    assert lineage.job_facets == {"sql": SQLJobFacet(query=sql)}

    assert lineage.run_facets["extractionError"].failedTasks == 1

    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    lineage_on_complete = op.get_openlineage_facets_on_complete(None)
    assert (
        OperatorLineage(
            inputs=lineage.inputs,
            outputs=lineage.outputs,
            run_facets={**lineage.run_facets, **{"completed": True}},
            job_facets=lineage.job_facets,
        )
        == lineage_on_complete
    )


def test_with_no_openlineage_provider():
    import importlib

    def mock__import__(name, globals_=None, locals_=None, fromlist=(), level=0):
        if level == 0 and name.startswith("airflow.providers.openlineage"):
            raise ImportError("No provider 'apache-airflow-providers-openlineage'")
        return importlib.__import__(name, globals=globals_, locals=locals_, fromlist=fromlist, level=level)

    with mock.patch("builtins.__import__", side_effect=mock__import__):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1;")
        assert op.get_openlineage_facets_on_start() is None
        assert op.get_openlineage_facets_on_complete(None) is None


class TestSQLExecuteQueryOperatorDeferrable:
    def test_execute_defers(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True)
        with pytest.raises(TaskDeferred) as exc:
            op.execute({})
        assert isinstance(exc.value.trigger, SQLExecuteQueryTrigger)
        assert exc.value.method_name == "execute_complete"

    def test_execute_complete_raises_on_none_event(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True)
        with pytest.raises(RuntimeError, match="Unknown error in SQLExecuteQueryTrigger"):
            op.execute_complete(context={}, event=None)

    def test_execute_complete_raises_on_error_event(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True)
        with pytest.raises(RuntimeError, match="something went wrong"):
            op.execute_complete(context={}, event={"status": "error", "message": "something went wrong"})

    def test_execute_complete_returns_none_when_no_xcom_push(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True, do_xcom_push=False)
        result = op.execute_complete(context={}, event={"status": "success", "results": [("row1",)]})
        assert result is None

    def test_execute_complete_returns_none_when_no_results(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True, do_xcom_push=True)
        result = op.execute_complete(context={}, event={"status": "success", "results": None})
        assert result is None

    def test_execute_sets_fetch_results_from_output_processing(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True, do_xcom_push=True)
        with pytest.raises(TaskDeferred) as exc:
            op.execute({})
        assert exc.value.trigger.fetch_results is True

    def test_execute_sets_fetch_results_false_without_output_processing(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True, do_xcom_push=False)
        with pytest.raises(TaskDeferred) as exc:
            op.execute({})
        assert exc.value.trigger.fetch_results is False

    def test_execute_complete_applies_default_handler(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True, do_xcom_push=True)
        result = op.execute_complete(
            context={},
            event={
                "status": "success",
                "results": [("a",), ("b",)],
                "descriptions": [[["col", 23, None, None, None, None, None]]],
            },
        )
        assert result == [("a",), ("b",)]

    def test_execute_complete_applies_custom_handler_on_worker(self):
        # The handler runs on the worker over a replayed cursor and can read rows and descriptions.
        def handler(cursor):
            return {"columns": [column[0] for column in cursor.description], "rows": cursor.fetchall()}

        op = SQLExecuteQueryOperator(
            task_id=TASK_ID, sql="SELECT 1", deferrable=True, do_xcom_push=True, handler=handler
        )
        result = op.execute_complete(
            context={},
            event={
                "status": "success",
                "results": [("x",)],
                "descriptions": [[["col", 23, None, None, None, None, None]]],
            },
        )
        assert result == {"columns": ["col"], "rows": [("x",)]}

    def test_execute_complete_applies_handler_per_statement(self):
        op = SQLExecuteQueryOperator(
            task_id=TASK_ID,
            sql=["SELECT 1", "SELECT 2"],
            deferrable=True,
            do_xcom_push=True,
            split_statements=True,
            return_last=False,
            handler=fetch_all_handler,
        )
        description = [["col", 23, None, None, None, None, None]]
        result = op.execute_complete(
            context={},
            event={
                "status": "success",
                "results": [[("a",)], [("b",)]],
                "descriptions": [description, description],
            },
        )
        assert result == [[("a",)], [("b",)]]

    def test_execute_complete_replay_cursor_rejects_live_cursor_features(self):
        def handler(cursor):
            return cursor.connection

        op = SQLExecuteQueryOperator(
            task_id=TASK_ID, sql="SELECT 1", deferrable=True, do_xcom_push=True, handler=handler
        )
        with pytest.raises(AttributeError, match="deferrable=False"):
            op.execute_complete(
                context={},
                event={"status": "success", "results": [("a",)], "descriptions": [None]},
            )

    def test_execute_defers_read_only_by_default(self):
        op = SQLExecuteQueryOperator(task_id=TASK_ID, sql="SELECT 1", deferrable=True)
        with pytest.raises(TaskDeferred) as exc:
            op.execute({})
        assert exc.value.trigger.read_only is True

    def test_execute_passes_enforce_read_only_false_to_trigger(self):
        op = SQLExecuteQueryOperator(
            task_id=TASK_ID, sql="INSERT INTO foo VALUES (1)", deferrable=True, enforce_read_only=False
        )
        with pytest.raises(TaskDeferred) as exc:
            op.execute({})
        assert exc.value.trigger.read_only is False

    def test_execute_raises_before_deferring_on_proven_write(self):
        op = SQLExecuteQueryOperator(
            task_id=TASK_ID, sql="INSERT INTO foo VALUES (1)", deferrable=True, enforce_read_only=True
        )
        with pytest.raises(ValueError, match="enforce_read_only=True but the SQL appears to contain a write"):
            op.execute({})


@pytest.mark.parametrize(
    ("sql", "expected_kind"),
    [
        pytest.param("SELECT * FROM foo", None, id="select"),
        pytest.param("SELECT 1;", None, id="select-trailing-semicolon"),
        pytest.param("INSERT INTO foo VALUES (1)", "INSERT", id="insert"),
        pytest.param("UPDATE foo SET x = 1", "UPDATE", id="update"),
        pytest.param("DELETE FROM foo", "DELETE", id="delete"),
        pytest.param(
            "MERGE INTO foo USING bar ON foo.id = bar.id WHEN MATCHED THEN UPDATE SET x = 1",
            "MERGE",
            id="merge",
        ),
        pytest.param("CREATE TABLE foo (id int)", "CREATE", id="create"),
        pytest.param("ALTER TABLE foo ADD COLUMN y int", "ALTER", id="alter"),
        pytest.param("DROP TABLE foo", "DROP", id="drop"),
        pytest.param("TRUNCATE TABLE foo", "TRUNCATETABLE", id="truncate"),
        pytest.param(
            "WITH cte AS (INSERT INTO foo VALUES (1) RETURNING *) SELECT * FROM cte",
            "INSERT",
            id="write-inside-cte",
        ),
    ],
)
def test_scan_for_writes_detects_write_kind(sql, expected_kind):
    is_write, reason = read_only_guard.scan_for_writes(sql)
    if expected_kind is None:
        assert is_write is False
        assert "no write detected" in reason
    else:
        assert is_write is True
        assert f"proven write ({expected_kind})" in reason


def test_scan_for_writes_detects_write_in_second_of_multiple_statements():
    is_write, reason = read_only_guard.scan_for_writes("SELECT 1; INSERT INTO foo VALUES (1)")
    assert is_write is True
    assert "statement #2" in reason
    assert "INSERT" in reason


def test_scan_for_writes_list_of_read_only_statements():
    is_write, reason = read_only_guard.scan_for_writes(["SELECT 1", "SELECT 2"])
    assert is_write is False
    assert "no write detected" in reason


def test_scan_for_writes_detects_write_across_list_of_statements():
    is_write, reason = read_only_guard.scan_for_writes(["SELECT 1", "INSERT INTO foo VALUES (1)"])
    assert is_write is True
    assert "statement #2" in reason


def test_scan_for_writes_unparseable_sql_defers_to_read_only_transaction():
    is_write, reason = read_only_guard.scan_for_writes("SELECT * FROM foo WHERE x = 'unterminated")
    assert is_write is False
    assert "unparseable" in reason


def test_scan_for_writes_sqlglot_missing_defers_to_read_only_transaction(monkeypatch):
    monkeypatch.setattr(read_only_guard, "_SQLGLOT_AVAILABLE", False)
    is_write, reason = read_only_guard.scan_for_writes("INSERT INTO foo VALUES (1)")
    assert is_write is False
    assert "sqlglot not installed" in reason
