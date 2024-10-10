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

from typing import Any, NamedTuple, Sequence
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SQLJobFacet,
)
from airflow.providers.common.sql.hooks.sql import DbApiHook, fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage

from dev.tests_common.test_utils.compat import AIRFLOW_V_2_8_PLUS

pytestmark = [
    pytest.mark.skipif(not AIRFLOW_V_2_8_PLUS, reason="Tests for Airflow 2.8.0+ only"),
]

DATE = "2017-04-20"
TASK_ID = "sql-operator"


class Row(NamedTuple):
    id: str
    value: str


class Row2(NamedTuple):
    id2: str
    value2: str


@pytest.mark.parametrize(
    "sql, return_last, split_statement, hook_results, hook_descriptions, expected_results",
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
    "sql, return_last, split_statement, hook_results, hook_descriptions, expected_results",
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
    "connection_port, default_port, expected_port",
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
