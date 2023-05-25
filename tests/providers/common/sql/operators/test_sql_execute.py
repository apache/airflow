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
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            id="Scalar: Single SQL statement, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy;select * from dummy2",
            True,
            True,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            id="Scalar: Multiple SQL statements, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy",
            False,
            False,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            id="Scalar: Single SQL statements, no return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            "select * from dummy",
            True,
            False,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            id="Scalar: Single SQL statements, return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy"],
            False,
            False,
            [[Row(id=1, value="value1"), Row(id=2, value="value2")]],
            [[("id",), ("value",)]],
            [[Row(id=1, value="value1"), Row(id=2, value="value2")]],
            id="Non-Scalar: Single SQL statements in list, no return_last, no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            False,
            False,
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")],
            ],
            id="Non-Scalar: Multiple SQL statements in list, no return_last (no matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            True,
            False,
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")],
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
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Single SQL statement, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy;select * from dummy2",
            True,
            True,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Multiple SQL statements, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy",
            False,
            False,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Single SQL statements, no return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            "select * from dummy",
            True,
            False,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Single SQL statements, return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy"],
            False,
            False,
            [[Row(id=1, value="value1"), Row(id=2, value="value2")]],
            [[("id",), ("value",)]],
            [([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")])],
            id="Non-Scalar: Single SQL statements in list, no return_last, no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            False,
            False,
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
                ([("id2",), ("value2",)], [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")]),
            ],
            id="Non-Scalar: Multiple SQL statements in list, no return_last (no matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            True,
            False,
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
                ([("id2",), ("value2",)], [Row2(id2=1, value2="value1"), Row2(id2=2, value2="value2")]),
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
