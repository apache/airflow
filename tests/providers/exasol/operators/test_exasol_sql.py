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

from typing import NamedTuple
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.exasol.hooks.exasol import exasol_fetch_all_handler
from airflow.providers.exasol.operators.exasol import ExasolOperator

DATE = "2017-04-20"
TASK_ID = "exasol-sql-operator"
DEFAULT_CONN_ID = "exasol_default"


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
    with patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook") as get_db_hook_mock:
        op = ExasolOperator(
            task_id=TASK_ID,
            sql=sql,
            do_xcom_push=True,
            return_last=return_last,
            split_statements=split_statement,
        )
        dbapi_hook = MagicMock()
        get_db_hook_mock.return_value = dbapi_hook
        dbapi_hook.run.return_value = hook_results
        dbapi_hook.descriptions = hook_descriptions

        execute_results = op.execute(None)

        assert execute_results == expected_results
        dbapi_hook.run.assert_called_once_with(
            sql=sql,
            parameters=None,
            handler=exasol_fetch_all_handler,
            autocommit=False,
            return_last=return_last,
            split_statements=split_statement,
        )
