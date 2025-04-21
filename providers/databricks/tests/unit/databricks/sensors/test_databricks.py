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

from unittest import mock

from airflow.providers.databricks.sensors.databricks import DatabricksSQLStatementsSensor

DEFAULT_CONN_ID = "databricks_default"
STATEMENT = "select * from test.test;"
STATEMENT_ID = "statement_id"
TASK_ID = "task_id"
WAREHOUSE_ID = "warehouse_id"


class TestDatabricksSQLStatementsSensor:
    """
    Validate and test the functionality of the DatabricksSQLStatementsSensor. This Sensor borrows heavily
    from the DatabricksSQLStatementOperator, meaning that much of the testing logic is also reused.

    There are a handful of additional tests that will be named/added:
        - test_exec_success -> test_exec_success_statement
        - test_exec_success_statement_id
        - test_exec_failure -> test_exec_failure_statement
        - test_exec_failure_statement_id
    """

    def test_init_statement(self):
        """Test initialization for traditional use-case (statement)."""
        op = DatabricksSQLStatementsSensor(task_id=TASK_ID, statement=STATEMENT, warehouse_id=WAREHOUSE_ID)

        assert op.statement == STATEMENT
        assert op.warehouse_id == WAREHOUSE_ID

    def test_init_statement_id(self):
        """Test initialization when a statement_id is passed, rather than a statement."""
        op = DatabricksSQLStatementsSensor(
            task_id=TASK_ID, statement_id=STATEMENT_ID, warehouse_id=WAREHOUSE_ID
        )

        assert op.statement_id == STATEMENT_ID
        assert op.warehouse_id == WAREHOUSE_ID

    @mock.patch("airflow.providers.databricks.sensors.databricks.DatabricksHook")
    def test_exec_success_statement(self, db_mock_class):
        """Test the execute function for non-deferrable execution."""
        expected_json = {
            "statement": STATEMENT,
            "warehouse_id": WAREHOUSE_ID,
            "catalog": None,
            "schema": None,
            "parameters": None,
            "wait_timeout": "0s",
        }

        op = DatabricksSQLStatementsSensor(
            task_id=TASK_ID, statement_id=STATEMENT_ID, warehouse_id=WAREHOUSE_ID
        )
        db_mock = db_mock_class.return_value
        db_mock.get_statement.return_value = STATEMENT_ID

        op.execute(None)  # No context is being passed in

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
            caller="DatabricksSQLStatementsSensor",
        )

        # Since a statement is being passed in rather than a statement_id, we're asserting that the
        # post_sql_statement method is called once
        db_mock.post_sql_statement.assert_called_once_with(expected_json)
        db_mock.get_sql_statement_state.assert_called_once_with(STATEMENT_ID)
        assert op.statement_id == STATEMENT_ID
