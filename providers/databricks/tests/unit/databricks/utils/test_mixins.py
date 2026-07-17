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

from unittest.mock import MagicMock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.utils.mixins import DatabricksSQLStatementsMixin


class DatabricksSQLStatements(DatabricksSQLStatementsMixin):
    def __init__(self):
        self.databricks_conn_id = "databricks_conn_id"
        self.databricks_retry_limit = 3
        self.databricks_retry_delay = 60
        self.databricks_retry_args = None
        self.polling_period_seconds = 10
        self.statement_id = "statement_id"
        self.task_id = "task_id"
        self.timeout = 60

        # Utilities
        self._hook = MagicMock()
        self.defer = MagicMock()
        self.log = MagicMock()


@pytest.fixture
def databricks_sql_statements():
    return DatabricksSQLStatements()


@pytest.fixture
def terminal_success_state():
    terminal_success_state = MagicMock()
    terminal_success_state.is_terminal = True
    terminal_success_state.is_successful = True
    return terminal_success_state


@pytest.fixture
def terminal_failure_state():
    terminal_fail_state = MagicMock()
    terminal_fail_state.is_terminal = True
    terminal_fail_state.is_successful = False
    terminal_fail_state.state = "FAILED"
    terminal_fail_state.error_code = "123"
    terminal_fail_state.error_message = "Query failed"
    return terminal_fail_state


class TestDatabricksSQLStatementsMixin:
    """
    We'll provide tests for each of the following methods:

    - _handle_execution
    - _handle_deferrable_execution
    - execute_complete
    - on_kill
    """

    def test_handle_execution_success(self, databricks_sql_statements, terminal_success_state):
        # Test an immediate success of the SQL statement
        databricks_sql_statements._hook.get_sql_statement_state.return_value = terminal_success_state
        databricks_sql_statements._handle_execution()

        databricks_sql_statements._hook.cancel_sql_statement.assert_not_called()

    def test_handle_execution_failure(self, databricks_sql_statements, terminal_failure_state):
        # Test an immediate failure of the SQL statement
        databricks_sql_statements._hook.get_sql_statement_state.return_value = terminal_failure_state

        with pytest.raises(AirflowException):
            databricks_sql_statements._handle_execution()

        databricks_sql_statements._hook.cancel_sql_statement.assert_not_called()

    def test_handle_deferrable_execution_running(self, databricks_sql_statements):
        terminal_running_state = MagicMock()
        terminal_running_state.is_terminal = False

        # Test an immediate success of the SQL statement
        databricks_sql_statements._hook.get_sql_statement_state.return_value = terminal_running_state
        databricks_sql_statements._handle_deferrable_execution()

        databricks_sql_statements.defer.assert_called_once()

    def test_handle_deferrable_execution_success(self, databricks_sql_statements, terminal_success_state):
        # Test an immediate success of the SQL statement
        databricks_sql_statements._hook.get_sql_statement_state.return_value = terminal_success_state
        databricks_sql_statements._handle_deferrable_execution()

        databricks_sql_statements.defer.assert_not_called()

    def test_handle_deferrable_execution_failure(self, databricks_sql_statements, terminal_failure_state):
        # Test an immediate failure of the SQL statement
        databricks_sql_statements._hook.get_sql_statement_state.return_value = terminal_failure_state

        with pytest.raises(AirflowException):
            databricks_sql_statements._handle_deferrable_execution()

    def test_execute_complete(self):
        # Both the TestDatabricksSQLStatementsOperator and TestDatabricksSQLStatementsSensor tests implement
        # a test_execute_complete_failure and test_execute_complete_success method, so we'll pass here
        pass

    def test_on_kill(self):
        # This test is implemented in both the TestDatabricksSQLStatementsOperator and
        # TestDatabricksSQLStatementsSensor tests, so it will not be implemented here
        pass
