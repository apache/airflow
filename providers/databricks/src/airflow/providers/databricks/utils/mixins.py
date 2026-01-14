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
#
from __future__ import annotations

import time
from logging import Logger
from typing import TYPE_CHECKING, Any, Protocol

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.hooks.databricks import DatabricksHook, SQLStatementState
from airflow.providers.databricks.triggers.databricks import DatabricksSQLStatementExecutionTrigger

if TYPE_CHECKING:
    from airflow.sdk import Context


class GetHookHasFields(Protocol):
    """Protocol for get_hook method."""

    databricks_conn_id: str
    databricks_retry_args: dict | None
    databricks_retry_delay: int
    databricks_retry_limit: int


class HandleExecutionHasFields(Protocol):
    """Protocol for _handle_execution method."""

    _hook: DatabricksHook
    log: Logger
    polling_period_seconds: int
    task_id: str
    timeout: int
    statement_id: str


class HandleDeferrableExecutionHasFields(Protocol):
    """Protocol for _handle_deferrable_execution method."""

    _hook: DatabricksHook
    databricks_conn_id: str
    databricks_retry_args: dict[Any, Any] | None
    databricks_retry_delay: int
    databricks_retry_limit: int
    defer: Any
    log: Logger
    polling_period_seconds: int
    statement_id: str
    task_id: str
    timeout: int


class ExecuteCompleteHasFields(Protocol):
    """Protocol for execute_complete method."""

    statement_id: str
    _hook: DatabricksHook
    log: Logger


class OnKillHasFields(Protocol):
    """Protocol for on_kill method."""

    _hook: DatabricksHook
    log: Logger
    statement_id: str
    task_id: str


class DatabricksSQLStatementsMixin:
    """
    Mixin class to be used by both the DatabricksSqlStatementsOperator, and the DatabricksSqlStatementSensor.

        - _handle_operator_execution (renamed to _handle_execution)
        - _handle_deferrable_operator_execution (renamed to _handle_deferrable_execution)
        - execute_complete
        - on_kill
    """

    def _handle_execution(self: HandleExecutionHasFields) -> None:
        """Execute a SQL statement in non-deferrable mode."""
        # Determine the time at which the Task will timeout. The statement_state is defined here in the event
        # the while-loop is never entered
        end_time = time.time() + self.timeout

        while end_time > time.time():
            statement_state: SQLStatementState = self._hook.get_sql_statement_state(self.statement_id)

            if statement_state.is_terminal:
                if statement_state.is_successful:
                    self.log.info("%s completed successfully.", self.task_id)
                    return

                error_message = (
                    f"{self.task_id} failed with terminal state: {statement_state.state} "
                    f"and with the error code {statement_state.error_code} "
                    f"and error message {statement_state.error_message}"
                )
                raise AirflowException(error_message)

            self.log.info("%s in run state: %s", self.task_id, statement_state.state)
            self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
            time.sleep(self.polling_period_seconds)

        # Once the timeout is exceeded, the query is cancelled. This is an important steps; if a query takes
        # to log, it needs to be killed. Otherwise, it may be the case that there are "zombie" queries running
        # that are no longer being orchestrated
        self._hook.cancel_sql_statement(self.statement_id)
        raise AirflowException(
            f"{self.task_id} timed out after {self.timeout} seconds with state: {statement_state}",
        )

    def _handle_deferrable_execution(
        self: HandleDeferrableExecutionHasFields, defer_method_name: str = "execute_complete"
    ) -> None:
        """Execute a SQL statement in deferrable mode."""
        statement_state: SQLStatementState = self._hook.get_sql_statement_state(self.statement_id)
        end_time: float = time.time() + self.timeout

        if not statement_state.is_terminal:
            # If the query is still running and there is no statement_id, this is somewhat of a "zombie"
            # query, and should throw an exception
            if not self.statement_id:
                raise AirflowException("Failed to retrieve statement_id after submitting SQL statement.")

            self.defer(
                trigger=DatabricksSQLStatementExecutionTrigger(
                    statement_id=self.statement_id,
                    databricks_conn_id=self.databricks_conn_id,
                    end_time=end_time,
                    polling_period_seconds=self.polling_period_seconds,
                    retry_limit=self.databricks_retry_limit,
                    retry_delay=self.databricks_retry_delay,
                    retry_args=self.databricks_retry_args,
                ),
                method_name=defer_method_name,
            )

        else:
            if statement_state.is_successful:
                self.log.info("%s completed successfully.", self.task_id)
            else:
                error_message = (
                    f"{self.task_id} failed with terminal state: {statement_state.state} "
                    f"and with the error code {statement_state.error_code} "
                    f"and error message {statement_state.error_message}"
                )
                raise AirflowException(error_message)

    def execute_complete(self: ExecuteCompleteHasFields, context: Context, event: dict):
        statement_state = SQLStatementState.from_json(event["state"])
        error = event["error"]
        # Save as instance attribute again after coming back from defer (e.g., for later use in listeners)
        self.statement_id = event["statement_id"]

        if statement_state.is_successful:
            self.log.info("SQL Statement with ID %s completed successfully.", self.statement_id)
            return

        error_message = f"SQL Statement execution failed with terminal state: {statement_state} and with the error {error}"
        raise AirflowException(error_message)

    def on_kill(self: OnKillHasFields) -> None:
        if self.statement_id:
            self._hook.cancel_sql_statement(self.statement_id)
            self.log.info(
                "Task: %s with statement ID: %s was requested to be cancelled.",
                self.task_id,
                self.statement_id,
            )
        else:
            self.log.error(
                "Error: Task: %s with invalid statement_id was requested to be cancelled.", self.task_id
            )
