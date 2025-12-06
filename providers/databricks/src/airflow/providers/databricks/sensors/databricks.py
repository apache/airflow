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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook, SQLStatementState
from airflow.providers.databricks.operators.databricks import DEFER_METHOD_NAME
from airflow.providers.databricks.utils.mixins import DatabricksSQLStatementsMixin

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

XCOM_STATEMENT_ID_KEY = "statement_id"


class DatabricksSQLStatementsSensor(DatabricksSQLStatementsMixin, BaseSensorOperator):
    """DatabricksSQLStatementsSensor."""

    template_fields: Sequence[str] = (
        "databricks_conn_id",
        "statement",
        "statement_id",
    )
    template_ext: Sequence[str] = (".json-tpl",)
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        warehouse_id: str,
        *,
        statement: str | None = None,
        statement_id: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        databricks_conn_id: str = "databricks_default",
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        databricks_retry_args: dict[Any, Any] | None = None,
        do_xcom_push: bool = True,
        wait_for_termination: bool = True,
        timeout: float = 3600,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        # Handle the scenario where either both statement and statement_id are set/not set
        if statement and statement_id:
            raise AirflowException("Cannot provide both statement and statement_id.")
        if not statement and not statement_id:
            raise AirflowException("One of either statement or statement_id must be provided.")

        if not warehouse_id:
            raise AirflowException("warehouse_id must be provided.")

        super().__init__(**kwargs)

        self.statement = statement
        self.statement_id = statement_id
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.parameters = parameters
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args
        self.wait_for_termination = wait_for_termination
        self.deferrable = deferrable
        self.timeout = timeout
        self.do_xcom_push = do_xcom_push

    @cached_property
    def _hook(self):
        return self._get_hook(caller="DatabricksSQLStatementsSensor")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: Context):
        if not self.statement_id:
            # Otherwise, we'll go ahead and "submit" the statement
            json = {
                "statement": self.statement,
                "warehouse_id": self.warehouse_id,
                "catalog": self.catalog,
                "schema": self.schema,
                "parameters": self.parameters,
                "wait_timeout": "0s",
            }

            self.statement_id = self._hook.post_sql_statement(json)
            self.log.info("SQL Statement submitted with statement_id: %s", self.statement_id)

        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key=XCOM_STATEMENT_ID_KEY, value=self.statement_id)

        # If we're not waiting for the query to complete execution, then we'll go ahead and return. However, a
        # recommendation to use the DatabricksSQLStatementOperator is made in this case
        if not self.wait_for_termination:
            self.log.info(
                "If setting wait_for_termination = False, consider using the DatabricksSQLStatementsOperator instead."
            )
            return

        if self.deferrable:
            self._handle_deferrable_execution(defer_method_name=DEFER_METHOD_NAME)  # type: ignore[misc]

    def poke(self, context: Context):
        """
        Handle non-deferrable Sensor execution.

        :param context: (Context)
        :return: (bool)
        """
        # This is going to very closely mirror the execute_complete
        statement_state: SQLStatementState = self._hook.get_sql_statement_state(self.statement_id)

        if statement_state.is_running:
            self.log.info("SQL Statement with ID %s is running", self.statement_id)
            return False
        if statement_state.is_successful:
            self.log.info("SQL Statement with ID %s completed successfully.", self.statement_id)
            return True
        raise AirflowException(
            f"SQL Statement with ID {statement_state} failed with error: {statement_state.error_message}"
        )
