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

import time
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator, conf
from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook
from airflow.providers.snowflake.triggers.snowflake_trigger import SnowflakeSqlApiTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SnowflakeNotebookOperator(BaseOperator):
    """
    Execute a Snowflake Notebook via the Snowflake SQL API.

    Submits an ``EXECUTE NOTEBOOK`` statement through the Snowflake SQL API,
    polls for completion, and pushes the statement query id to XCom.
    Supports both synchronous polling and deferrable (async) mode for
    long-running notebook executions.

    The operator supports the following authentication methods via the Snowflake connection:

    - **Key pair**: provide ``private_key_file`` or ``private_key_content`` in the connection extras.
    - **OAuth**: provide ``refresh_token``, ``client_id``, and ``client_secret`` in the connection extras.
    - **Programmatic Access Token (PAT)**: set ``authenticator`` to ``programmatic_access_token`` in
      the connection extras and put the PAT value in the connection ``password`` field.

    .. seealso::
        `Snowflake EXECUTE NOTEBOOK
        <https://docs.snowflake.com/en/sql-reference/sql/execute-notebook>`_

    :param notebook: Fully-qualified notebook name
        (e.g. ``MY_DB.MY_SCHEMA.MY_NOTEBOOK``).
    :param parameters: Optional list of parameter strings to pass to the
        notebook.  Only string values are supported by Snowflake; other
        data types are interpreted as NULL.  Parameters are accessible in
        the notebook via ``sys.argv``.
    :param snowflake_conn_id: Reference to the Snowflake connection.
    :param warehouse: Snowflake warehouse name (overrides connection default).
    :param database: Snowflake database name (overrides connection default).
    :param schema: Snowflake schema name (overrides connection default).
    :param role: Snowflake role name (overrides connection default).
    :param authenticator: Snowflake authenticator type.
    :param session_parameters: Snowflake session-level parameters.
    :param poll_interval: Seconds between status checks (default 5).
    :param token_life_time: Lifetime of the JWT token.
    :param token_renewal_delta: When to renew the JWT token before expiry.
    :param deferrable: If True, run in deferrable mode (frees the worker
        slot while waiting).  Defaults to the ``operators.default_deferrable``
        Airflow config.
    :param snowflake_api_retry_args: Optional dict of arguments forwarded to
        ``tenacity.Retrying`` for API call retries.
    """

    LIFETIME = timedelta(minutes=59)
    RENEWAL_DELTA = timedelta(minutes=54)

    template_fields: Sequence[str] = (
        "notebook",
        "parameters",
        "snowflake_conn_id",
    )

    ui_color = "#ededed"

    def __init__(
        self,
        *,
        notebook: str,
        parameters: list[str] | None = None,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict[str, Any] | None = None,
        poll_interval: int = 5,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        snowflake_api_retry_args: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook = notebook
        self.parameters = parameters
        self.snowflake_conn_id = snowflake_conn_id
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.poll_interval = poll_interval
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        self.deferrable = deferrable
        self.snowflake_api_retry_args = snowflake_api_retry_args or {}
        self.query_ids: list[str] = []

    @cached_property
    def _hook(self) -> SnowflakeSqlApiHook:
        hook_kwargs: dict[str, Any] = {}
        if any(
            [
                self.warehouse,
                self.database,
                self.role,
                self.schema,
                self.authenticator,
                self.session_parameters,
            ]
        ):
            hook_kwargs = {
                "warehouse": self.warehouse,
                "database": self.database,
                "role": self.role,
                "schema": self.schema,
                "authenticator": self.authenticator,
                "session_parameters": self.session_parameters,
            }
        return SnowflakeSqlApiHook(
            snowflake_conn_id=self.snowflake_conn_id,
            token_life_time=self.token_life_time,
            token_renewal_delta=self.token_renewal_delta,
            deferrable=self.deferrable,
            api_retry_args=self.snowflake_api_retry_args,
            **hook_kwargs,
        )

    def _build_execute_notebook_query(self) -> str:
        """Build the ``EXECUTE NOTEBOOK`` SQL statement."""
        params_clause = ""
        if self.parameters:
            escaped = ", ".join(f"'{p.replace(chr(39), chr(39) + chr(39))}'" for p in self.parameters)
            params_clause = escaped
        return f"EXECUTE NOTEBOOK {self.notebook}({params_clause})"

    def execute(self, context: Context) -> None:
        """Submit the notebook execution and wait for completion."""
        sql = self._build_execute_notebook_query()
        self.log.info("Executing Snowflake Notebook: %s", sql)

        self.query_ids = self._hook.execute_query(sql, statement_count=1)
        self.log.info("Notebook submitted with query ids: %s", self.query_ids)

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        succeeded_query_ids = []
        for query_id in self.query_ids:
            self.log.info("Retrieving status for query id %s", query_id)
            statement_status = self._hook.get_sql_api_query_status(query_id)
            if statement_status.get("status") == "running":
                break
            if statement_status.get("status") == "success":
                succeeded_query_ids.append(query_id)
            else:
                raise AirflowException(f"{statement_status.get('status')}: {statement_status.get('message')}")

        if len(self.query_ids) == len(succeeded_query_ids):
            self.log.info("Notebook %s completed successfully.", self.notebook)
            return

        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=SnowflakeSqlApiTrigger(
                    poll_interval=self.poll_interval,
                    query_ids=self.query_ids,
                    snowflake_conn_id=self.snowflake_conn_id,
                    token_life_time=self.token_life_time,
                    token_renewal_delta=self.token_renewal_delta,
                ),
                method_name="execute_complete",
            )
        else:
            self._poll_until_complete()
            self.log.info("Notebook %s completed successfully.", self.notebook)

    def _poll_until_complete(self) -> None:
        """Poll query status until all queries reach a terminal state."""
        while True:
            all_done = True
            for query_id in self.query_ids:
                statement_status = self._hook.get_sql_api_query_status(query_id)
                if statement_status.get("status") == "error":
                    raise AirflowException(
                        f"{statement_status.get('status')}: {statement_status.get('message')}"
                    )
                if statement_status.get("status") == "running":
                    all_done = False
            if all_done:
                break
            time.sleep(self.poll_interval)

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> None:
        """Handle the event fired by the trigger on completion."""
        if event:
            if event.get("status") == "error":
                raise AirflowException(f"{event['status']}: {event.get('message')}")
            if event.get("status") == "success":
                self.query_ids = list(event.get("statement_query_ids", []))
                self.log.info("Notebook %s completed successfully.", self.notebook)
                return
        self.log.info("Notebook %s completed successfully.", self.notebook)

    def on_kill(self) -> None:
        """Cancel the running notebook execution."""
        if self.query_ids:
            self.log.info("Cancelling notebook query ids: %s", self.query_ids)
            self._hook.cancel_queries(self.query_ids)
            self.log.info("Notebook query ids %s cancelled.", self.query_ids)
