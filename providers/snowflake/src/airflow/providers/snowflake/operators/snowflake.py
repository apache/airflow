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
from collections.abc import Iterable, Mapping, Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, SupportsAbs, cast

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
    SQLIntervalCheckOperator,
    SQLValueCheckOperator,
)
from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook
from airflow.providers.snowflake.triggers.snowflake_trigger import SnowflakeSqlApiTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SnowflakeCheckOperator(SQLCheckOperator):
    """
    Perform a check against Snowflake.

    The ``SnowflakeCheckOperator`` expects a sql query that will return a single row. Each
    value on that first row is evaluated using python ``bool`` casting. If any of the values
    return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    """

    template_fields: Sequence[str] = tuple(set(SQLCheckOperator.template_fields) | {"snowflake_conn_id"})
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"
    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        sql: str,
        snowflake_conn_id: str = "snowflake_default",
        parameters: Iterable | Mapping[str, Any] | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        if any([warehouse, database, role, schema, authenticator, session_parameters]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(sql=sql, parameters=parameters, conn_id=snowflake_conn_id, **kwargs)
        self.query_ids: list[str] = []


class SnowflakeValueCheckOperator(SQLValueCheckOperator):
    """
    Performs a simple check using sql code against a specified value, within a certain level of tolerance.

    :param sql: the sql to be executed
    :param pass_value: the value to check against
    :param tolerance: (optional) the tolerance allowed to accept the query as
        passing
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    """

    template_fields: Sequence[str] = tuple(set(SQLValueCheckOperator.template_fields) | {"snowflake_conn_id"})

    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        snowflake_conn_id: str = "snowflake_default",
        parameters: Iterable | Mapping[str, Any] | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        if any([warehouse, database, role, schema, authenticator, session_parameters]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(
            sql=sql,
            pass_value=pass_value,
            tolerance=tolerance,
            conn_id=snowflake_conn_id,
            parameters=parameters,
            **kwargs,
        )
        self.query_ids: list[str] = []


class SnowflakeIntervalCheckOperator(SQLIntervalCheckOperator):
    """
    Checks that the metrics given as SQL expressions are within tolerance of the ones from days_back before.

    This method constructs a query like so ::

        SELECT {metrics_threshold_dict_key} FROM {table}
        WHERE {date_filter_column}=<date>

    :param table: the table name
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
        example 'COUNT(*)': 1.5 would require a 50 percent or less difference
        between the current day, and the prior days_back.
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    """

    template_fields: Sequence[str] = tuple(
        set(SQLIntervalCheckOperator.template_fields) | {"snowflake_conn_id"}
    )
    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: dict,
        date_filter_column: str = "ds",
        days_back: SupportsAbs[int] = -7,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        if any([warehouse, database, role, schema, authenticator, session_parameters]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(
            table=table,
            metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column,
            days_back=days_back,
            conn_id=snowflake_conn_id,
            **kwargs,
        )
        self.query_ids: list[str] = []


class SnowflakeSqlApiOperator(SQLExecuteQueryOperator):
    """
    Implemented Snowflake SQL API Operator to support multiple SQL statements sequentially.

    This is the behavior of the SQLExecuteQueryOperator, the Snowflake SQL API allows submitting
    multiple SQL statements in a single request. It make post request to submit SQL
    statements for execution, poll to check the status of the execution of a statement. Fetch query results
    concurrently.
    This Operator currently uses key pair authentication, so you need to provide private key raw content or
    private key file path in the snowflake connection along with other details

    .. seealso::

        `Snowflake SQL API key pair Authentication <https://docs.snowflake.com/en/developer-guide/sql-api/authenticating.html#label-sql-api-authenticating-key-pair>`_

    Where can this operator fit in?
         - To execute multiple SQL statements in a single request
         - To execute the SQL statement asynchronously and to execute standard queries and most DDL and DML statements
         - To develop custom applications and integrations that perform queries
         - To create provision users and roles, create table, etc.

    The following commands are not supported:
        - The PUT command (in Snowflake SQL)
        - The GET command (in Snowflake SQL)
        - The CALL command with stored procedures that return a table(stored procedures with the RETURNS TABLE clause).

    .. seealso::

        - `Snowflake SQL API <https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#introduction-to-the-sql-api>`_
        - `API Reference <https://docs.snowflake.com/en/developer-guide/sql-api/reference.html#snowflake-sql-api-reference>`_
        - `Limitation on snowflake SQL API <https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#limitations-of-the-sql-api>`_

    :param snowflake_conn_id: Reference to Snowflake connection id
    :param sql: the sql code to be executed. (templated)
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param poll_interval: the interval in seconds to poll the query
    :param statement_count: Number of SQL statement to be executed
    :param token_life_time: lifetime of the JWT Token
    :param token_renewal_delta: Renewal time of the JWT Token
    :param bindings: (Optional) Values of bind variables in the SQL statement.
            When executing the statement, Snowflake replaces placeholders (? and :name) in
            the statement with these specified values.
    :param deferrable: Run operator in the deferrable mode.
    :param snowflake_api_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` & ``tenacity.AsyncRetrying`` classes.
    """

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minutes lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes

    template_fields: Sequence[str] = tuple(
        set(SQLExecuteQueryOperator.template_fields) | {"snowflake_conn_id"}
    )
    conn_id_field = "snowflake_conn_id"

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict[str, Any] | None = None,
        poll_interval: int = 5,
        statement_count: int = 0,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        bindings: dict[str, Any] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        snowflake_api_retry_args: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        self.poll_interval = poll_interval
        self.statement_count = statement_count
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        self.bindings = bindings
        self.execute_async = False
        self.snowflake_api_retry_args = snowflake_api_retry_args or {}
        self.deferrable = deferrable
        self.query_ids: list[str] = []
        if any([warehouse, database, role, schema, authenticator, session_parameters]):  # pragma: no cover
            hook_params = kwargs.pop("hook_params", {})  # pragma: no cover
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(conn_id=snowflake_conn_id, **kwargs)  # pragma: no cover

    @cached_property
    def _hook(self):
        return SnowflakeSqlApiHook(
            snowflake_conn_id=self.snowflake_conn_id,
            token_life_time=self.token_life_time,
            token_renewal_delta=self.token_renewal_delta,
            deferrable=self.deferrable,
            api_retry_args=self.snowflake_api_retry_args,
            **self.hook_params,
        )

    def execute(self, context: Context) -> None:
        """
        Make a POST API request to snowflake by using SnowflakeSQL and execute the query to get the ids.

        By deferring the SnowflakeSqlApiTrigger class passed along with query ids.
        """
        self.log.info("Executing: %s", self.sql)
        self.query_ids = self._hook.execute_query(
            self.sql,
            statement_count=self.statement_count,
            bindings=self.bindings,
        )
        self.log.info("List of query ids %s", self.query_ids)

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
            self.log.info("%s completed successfully.", self.task_id)
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
            while True:
                statement_status = self.poll_on_queries()
                if statement_status["error"]:
                    raise AirflowException(statement_status["error"])
                if not statement_status["running"]:
                    break

            self._hook.check_query_output(self.query_ids)

    def poll_on_queries(self):
        """Poll on requested queries."""
        queries_in_progress = set(self.query_ids)
        statement_success_status = {}
        statement_error_status = {}
        statement_running_status = {}
        for query_id in self.query_ids:
            if not len(queries_in_progress):
                break
            self.log.info("checking : %s", query_id)
            try:
                statement_status = self._hook.get_sql_api_query_status(query_id)
            except Exception as e:
                raise ValueError({"status": "error", "message": str(e)})
            if statement_status.get("status") == "error":
                queries_in_progress.remove(query_id)
                statement_error_status[query_id] = statement_status
            if statement_status.get("status") == "success":
                statement_success_status[query_id] = statement_status
                queries_in_progress.remove(query_id)
            if statement_status.get("status") == "running":
                statement_running_status[query_id] = statement_status
            time.sleep(self.poll_interval)
        return {
            "success": statement_success_status,
            "error": statement_error_status,
            "running": statement_running_status,
        }

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> None:
        """
        Execute callback when the trigger fires; returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = f"{event['status']}: {event['message']}"
                raise AirflowException(msg)
            if "status" in event and event["status"] == "success":
                self.query_ids = cast("list[str]", event["statement_query_ids"])
                self._hook.check_query_output(self.query_ids)
                self.log.info("%s completed successfully.", self.task_id)
                # Re-assign query_ids to hook after coming back from deferral to be consistent for listeners.
                if not self._hook.query_ids:
                    self._hook.query_ids = self.query_ids
        else:
            self.log.info("%s completed successfully.", self.task_id)

    def on_kill(self) -> None:
        """Cancel the running query."""
        if self.query_ids:
            self.log.info("Cancelling the query ids %s", self.query_ids)
            self._hook.cancel_queries(self.query_ids)
            self.log.info("Query ids %s cancelled successfully", self.query_ids)
