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

from contextlib import closing
from copy import copy
from typing import Any, Callable, Iterable, Mapping

from databricks import sql  # type: ignore[attr-defined]
from databricks.sql.client import Connection  # type: ignore[attr-defined]

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

LIST_SQL_ENDPOINTS_ENDPOINT = ('GET', 'api/2.0/sql/endpoints')


class DatabricksSqlHook(BaseDatabricksHook, DbApiHook):
    """
    Hook to interact with Databricks SQL.

    :param databricks_conn_id: Reference to the
        :ref:`Databricks connection <howto/connection:databricks>`.
    :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
        If not specified, it should be either specified in the Databricks connection's extra parameters,
        or ``sql_endpoint_name`` must be specified.
    :param sql_endpoint_name: Optional name of Databricks SQL Endpoint. If not specified, ``http_path``
        must be provided as described above.
    :param session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
        If not specified, it could be specified in the Databricks connection's extra parameters.
    :param http_headers: An optional list of (k, v) pairs that will be set as HTTP headers
        on every request
    :param catalog: An optional initial catalog to use. Requires DBR version 9.0+
    :param schema: An optional initial schema to use. Requires DBR version 9.0+
    :param kwargs: Additional parameters internal to Databricks SQL Connector parameters
    """

    hook_name = 'Databricks SQL'

    def __init__(
        self,
        databricks_conn_id: str = BaseDatabricksHook.default_conn_name,
        http_path: str | None = None,
        sql_endpoint_name: str | None = None,
        session_configuration: dict[str, str] | None = None,
        http_headers: list[tuple[str, str]] | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        caller: str = "DatabricksSqlHook",
        **kwargs,
    ) -> None:
        super().__init__(databricks_conn_id, caller=caller)
        self._sql_conn = None
        self._token: str | None = None
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.supports_autocommit = True
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.additional_params = kwargs

    def _get_extra_config(self) -> dict[str, Any | None]:
        extra_params = copy(self.databricks_conn.extra_dejson)
        for arg in ['http_path', 'session_configuration'] + self.extra_parameters:
            if arg in extra_params:
                del extra_params[arg]

        return extra_params

    def _get_sql_endpoint_by_name(self, endpoint_name) -> dict[str, Any]:
        result = self._do_api_call(LIST_SQL_ENDPOINTS_ENDPOINT)
        if 'endpoints' not in result:
            raise AirflowException("Can't list Databricks SQL endpoints")
        lst = [endpoint for endpoint in result['endpoints'] if endpoint['name'] == endpoint_name]
        if len(lst) == 0:
            raise AirflowException(f"Can't f Databricks SQL endpoint with name '{endpoint_name}'")
        return lst[0]

    def get_conn(self) -> Connection:
        """Returns a Databricks SQL connection object"""
        if not self._http_path:
            if self._sql_endpoint_name:
                endpoint = self._get_sql_endpoint_by_name(self._sql_endpoint_name)
                self._http_path = endpoint['odbc_params']['path']
            elif 'http_path' in self.databricks_conn.extra_dejson:
                self._http_path = self.databricks_conn.extra_dejson['http_path']
            else:
                raise AirflowException(
                    "http_path should be provided either explicitly, "
                    "or in extra parameter of Databricks connection, "
                    "or sql_endpoint_name should be specified"
                )

        requires_init = True
        if not self._token:
            self._token = self._get_token(raise_error=True)
        else:
            new_token = self._get_token(raise_error=True)
            if new_token != self._token:
                self._token = new_token
            else:
                requires_init = False

        if not self.session_config:
            self.session_config = self.databricks_conn.extra_dejson.get('session_configuration')

        if not self._sql_conn or requires_init:
            if self._sql_conn:  # close already existing connection
                self._sql_conn.close()
            self._sql_conn = sql.connect(
                self.host,
                self._http_path,
                self._token,
                schema=self.schema,
                catalog=self.catalog,
                session_configuration=self.session_config,
                http_headers=self.http_headers,
                _user_agent_entry=self.user_agent_value,
                **self._get_extra_config(),
                **self.additional_params,
            )
        return self._sql_conn

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping | None = None,
        handler: Callable | None = None,
        split_statements: bool = True,
        return_last: bool = True,
    ) -> tuple[str, Any] | list[tuple[str, Any]] | None:
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        :param split_statements: Whether to split a single SQL string into statements and run separately
        :param return_last: Whether to return result for only last statement or for all after split
        :return: return only result of the LAST SQL expression if handler was provided.
        """
        scalar_return_last = isinstance(sql, str) and return_last
        if isinstance(sql, str):
            if split_statements:
                sql = self.split_sql_string(sql)
            else:
                sql = [self.strip_sql_string(sql)]

        if sql:
            self.log.debug("Executing following statements against Databricks DB: %s", list(sql))
        else:
            raise ValueError("List of SQL statements is empty")

        results = []
        for sql_statement in sql:
            # when using AAD tokens, it could expire if previous query run longer than token lifetime
            with closing(self.get_conn()) as conn:
                self.set_autocommit(conn, autocommit)

                with closing(conn.cursor()) as cur:
                    self._run_command(cur, sql_statement, parameters)

                    if handler is not None:
                        result = handler(cur)
                        schema = cur.description
                        results.append((schema, result))

            self._sql_conn = None

        if handler is None:
            return None
        elif scalar_return_last:
            return results[-1]
        else:
            return results

    def test_connection(self):
        """Test the Databricks SQL connection by running a simple query."""
        try:
            self.run(sql="select 42")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully checked"

    def bulk_dump(self, table, tmp_file):
        raise NotImplementedError()

    def bulk_load(self, table, tmp_file):
        raise NotImplementedError()
