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

import warnings
from collections import namedtuple
from contextlib import closing
from copy import copy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    Mapping,
    Sequence,
    TypeVar,
    cast,
    overload,
)

from databricks import sql  # type: ignore[attr-defined]

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook, return_single_query_results
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from databricks.sql.types import Row

LIST_SQL_ENDPOINTS_ENDPOINT = ("GET", "api/2.0/sql/endpoints")


T = TypeVar("T")


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
    :param return_tuple: Return a ``namedtuple`` object instead of a ``databricks.sql.Row`` object. Default
        to False. In a future release of the provider, this will become True by default. This parameter
        ensures backward-compatibility during the transition phase to common tuple objects for all hooks based
        on DbApiHook. This flag will also be removed in a future release.
    :param kwargs: Additional parameters internal to Databricks SQL Connector parameters
    """

    hook_name = "Databricks SQL"
    _test_connection_sql = "select 42"

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
        return_tuple: bool = False,
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
        self.return_tuple = return_tuple
        self.additional_params = kwargs

        if not self.return_tuple:
            warnings.warn(
                """Returning a raw `databricks.sql.Row` object is deprecated. A namedtuple will be
                returned instead in a future release of the databricks provider. Set `return_tuple=True` to
                enable this behavior.""",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )

    def _get_extra_config(self) -> dict[str, Any | None]:
        extra_params = copy(self.databricks_conn.extra_dejson)
        for arg in ["http_path", "session_configuration", *self.extra_parameters]:
            if arg in extra_params:
                del extra_params[arg]

        return extra_params

    def _get_sql_endpoint_by_name(self, endpoint_name) -> dict[str, Any]:
        result = self._do_api_call(LIST_SQL_ENDPOINTS_ENDPOINT)
        if "endpoints" not in result:
            raise AirflowException("Can't list Databricks SQL endpoints")
        try:
            endpoint = next(
                endpoint
                for endpoint in result["endpoints"]
                if endpoint["name"] == endpoint_name
            )
        except StopIteration:
            raise AirflowException(
                f"Can't find Databricks SQL endpoint with name '{endpoint_name}'"
            )
        else:
            return endpoint

    def get_conn(self) -> Connection:
        """Return a Databricks SQL connection object."""
        if not self._http_path:
            if self._sql_endpoint_name:
                endpoint = self._get_sql_endpoint_by_name(self._sql_endpoint_name)
                self._http_path = endpoint["odbc_params"]["path"]
            elif "http_path" in self.databricks_conn.extra_dejson:
                self._http_path = self.databricks_conn.extra_dejson["http_path"]
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
            self.session_config = self.databricks_conn.extra_dejson.get(
                "session_configuration"
            )

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

    @overload  # type: ignore[override]
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: None = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> None: ...

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: Callable[[Any], T] = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None: ...

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = True,
        return_last: bool = True,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None:
        """
        Run a command or a list of commands.

        Pass a list of SQL statements to the SQL parameter to get them to
        execute sequentially.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query. Note that currently there is no commit functionality
            in Databricks SQL so this flag has no effect.

        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        :param split_statements: Whether to split a single SQL string into statements and run separately
        :param return_last: Whether to return result for only last statement or for all after split
        :return: return only result of the LAST SQL expression if handler was provided unless return_last
            is set to False.
        """
        self.descriptions = []
        if isinstance(sql, str):
            if split_statements:
                sql_list = [self.strip_sql_string(s) for s in self.split_sql_string(sql)]
            else:
                sql_list = [self.strip_sql_string(sql)]
        else:
            sql_list = [self.strip_sql_string(s) for s in sql]

        if sql_list:
            self.log.debug(
                "Executing following statements against Databricks DB: %s", sql_list
            )
        else:
            raise ValueError("List of SQL statements is empty")

        conn = None
        results = []
        for sql_statement in sql_list:
            # when using AAD tokens, it could expire if previous query run longer than token lifetime
            conn = self.get_conn()
            with closing(conn.cursor()) as cur:
                self.set_autocommit(conn, autocommit)

                with closing(conn.cursor()) as cur:
                    self._run_command(cur, sql_statement, parameters)  # type: ignore[attr-defined]
                    if handler is not None:
                        raw_result = handler(cur)
                        if self.return_tuple:
                            result = self._make_common_data_structure(raw_result)
                        else:
                            # Returning raw result is deprecated, and do not comply with current common.sql interface
                            result = raw_result  # type: ignore[assignment]
                        if return_single_query_results(
                            sql, return_last, split_statements
                        ):
                            results = [result]
                            self.descriptions = [cur.description]
                        else:
                            results.append(result)
                            self.descriptions.append(cur.description)
        if conn:
            conn.close()
            self._sql_conn = None

        if handler is None:
            return None
        if return_single_query_results(sql, return_last, split_statements):
            return results[-1]
        else:
            return results

    def _make_common_data_structure(
        self, result: Sequence[Row] | Row
    ) -> list[tuple] | tuple:
        """Transform the databricks Row objects into namedtuple."""
        # Below ignored lines respect namedtuple docstring, but mypy do not support dynamically
        # instantiated namedtuple, and will never do: https://github.com/python/mypy/issues/848
        if isinstance(result, list):
            rows: list[Row] = result
            if not rows:
                return []
            rows_fields = tuple(rows[0].__fields__)
            rows_object = namedtuple("Row", rows_fields, rename=True)  # type: ignore
            return cast(List[tuple], [rows_object(*row) for row in rows])
        else:
            row: Row = result
            row_fields = tuple(row.__fields__)
            row_object = namedtuple("Row", row_fields, rename=True)  # type: ignore
            return cast(tuple, row_object(*row))

    def bulk_dump(self, table, tmp_file):
        raise NotImplementedError()

    def bulk_load(self, table, tmp_file):
        raise NotImplementedError()
