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

import threading
from collections import namedtuple
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import closing
from copy import copy
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
    overload,
)

from databricks import sql
from databricks.sql.types import Row

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.common.sql.hooks.handlers import return_single_query_results
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.databricks.exceptions import DatabricksSqlExecutionError, DatabricksSqlExecutionTimeout
from airflow.providers.databricks.hooks.databricks import LIST_SQL_ENDPOINTS_ENDPOINT
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from sqlalchemy.engine import URL
    from airflow.models.connection import Connection as AirflowConnection
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.sqlparser import DatabaseInfo


T = TypeVar("T")


def create_timeout_thread(cur, execution_timeout: timedelta | None) -> threading.Timer | None:
    if execution_timeout is not None:
        seconds_to_timeout = execution_timeout.total_seconds()
        t = threading.Timer(seconds_to_timeout, cur.connection.cancel)
    else:
        t = None

    return t


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
        **kwargs,
    ) -> None:
        super().__init__(databricks_conn_id, caller=caller)
        self._sql_conn: Connection | None = None
        self._token: str | None = None
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.supports_autocommit = True
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.additional_params = kwargs
        self.query_ids: list[str] = []

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
            endpoint = next(endpoint for endpoint in result["endpoints"] if endpoint["name"] == endpoint_name)
        except StopIteration:
            raise AirflowException(f"Can't find Databricks SQL endpoint with name '{endpoint_name}'")
        else:
            return endpoint

    def get_conn(self) -> AirflowConnection:
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

        prev_token = self._token
        new_token = self._get_token(raise_error=True)
        if not self._token or new_token != self._token:
            self._token = new_token

        if not self.session_config:
            self.session_config = self.databricks_conn.extra_dejson.get("session_configuration")

        if not self._sql_conn or prev_token != new_token:
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

        if self._sql_conn is None:
            raise AirflowException("SQL connection is not initialized")
        return cast("AirflowConnection", self._sql_conn)

    @property
    def sqlalchemy_url(self) -> "URL":
        """
        Return a Sqlalchemy.engine.URL object from the connection.

        :return: the extracted sqlalchemy.engine.URL object.
        """
        try:
            from sqlalchemy.engine import URL
        except ImportError:
            from airflow.exceptions import AirflowOptionalProviderFeatureException
            raise AirflowOptionalProviderFeatureException(
                "The 'sqlalchemy' extra is required to use 'sqlalchemy_url'. "
                "Please install it with: pip install 'apache-airflow-providers-databricks[sqlalchemy]'"
            )
        url_query = {
            "http_path": self._http_path,
            "catalog": self.catalog,
            "schema": self.schema,
        }
        url_query_formatted: dict[str, str] = {k: v for k, v in url_query.items() if v is not None}
        return URL.create(
            drivername="databricks",
            username="token",
            password=self._get_token(raise_error=True),
            host=self.host,
            query=url_query_formatted,
        )

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.

        :return: the extracted uri.
        """
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: None = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
        execution_timeout: timedelta | None = None,
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
        execution_timeout: timedelta | None = None,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None: ...

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = True,
        return_last: bool = True,
        execution_timeout: timedelta | None = None,
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
        :param execution_timeout: max time allowed for the execution of this task instance, if it goes beyond
            it will raise and fail.
        """
        self.descriptions = []
        self.query_ids = []

        if isinstance(sql, str):
            if split_statements:
                sql_list = [self.strip_sql_string(s) for s in self.split_sql_string(sql)]
            else:
                sql_list = [self.strip_sql_string(sql)]
        else:
            sql_list = [self.strip_sql_string(s) for s in sql]

        if sql_list:
            self.log.debug("Executing following statements against Databricks DB: %s", sql_list)
        else:
            raise ValueError("List of SQL statements is empty")

        conn = None
        results = []
        for sql_statement in sql_list:
            self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
            # when using AAD tokens, it could expire if previous query run longer than token lifetime
            conn = self.get_conn()
            with closing(conn.cursor()):
                self.set_autocommit(conn, autocommit)

                with closing(conn.cursor()) as cur:
                    t = create_timeout_thread(cur, execution_timeout)

                    # TODO: adjust this to make testing easier
                    try:
                        self._run_command(cur, sql_statement, parameters)
                    except Exception as e:
                        if t is None or t.is_alive():
                            raise DatabricksSqlExecutionError(
                                f"Error running SQL statement: {sql_statement}. {str(e)}"
                            )
                        raise DatabricksSqlExecutionTimeout(
                            f"Timeout threshold exceeded for SQL statement: {sql_statement} was cancelled."
                        )
                    finally:
                        if t is not None:
                            t.cancel()

                    if query_id := cur.query_id:
                        self.log.info("Databricks query id: %s", query_id)
                        self.query_ids.append(query_id)

                    if handler is not None:
                        raw_result = handler(cur)
                        result = self._make_common_data_structure(raw_result)

                        if return_single_query_results(sql, return_last, split_statements):
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
        return results

    def _make_common_data_structure(self, result: T | Sequence[T]) -> tuple[Any, ...] | list[tuple[Any, ...]]:
        """Transform the databricks Row objects into namedtuple."""
        # Below ignored lines respect namedtuple docstring, but mypy do not support dynamically
        # instantiated namedtuple, and will never do: https://github.com/python/mypy/issues/848
        if isinstance(result, list):
            rows: Sequence[Row] = result
            if not rows:
                return []
            rows_fields = tuple(rows[0].__fields__)
            rows_object = namedtuple("Row", rows_fields, rename=True)  # type: ignore
            return cast("list[tuple[Any, ...]]", [rows_object(*row) for row in rows])
        if isinstance(result, Row):
            row_fields = tuple(result.__fields__)
            row_object = namedtuple("Row", row_fields, rename=True)  # type: ignore
            return cast("tuple[Any, ...]", row_object(*result))
        raise TypeError(f"Expected Sequence[Row] or Row, but got {type(result)}")

    def bulk_dump(self, table, tmp_file):
        raise NotImplementedError()

    def bulk_load(self, table, tmp_file):
        raise NotImplementedError()

    def get_openlineage_database_info(self, connection) -> DatabaseInfo:
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        return DatabaseInfo(
            scheme=self.get_openlineage_database_dialect(connection),
            authority=self.host,
            database=self.catalog,
            information_schema_columns=[
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "data_type",
                "table_catalog",
            ],
            is_information_schema_cross_db=True,
        )

    def get_openlineage_database_dialect(self, _) -> str:
        return "databricks"

    def get_openlineage_default_schema(self) -> str | None:
        return self.schema or "default"

    def get_openlineage_database_specific_lineage(self, task_instance) -> OperatorLineage | None:
        """
        Emit separate OpenLineage events for each Databricks query, based on executed query IDs.

        If a single query ID is present, also add an `ExternalQueryRunFacet` to the returned lineage metadata.

        Note that `get_openlineage_database_specific_lineage` is usually called after task's execution,
        so if multiple query IDs are present, both START and COMPLETE event for each query will be emitted
        after task's execution. If we are able to query Databricks for query execution metadata,
        query event times will correspond to actual query's start and finish times.

        Args:
            task_instance: The Airflow TaskInstance object for which lineage is being collected.

        Returns:
            An `OperatorLineage` object if a single query ID is found; otherwise `None`.
        """
        from airflow.providers.common.compat.openlineage.facet import ExternalQueryRunFacet
        from airflow.providers.databricks.utils.openlineage import (
            emit_openlineage_events_for_databricks_queries,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        if not self.query_ids:
            self.log.info("OpenLineage could not find databricks query ids.")
            return None

        self.log.debug("openlineage: getting connection to get database info")
        connection = self.get_connection(self.get_conn_id())
        namespace = SQLParser.create_namespace(self.get_openlineage_database_info(connection))

        self.log.info("Separate OpenLineage events will be emitted for each Databricks query_id.")
        emit_openlineage_events_for_databricks_queries(
            task_instance=task_instance,
            hook=self,
            query_ids=self.query_ids,
            query_for_extra_metadata=True,
            query_source_namespace=namespace,
        )

        if len(self.query_ids) == 1:
            self.log.debug("Attaching ExternalQueryRunFacet with single query_id to OpenLineage event.")
            return OperatorLineage(
                run_facets={
                    "externalQuery": ExternalQueryRunFacet(
                        externalQueryId=self.query_ids[0], source=namespace
                    )
                }
            )

        return None
