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
"""This module contains Databricks sensors."""

from __future__ import annotations

from airflow.compat.functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Iterable, Sequence

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksSqlSensor(BaseSensorOperator):
    """
    Sensor to execute SQL statements on a Delta table via Databricks.

        :param databricks_conn_id: Reference to :ref:`Databricks
            connection id<howto/connection:databricks>` (templated), defaults to
            DatabricksSqlHook.default_conn_name
        :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
            If not specified, it should be either specified in the Databricks connection's
            extra parameters, or ``sql_endpoint_name`` must be specified.
        :param sql_endpoint_name: Optional name of Databricks SQL Endpoint. If not specified, ``http_path``
            must be provided as described above, defaults to None
        :param session_configuration: An optional dictionary of Spark session parameters. If not specified,
            it could be specified in the Databricks connection's extra parameters., defaults to None
        :param http_headers: An optional list of (k, v) pairs
            that will be set as HTTP headers on every request. (templated).
        :param catalog: An optional initial catalog to use.
            Requires DBR version 9.0+ (templated), defaults to ""
        :param schema: An optional initial schema to use.
            Requires DBR version 9.0+ (templated), defaults to "default"
        :param sql: SQL statement to be executed.
        :param handler: Handler for DbApiHook.run() to return results, defaults to fetch_all_handler
        :param client_parameters: Additional parameters internal to Databricks SQL Connector parameters.
    """

    template_fields: Sequence[str] = (
        "databricks_conn_id",
        "sql",
        "catalog",
        "schema",
        "http_headers",
    )

    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        *,
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: str | None = None,
        sql_endpoint_name: str | None = None,
        session_configuration=None,
        http_headers: list[tuple[str, str]] | None = None,
        catalog: str = "",
        schema: str = "default",
        sql: str | Iterable[str],
        handler: Callable[[Any], Any] = fetch_all_handler,
        client_parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        self.databricks_conn_id = databricks_conn_id
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.sql = sql
        self.caller = "DatabricksSqlSensor"
        self.client_parameters = client_parameters or {}
        self.hook_params = kwargs.pop("hook_params", {})
        self.handler = handler
        super().__init__(**kwargs)

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            self._http_path,
            self._sql_endpoint_name,
            self.session_config,
            self.http_headers,
            self.catalog,
            self.schema,
            self.caller,
            **self.client_parameters,
            **self.hook_params,
        )

    @cached_property
    def _get_results(self) -> bool:
        hook = self._get_hook()
        sql_result = hook.run(
            self.sql,
            handler=self.handler if self.do_xcom_push else None,
        )
        self.log.debug("SQL result: %s", sql_result)
        return bool(sql_result)

    def poke(self, context: Context) -> bool:
        return self._get_results
