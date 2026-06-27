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

from collections.abc import Callable, Iterable, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.utils.query_tags import build_query_tags

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DatabricksSqlSensor(BaseSensorOperator):
    """
    Sensor that runs a SQL query on Databricks.

    :param databricks_conn_id: Reference to :ref:`Databricks
        connection id<howto/connection:databricks>` (templated), defaults to
        DatabricksSqlHook.default_conn_name.
    :param sql_warehouse_name: Optional name of Databricks SQL warehouse. If not specified, ``http_path``
        must be provided as described below, defaults to None
    :param http_path: Optional string specifying HTTP path of Databricks SQL warehouse or All Purpose cluster.
        If not specified, it should be either specified in the Databricks connection's
        extra parameters, or ``sql_warehouse_name`` must be specified.
    :param session_configuration: An optional dictionary of Spark session parameters. If not specified,
        it could be specified in the Databricks connection's extra parameters, defaults to None
    :param http_headers: An optional list of (k, v) pairs
        that will be set as HTTP headers on every request. (templated).
    :param catalog: An optional initial catalog to use.
        Requires Databricks Runtime version 9.0+ (templated), defaults to ""
    :param schema: An optional initial schema to use.
        Requires Databricks Runtime version 9.0+ (templated), defaults to "default"
    :param sql: SQL statement to be executed.
    :param handler: Handler for DbApiHook.run() to return results, defaults to fetch_all_handler
    :param client_parameters: Additional parameters internal to Databricks SQL connector parameters.
    :param query_tags: Optional dictionary of query tags to attach to Databricks SQL queries.
        Tags are injected via the ``QUERY_TAGS`` Databricks session parameter so they appear in
        ``system.query.history``. (templated)
    :param include_airflow_query_tags: If True, add Airflow DAG/task/run metadata as query tags.
        Defaults to True.
    """

    template_fields: Sequence[str] = (
        "databricks_conn_id",
        "sql",
        "catalog",
        "schema",
        "http_headers",
        "query_tags",
    )

    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        *,
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: str | None = None,
        sql_warehouse_name: str | None = None,
        session_configuration=None,
        http_headers: list[tuple[str, str]] | None = None,
        catalog: str = "",
        schema: str = "default",
        sql: str | Iterable[str],
        handler: Callable[[Any], Any] = fetch_all_handler,
        client_parameters: dict[str, Any] | None = None,
        query_tags: dict[str, str | None] | None = None,
        include_airflow_query_tags: bool = True,
        **kwargs,
    ) -> None:
        """Create DatabricksSqlSensor object using the specified input arguments."""
        self.databricks_conn_id = databricks_conn_id
        self._http_path = http_path
        self._sql_warehouse_name = sql_warehouse_name
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.sql = sql
        self.caller = "DatabricksSqlSensor"
        self.client_parameters = client_parameters or {}
        self.hook_params = kwargs.pop("hook_params", {})
        self.handler = handler
        self.query_tags = query_tags or {}
        self.include_airflow_query_tags = include_airflow_query_tags
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> DatabricksSqlHook:
        """Creates and returns a DatabricksSqlHook object."""
        return DatabricksSqlHook(
            self.databricks_conn_id,
            self._http_path,
            self._sql_warehouse_name,
            self.session_config,
            self.http_headers,
            self.catalog,
            self.schema,
            caller=self.caller,
            **self.client_parameters,
            **self.hook_params,
        )

    def _get_results(self) -> bool:
        """Use the Databricks SQL hook and run the specified SQL query."""
        if not (self._http_path or self._sql_warehouse_name):
            message = (
                "Databricks SQL warehouse/cluster configuration missing. Please specify either"
                " http_path or sql_warehouse_name."
            )
            raise AirflowException(message)
        hook = self.hook
        sql_result = hook.run(
            self.sql,
            handler=self.handler if self.do_xcom_push else None,
        )
        self.log.debug("SQL result: %s", sql_result)
        return bool(sql_result)

    def poke(self, context: Context) -> bool:
        """Sensor poke function to get and return results from the SQL sensor."""
        self.hook.query_tags = build_query_tags(context, self.query_tags, self.include_airflow_query_tags)
        return self._get_results()
