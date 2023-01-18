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

from datetime import datetime, timedelta
from typing import Any, Callable

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor
from airflow.utils.context import Context


class DatabricksTableChangesSensor(DatabricksSqlSensor):
    """Sensor to detect changes in a Delta table.

    :param databricks_conn_id: _description_, defaults to DatabricksSqlHook.default_conn_name
    :param http_path: _description_, defaults to None
    :param sql_endpoint_name: _description_, defaults to None
    :param session_configuration: _description_, defaults to None
    :param http_headers: _description_, defaults to None
    :param catalog: _description_, defaults to ""
    :param schema: _description_, defaults to "default"
    :param table_name: _description_, defaults to ""
    :param partition_name: _description_, defaults to {"date": "2023-1-1"}
    :param handler: _description_, defaults to fetch_all_handler
    :param timestamp: _description_, defaults to datetime.now()-timedelta(days=7)
    :param caller: _description_, defaults to "DatabricksTableChangesSensor"
    :param client_parameters: _description_, defaults to None
    """

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
        table_name: str = "",
        partition_name: dict = {"date": "2023-1-1"},
        handler: Callable[[Any], Any] = fetch_all_handler,
        timestamp: datetime = datetime.now() - timedelta(days=7),
        caller: str = "DatabricksTableChangesSensor",
        client_parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.table_name = table_name
        self.partition_name = partition_name
        self.timestamp = timestamp
        self.caller = caller
        self.client_parameters = client_parameters or {}
        self.hook_params = kwargs.pop("hook_params", {})
        self.handler = handler

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

    def _sql_sensor(self, sql):
        hook = self._get_hook()
        sql_result = hook.run(
            sql,
            handler=self.handler if self.do_xcom_push else None,
        )
        return sql_result

    @staticmethod
    def get_previous_version(context: Context, lookup_key):
        return context["ti"].xcom_pull(key=lookup_key, include_prior_dates=True)

    @staticmethod
    def set_version(context: Context, lookup_key, version):
        context["ti"].xcom_push(key=lookup_key, value=version)

    def get_current_table_version(self, table_name, time_range):
        change_sql = (
            f"SELECT COUNT(version) as versions from "
            f"(DESCRIBE HISTORY {table_name}) "
            f"WHERE timestamp >= '{time_range}'"
        )
        result = self._sql_sensor(change_sql)[0][0]
        return result

    def _check_table_changes(self, context: Context) -> bool:
        if self.catalog is not None:
            complete_table_name = str(self.catalog + "." + self.schema + "." + self.table_name)
            self.log.info("Table name generated from arguments: %s", complete_table_name)
        else:
            raise AirflowException("Catalog name not specified, aborting query execution.")
        prev_version = -1
        if context is not None:
            lookup_key = complete_table_name
            prev_data = self.get_previous_version(lookup_key=lookup_key, context=context)
            self.log.info("prev_data: %s, type=%s", str(prev_data), type(prev_data))
            if isinstance(prev_data, int):
                prev_version = prev_data
            elif prev_data is not None:
                raise AirflowException("Incorrect type for previous XCom data: %s", type(prev_data))
            version = self.get_current_table_version(
                table_name=complete_table_name, time_range=self.timestamp
            )
            self.log.info("Current table version: %s", version)
            result = prev_version <= version
            if prev_version != version:
                self.set_version(lookup_key=lookup_key, version=version, context=context)
            return result

    def poke(self, context: Context) -> bool:
        result = self._check_table_changes(context=context)
        return result
