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
    def __init__(
        self,
        *,
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: str | None = None,
        sql_endpoint_name: str | None = None,
        session_configuration=None,
        http_headers: list[tuple[str, str]] | None = None,
        catalog: str = "",
        schema: str | None = "default",
        table_name: str | None = None,
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

    def _check_table_partitions(self) -> bool:
        if self.catalog is not None:
            complete_table_name = str(self.catalog + "." + self.schema + "." + self.table_name)
            self.log.info("Table name generated from arguments: %s", complete_table_name)
        else:
            raise AirflowException("Catalog name not specified, aborting query execution.")
        partitions_list = []
        for partition_col, partition_value in self.partition_name.items():
            if isinstance(partition_value, (int, float, complex)):
                partitions_list.append(f"""{partition_col}={partition_value}""")
            else:
                partitions_list.append(f"""{partition_col}=\"{partition_value}\"""")
        partitions = " AND ".join(partitions_list)
        partition_sql = f"SELECT 1 FROM {complete_table_name} WHERE {partitions}"
        result = self._sql_sensor(partition_sql)
        self.log.info("result: %s", result)
        if len(result) > 1 and result[0][0] == 1:
            return True
        else:
            return False

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
        if result:
            return True
        else:
            return False
