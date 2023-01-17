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


class DatabricksPartitionSensor(DatabricksSqlSensor):
    """Sensor to detect the existence of partitions in a Delta table.

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
    :param caller: _description_, defaults to "DatabricksPartitionSensor"
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
        caller: str = "DatabricksPartitionSensor",
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
        return result

    def poke(self, context: Context) -> bool:
        result = self._check_table_partitions()
        if result:
            return True
        else:
            return False
