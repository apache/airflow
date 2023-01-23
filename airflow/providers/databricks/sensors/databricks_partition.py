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

from typing import Any, Callable, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor
from airflow.utils.context import Context

template_fields: Sequence[str] = ("databricks_conn_id", "catalog", "schema", "table_name", "partition_name")


class DatabricksPartitionSensor(DatabricksSqlSensor):
    """Sensor to detect the existence of partitions in a Delta table.

    :param databricks_conn_id: Reference to :ref:`Databricks
        connection id<howto/connection:databricks>` (templated).
    :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
        If not specified, it should be either specified in the Databricks connection's extra parameters,
        or ``sql_endpoint_name`` must be specified.
    :param sql_endpoint_name: Optional name of Databricks SQL Endpoint.
        If not specified, ``http_path`` must be provided as described above.
    :param session_configuration: An optional dictionary of Spark session parameters. If not specified,
        it could be specified in the Databricks connection's extra parameters.
    :param http_headers: An optional list of (k, v) pairs that will be set
        as HTTP headers on every request. (templated)
    :param catalog: An optional initial catalog to use. Requires DBR version 9.0+ (templated)
    :param schema: An optional initial schema to use. Requires DBR version 9.0+ (templated)
    :param table_name: Table name to generate the SQL query.
    :param partition_name: Partition to check, defaults to {"date": datetime.now().date()}
    :param handler: Handler for DbApiHook.run() to return results, defaults to fetch_all_handler
    :param caller: String passed to name a hook to Databricks, defaults to "DatabricksPartitionSensor"
    :param client_parameters: Additional parameters internal to Databricks SQL Connector parameters.
    """

    template_fields: Sequence[str] = (
        "databricks_conn_id",
        "schema",
        "http_headers",
        "catalog",
        "table_name",
        "partition_name",
    )

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

    def _check_table_partitions(self) -> list:
        if self.catalog is not None:
            complete_table_name = str(self.catalog + "." + self.schema + "." + self.table_name)
            self.log.debug("Table name generated from arguments: %s", complete_table_name)
        else:
            raise AirflowException("Catalog name not specified, aborting query execution.")
        partition_columns = list(self._sql_sensor(f"describe detail {complete_table_name}")[0][7])
        self.log.debug("table_info: %s", partition_columns)
        if len(partition_columns) < 1:
            raise AirflowException("Table %s does not have partitions", complete_table_name)
        partitions_list = []
        for partition_col, partition_value in self.partition_name.items():
            if partition_col in partition_columns:
                if isinstance(partition_value, (int, float, complex)):
                    partitions_list.append(f"""{partition_col}={partition_value}""")
                else:
                    partitions_list.append(f"""{partition_col}=\"{partition_value}\"""")
            else:
                raise AirflowException(
                    "Column %s not part of table partitions: %s", partition_col, partition_columns
                )
        partitions = " AND ".join(partitions_list)
        partition_sql = f"SELECT 1 FROM {complete_table_name} WHERE {partitions}"
        return self._sql_sensor(partition_sql)

    def poke(self, context: Context) -> bool:
        result = self._check_table_partitions()
        self.log.debug("Partition sensor result: %s", result)
        if len(result) < 1:
            raise AirflowException("Databricks SQL partition sensor failed.")
        return True
