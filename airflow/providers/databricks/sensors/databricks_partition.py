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

from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor
from airflow.utils.context import Context
from airflow.providers.databricks.utils.databricks import ParamEscaper


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
    :param _catalog: An optional initial catalog to use. Requires DBR version 9.0+ (templated)
    :param _schema: An optional initial schema to use. Requires DBR version 9.0+ (templated)
    :param table_name: Table name to generate the SQL query.
    :param partitions: Partitions to check, supplied via a dict.
    :param handler: Handler for DbApiHook.run() to return results, defaults to fetch_one_handler
    :param client_parameters: Additional parameters internal to Databricks SQL Connector parameters.
    :param partition_operator: Comparison operator for partitions.
    """

    template_fields: Sequence[str] = (
        "databricks_conn_id",
        "_schema",
        "http_headers",
        "_catalog",
        "table_name",
        "partitions",
    )

    def __init__(
        self,
        table_name: str,
        partitions: dict,
        partition_operator: str = "=",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.caller = "DatabricksPartitionSensor"
        self.partitions = partitions
        self.partition_operator = partition_operator
        self.table_name = table_name
        self.escaper = ParamEscaper()

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            self._http_path,
            self._sql_endpoint_name,
            self.session_config,
            self.http_headers,
            self._catalog,
            self._schema,
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

    def _generate_partition_query(
        self,
        prefix: str,
        suffix: str,
        joiner_val: str,
        table_name: str,
        opts: dict[str, str] | None = None,
        escape_key: bool = False,
    ) -> str:
        partition_columns = self._sql_sensor(f"DESCRIBE DETAIL {table_name}")[0][7]
        self.log.info("table_info: %s", partition_columns)
        if len(partition_columns) < 1:
            raise AirflowException("Table %s does not have partitions", table_name)
        formatted_opts = ""
        if opts is not None and len(opts) > 0:
            output_list = []
            for partition_col, partition_value in self.partitions.items():
                if escape_key:
                    partition_col = self.escaper.escape_item(partition_col)
                if partition_col in partition_columns:
                    if isinstance(partition_value, list):
                        output_list.append(f"""{partition_col} in {tuple(partition_value)}""")
                    if isinstance(partition_value, (int, float, complex)):
                        output_list.append(
                            f"""{partition_col}{self.partition_operator}{self.escaper.escape_item(partition_value)}"""
                        )
                    if isinstance(partition_value, str):
                        output_list.append(
                            f"""{partition_col}{self.partition_operator}{self.escaper.escape_item(partition_value)}"""
                        )
                    # TODO: Check date types.
                else:
                    raise AirflowException(
                        "Column %s not part of table partitions: %s", partition_col, partition_columns
                    )
        formatted_opts = f"{prefix} {joiner_val.join(output_list)} {suffix}"

        return formatted_opts.strip()

    def _check_table_partitions(self) -> list:
        _fully_qualified_table_name = str(self._catalog + "." + self._schema + "." + self.table_name)
        self.log.debug("Table name generated from arguments: %s", _fully_qualified_table_name)
        _joiner_val = " AND "
        _prefix = f"SELECT 1 FROM {_fully_qualified_table_name} WHERE"
        _suffix = " LIMIT 1"

        partition_sql = self._generate_partition_query(
            prefix=_prefix,
            suffix=_suffix,
            joiner_val=_joiner_val,
            opts=self.partitions,
            table_name=_fully_qualified_table_name,
            escape_key=False,
        )
        # partition_sql = f"SELECT 1 FROM {_fully_qualified_table_name} WHERE {partitions} LIMIT 1"
        return self._sql_sensor(partition_sql)

    def _get_results(self) -> bool:
        result = self._check_table_partitions()
        self.log.debug("Partition sensor result: %s", result)
        if len(result) < 1:
            raise AirflowException("Databricks SQL partition sensor failed.")
        return True

    def poke(self, context: Context) -> bool:
        return self._get_results()
