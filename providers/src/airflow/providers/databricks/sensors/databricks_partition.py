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

from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Sequence

from databricks.sql.utils import ParamEscaper

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksPartitionSensor(BaseSensorOperator):
    """
    Sensor to detect the presence of table partitions in Databricks.

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
    :param table_name: Name of the table to check partitions.
    :param partitions: Name of the partitions to check.
        Example: {"date": "2023-01-03", "name": ["abc", "def"]}
    :param partition_operator: Optional comparison operator for partitions, such as >=.
    :param handler: Handler for DbApiHook.run() to return results, defaults to fetch_all_handler
    :param client_parameters: Additional parameters internal to Databricks SQL connector parameters.
    """

    template_fields: Sequence[str] = (
        "databricks_conn_id",
        "catalog",
        "schema",
        "table_name",
        "partitions",
        "http_headers",
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
        table_name: str,
        partitions: dict,
        partition_operator: str = "=",
        handler: Callable[[Any], Any] = fetch_all_handler,
        client_parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        self.databricks_conn_id = databricks_conn_id
        self._http_path = http_path
        self._sql_warehouse_name = sql_warehouse_name
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.caller = "DatabricksPartitionSensor"
        self.partitions = partitions
        self.partition_operator = partition_operator
        self.table_name = table_name
        self.client_parameters = client_parameters or {}
        self.hook_params = kwargs.pop("hook_params", {})
        self.handler = handler
        self.escaper = ParamEscaper()
        super().__init__(**kwargs)

    def _sql_sensor(self, sql):
        """Execute the supplied SQL statement using the hook object."""
        hook = self._get_hook
        sql_result = hook.run(
            sql,
            handler=self.handler if self.do_xcom_push else None,
        )
        self.log.debug("SQL result: %s", sql_result)
        return sql_result

    @cached_property
    def _get_hook(self) -> DatabricksSqlHook:
        """Create and return a DatabricksSqlHook object."""
        return DatabricksSqlHook(
            self.databricks_conn_id,
            self._http_path,
            self._sql_warehouse_name,
            self.session_config,
            self.http_headers,
            self.catalog,
            self.schema,
            self.caller,
            **self.client_parameters,
            **self.hook_params,
        )

    def _check_table_partitions(self) -> list:
        """Generate the fully qualified table name, generate partition, and call the _sql_sensor method."""
        if self.table_name.split(".")[0] == "delta":
            _fully_qualified_table_name = self.table_name
        else:
            _fully_qualified_table_name = f"{self.catalog}.{self.schema}.{self.table_name}"
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
        return self._sql_sensor(partition_sql)

    def _generate_partition_query(
        self,
        prefix: str,
        suffix: str,
        joiner_val: str,
        table_name: str,
        opts: dict[str, str] | None = None,
        escape_key: bool = False,
    ) -> str:
        """
        Query the table for available partitions.

        Generates the SQL query based on the partition data types.
            * For a list, it prepares the SQL in the format:
                column_name in (value1, value2,...)
            * For a numeric type, it prepares the format:
                column_name =(or other provided operator such as >=) value
            * For a date type, it prepares the format:
                column_name =(or other provided operator such as >=) value
        Once the filter predicates have been generated like above, the query
        is prepared to be executed using the prefix and suffix supplied, which are:
        "SELECT 1 FROM {_fully_qualified_table_name} WHERE" and "LIMIT 1".
        """
        partition_columns = self._sql_sensor(f"DESCRIBE DETAIL {table_name}")[0][7]
        self.log.debug("Partition columns: %s", partition_columns)
        if len(partition_columns) < 1:
            message = f"Table {table_name} does not have partitions"
            raise AirflowException(message)

        formatted_opts = ""
        if opts:
            output_list = []
            for partition_col, partition_value in opts.items():
                if escape_key:
                    partition_col = self.escaper.escape_item(partition_col)
                if partition_col in partition_columns:
                    if isinstance(partition_value, list):
                        output_list.append(f"""{partition_col} in {tuple(partition_value)}""")
                        self.log.debug("List formatting for partitions: %s", output_list)
                    if isinstance(partition_value, (int, float, complex)):
                        output_list.append(
                            f"""{partition_col}{self.partition_operator}{self.escaper.escape_item(partition_value)}"""
                        )
                    if isinstance(partition_value, (str, datetime)):
                        output_list.append(
                            f"""{partition_col}{self.partition_operator}{self.escaper.escape_item(partition_value)}"""
                        )
                else:
                    message = f"Column {partition_col} not part of table partitions: {partition_columns}"
                    raise AirflowException(message)
        else:
            # Raises exception if the table does not have any partitions.
            message = "No partitions specified to check with the sensor."
            raise AirflowException(message)
        formatted_opts = f"{prefix} {joiner_val.join(output_list)} {suffix}"
        self.log.debug("Formatted options: %s", formatted_opts)

        return formatted_opts.strip()

    def poke(self, context: Context) -> bool:
        """Check the table partitions and return the results."""
        partition_result = self._check_table_partitions()
        self.log.debug("Partition sensor result: %s", partition_result)
        if partition_result:
            return True
        else:
            message = f"Specified partition(s): {self.partitions} were not found."
            raise AirflowException(message)
