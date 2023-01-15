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
from typing import Any, Callable, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class DatabricksSqlSensor(BaseSensorOperator):
    """
    Generic Databricks SQL sensor.

    :param databricks_conn_id:str=DatabricksSqlHook.default_conn_name: Specify the name of the connection
    to use with Databricks on Airflow
    :param http_path:str: Specify the path to the sql endpoint
    :param sql_endpoint_name:str: Specify the name of the sql endpoint to use
    :param session_configuration: Pass in the session configuration to be used
    :param http_headers:list[tuple[str, str]]: Pass http headers to the databricks API
    :param catalog:str|None=None: Specify the catalog to use for the query
    :param schema:str|None="default": Specify the schema of the table to be queried
    :param table_name:str: Specify the table that we want to monitor
    :param partition_name:dict|: Pass in the partition name to be used for the sensor
    :param handler:Callable[[Any, Any]=fetch_all_handler: Define the handler function that will be used
    to process the results of a query
    :param db_sensor_type:str: Choose the sensor you want to use. Available options: table_partition,
    table_changes.
    :param timestamp:datetime: To be used with query filters or as other argument values for timestamp
    :param caller: Identify the source of this sensor in logs
    :param client_parameters:dict[str, Any]: Additional client parameters
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
        schema: str | None = "default",
        table_name: str,
        partition_name: dict = {"date": "2023-1-1"},
        handler: Callable[[Any], Any] = fetch_all_handler,
        db_sensor_type: str,
        timestamp: datetime = datetime.now() - timedelta(days=7),
        caller: str = "DatabricksSqlSensor",
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
        self.schema = "default" if not schema else schema
        self.table_name = table_name
        self.partition_name = partition_name
        self.db_sensor_type = db_sensor_type
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

    def _generic_sql_sensor(self, sql):
        """Runs SQL commands in Databricks SQL Warehouse.

        Args:
            sql (_type_): SQL query

        Returns:
            Any: SQL query results, mixed type.
        """
        hook = self._get_hook()
        sql_result = hook.run(
            sql,
            handler=self.handler if self.do_xcom_push else None,
        )
        return sql_result

    @staticmethod
    def get_previous_version(context: Context, lookup_key):
        """Gets previous version of the table stored in Airflow metadata.

        Args:
            context (Context): Airflow context
            lookup_key (_type_): Unique lookup key used to store values related to a specific table.

        Returns:
            int: Version number
        """
        return context["ti"].xcom_pull(key=lookup_key, include_prior_dates=True)

    @staticmethod
    def set_version(context: Context, lookup_key, version):
        """Sets a specific version number for a Databricks table to the Airflow metadata using
        an existing lookup key.

        Args:
            context (Context): Airflow context
            lookup_key (_type_): Unique lookup key used to store values related to a specific table.
            version (int): Version number
        """
        context["ti"].xcom_push(key=lookup_key, value=version)

    def get_current_table_version(self, table_name, time_range):
        change_sql = (
            f"SELECT COUNT(version) as versions from "
            f"(DESCRIBE HISTORY {table_name}) "
            f"WHERE timestamp >= '{time_range}'"
        )
        result = self._generic_sql_sensor(change_sql)[0][0]
        return result

    def _check_table_partitions(self) -> bool:
        """Checks for the presence of the specified partition in the Databricks table.

        Raises:
            AirflowException

        Returns:
            bool: True if the partition value exists, False if not.
        """
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
        result = self._generic_sql_sensor(partition_sql)
        self.log.info("result: %s", result)
        if result[0][0] == 1:
            return True
        else:
            return False

    def _check_table_changes(self, context: Context) -> bool:
        """

        Args:
            context (Context): Airflow context

        Raises:
            AirflowException

        Returns:
            bool: True if a version number greater than or equal to current version has been detected.
        """
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

    template_fields: Sequence[str] = (
        "table_name",
        "schema",
        "partition_name",
    )

    def poke(self, context: Context) -> bool:
        if self.db_sensor_type == "table_partition":
            return self._check_table_partitions()
        elif self.db_sensor_type == "table_changes":
            return self._check_table_changes(context)
        return True
