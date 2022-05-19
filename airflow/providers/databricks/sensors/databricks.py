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

from typing import Dict, Any, List, Optional, Sequence, Tuple
from datetime import datetime
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook


from airflow.utils.context import Context


class DatabricksBaseSensor(BaseSensorOperator):

    def __init__(
        self,
        *,
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: Optional[str] = None,
        sql_endpoint_name: Optional[str] = None,
        session_configuration=None,
        http_headers: Optional[List[Tuple[str, str]]] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        client_parameters: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksSqlSensor``."""
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.client_parameters = client_parameters or {}

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            http_path=self._http_path,
            session_configuration=self.session_config,
            sql_endpoint_name=self._sql_endpoint_name,
            http_headers=self.http_headers,
            catalog=self.catalog,
            schema=self.schema,
            **self.client_parameters,
        )


class DatabricksPartitionTableSensor(DatabricksBaseSensor):
    """
    Waits for a partition to show up in Databricks.

    :param table_name (str): The name of the table to wait for.
    :param partition_name (str): The partition clause to wait for.
    :param database_name (str): The name of the database in Databricks. It uses 'default' if nothing is provided
    :param databricks_conn_id (str): Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    """
    template_fields: Sequence[str] = (
        'database_name',
        'table_name',
        'partition_name',
    )

    def __init__(self, *,
                 databricks_conn_id: str,
                 table_name: str,
                 partition_name: str,
                 database_name: Optional[str] = 'default',
                 **kwargs: Any):
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.table_name = table_name
        self.partition_name = partition_name
        self.database_name = database_name

    def poke(self, context: Context) -> bool:
        hook = self._get_hook()
        _, result = hook.run(f'SHOW PARTITIONS {self.database_name}.{self.table_name}')
        record = result[0] if result else {}
        return self.partition_name in record


class DatabricksDeltaTableChangeSensor(DatabricksBaseSensor):
    """
    Waits for Delta table event

    :param table_name (str): The name of the table to wait for, supports the dot
    :param timestamp (datetime): The timestamp that will be used to filter new events.
    :param database (Optional[str]): The name of the database in Databrick. It uses 'default' if nothing is provided
    :param databricks_conn_id (str): Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    """
    template_fields: Sequence[str] = (
        'database_name',
        'table_name',
    )

    def __init__(self, *,
                 databricks_conn_id: str,
                 table_name: str,
                 timestamp: datetime,
                 database_name: Optional[str] = 'default',
                 **kwargs: Any):
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.table_name = table_name
        self.timestamp = timestamp
        self.database_name = 'default' if not database_name else database_name

    def poke(self, context: Context) -> bool:
        hook = self._get_hook()

        _, results = hook.run(
            f'SELECT COUNT(1) as new_events from (DESCRIBE '
            f'HISTORY {self.database_name}.{self.table_name}) '
            f'WHERE timestamp > "{self.timestamp}"')

        return results[0].new_events > 0
