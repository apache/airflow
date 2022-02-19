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

from typing import TYPE_CHECKING, Any, Optional, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.databricks.hooks.databricks import DatabricksSQLHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksPartitionTableSensor(BaseSensorOperator):
    """
    Waits for a partition to show up in Databricks.

    :param table: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :param partition: The partition clause to wait for.
    :param database: The name of the database in Databrick. It uses 'default' if nothing is provided
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    """
    template_fields: Sequence[str] = (
        'database',
        'table',
        'partition',
    )

    def __init__(self, *,  databricks_conn_id: str, table: str, partition: str, database: Optional[str] = 'default', **kwargs: Any):
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.table = table
        self.partition = partition
        self.database = 'default' if not database else database

    def poke(self, context: Context) -> bool:
        databricks_sql_hook = DatabricksSQLHook(databricks_conn_id=self.databricks_conn_id)

        for partition in databricks_sql_hook.execute_sql(f'SHOW PARTITIONS {self.database}.{self.table}'):
            if partition == self.partition:
                return True
