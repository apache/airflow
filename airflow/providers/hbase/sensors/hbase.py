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
"""HBase sensors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.hbase.hooks.hbase import HBaseHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HBaseTableSensor(BaseSensorOperator):
    """
    Sensor to check if HBase table exists.
    
    :param table_name: Name of the table to check.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name",)

    def __init__(
        self,
        table_name: str,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.hbase_conn_id = hbase_conn_id

    def poke(self, context: Context) -> bool:
        """Check if table exists."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        exists = hook.table_exists(self.table_name)
        self.log.info("Table %s exists: %s", self.table_name, exists)
        return exists


class HBaseRowSensor(BaseSensorOperator):
    """
    Sensor to check if specific row exists in HBase table.
    
    :param table_name: Name of the table to check.
    :param row_key: Row key to check for existence.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "row_key")

    def __init__(
        self,
        table_name: str,
        row_key: str,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.row_key = row_key
        self.hbase_conn_id = hbase_conn_id

    def poke(self, context: Context) -> bool:
        """Check if row exists."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        try:
            row_data = hook.get_row(self.table_name, self.row_key)
            exists = bool(row_data)
            self.log.info("Row %s in table %s exists: %s", self.row_key, self.table_name, exists)
            return exists
        except Exception as e:
            self.log.error("Error checking row existence: %s", e)
            return False