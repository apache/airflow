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
"""HBase hook module."""

from __future__ import annotations

from typing import Any

import happybase

from airflow.hooks.base import BaseHook


class HBaseHook(BaseHook):
    """
    Wrapper for connection to interact with HBase.
    
    This hook provides basic functionality to connect to HBase
    and perform operations on tables.
    """

    conn_name_attr = "hbase_conn_id"
    default_conn_name = "hbase_default"
    conn_type = "hbase"
    hook_name = "HBase"

    def __init__(self, hbase_conn_id: str = default_conn_name) -> None:
        """
        Initialize HBase hook.

        :param hbase_conn_id: The connection ID to use for HBase connection.
        """
        super().__init__()
        self.hbase_conn_id = hbase_conn_id
        self._connection = None

    def get_conn(self) -> happybase.Connection:
        """Return HBase connection."""
        if self._connection is None:
            conn = self.get_connection(self.hbase_conn_id)
            
            connection_args = {
                "host": conn.host or "localhost",
                "port": conn.port or 9090,
            }
            
            # Add extra parameters from connection
            if conn.extra_dejson:
                connection_args.update(conn.extra_dejson)
            
            self.log.info("Connecting to HBase at %s:%s", connection_args["host"], connection_args["port"])
            self._connection = happybase.Connection(**connection_args)
        
        return self._connection

    def get_table(self, table_name: str) -> happybase.Table:
        """
        Get HBase table object.
        
        :param table_name: Name of the table to get.
        :return: HBase table object.
        """
        connection = self.get_conn()
        return connection.table(table_name)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in HBase.
        
        :param table_name: Name of the table to check.
        :return: True if table exists, False otherwise.
        """
        connection = self.get_conn()
        return table_name.encode() in connection.tables()

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """
        Create HBase table.
        
        :param table_name: Name of the table to create.
        :param families: Dictionary of column families and their configuration.
        """
        connection = self.get_conn()
        connection.create_table(table_name, families)
        self.log.info("Created table %s", table_name)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """
        Delete HBase table.
        
        :param table_name: Name of the table to delete.
        :param disable: Whether to disable table before deletion.
        """
        connection = self.get_conn()
        if disable:
            connection.disable_table(table_name)
        connection.delete_table(table_name)
        self.log.info("Deleted table %s", table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """
        Put data into HBase table.
        
        :param table_name: Name of the table.
        :param row_key: Row key for the data.
        :param data: Dictionary of column:value pairs to insert.
        """
        table = self.get_table(table_name)
        table.put(row_key, data)
        self.log.info("Put row %s into table %s", row_key, table_name)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """
        Get row from HBase table.
        
        :param table_name: Name of the table.
        :param row_key: Row key to retrieve.
        :param columns: List of columns to retrieve (optional).
        :return: Dictionary of column:value pairs.
        """
        table = self.get_table(table_name)
        return table.row(row_key, columns=columns)

    def scan_table(
        self, 
        table_name: str, 
        row_start: str | None = None, 
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Scan HBase table.
        
        :param table_name: Name of the table.
        :param row_start: Start row key for scan.
        :param row_stop: Stop row key for scan.
        :param columns: List of columns to retrieve.
        :param limit: Maximum number of rows to return.
        :return: List of (row_key, data) tuples.
        """
        table = self.get_table(table_name)
        return list(table.scan(
            row_start=row_start,
            row_stop=row_stop,
            columns=columns,
            limit=limit
        ))

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for HBase connection."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {},
        }

    def close(self) -> None:
        """Close HBase connection."""
        if self._connection:
            self._connection.close()
            self._connection = None