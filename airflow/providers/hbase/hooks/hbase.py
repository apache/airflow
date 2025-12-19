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

import subprocess
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

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """
        Insert multiple rows in batch.
        
        :param table_name: Name of the table.
        :param rows: List of dictionaries with 'row_key' and data columns.
        """
        table = self.get_table(table_name)
        with table.batch() as batch:
            for row in rows:
                row_key = row.pop('row_key')
                batch.put(row_key, row)
        self.log.info("Batch put %d rows into table %s", len(rows), table_name)

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """
        Get multiple rows in batch.
        
        :param table_name: Name of the table.
        :param row_keys: List of row keys to retrieve.
        :param columns: List of columns to retrieve.
        :return: List of row data dictionaries.
        """
        table = self.get_table(table_name)
        return [dict(data) for key, data in table.rows(row_keys, columns=columns)]

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """
        Delete row or specific columns from HBase table.
        
        :param table_name: Name of the table.
        :param row_key: Row key to delete.
        :param columns: List of columns to delete (if None, deletes entire row).
        """
        table = self.get_table(table_name)
        table.delete(row_key, columns=columns)
        self.log.info("Deleted row %s from table %s", row_key, table_name)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """
        Get column families for a table.
        
        :param table_name: Name of the table.
        :return: Dictionary of column families and their properties.
        """
        table = self.get_table(table_name)
        return table.families()

    def get_openlineage_database_info(self, connection):
        """Return HBase specific information for OpenLineage."""
        try:
            from airflow.providers.openlineage.sqlparser import DatabaseInfo
            return DatabaseInfo(
                scheme="hbase",
                authority=f"{connection.host}:{connection.port or 9090}",
                database="default",
            )
        except ImportError:
            return None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for HBase connection."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {
                "host": "HBase Thrift Server Host",
                "port": "HBase Thrift Server Port",
            },
            "placeholders": {
                "host": "localhost",
                "port": "9090",
            },
        }



    def execute_hbase_command(self, command: str, ssh_conn_id: str | None = None, **kwargs) -> str:
        """
        Execute HBase shell command.
        
        :param command: HBase command to execute (without 'hbase' prefix).
        :param ssh_conn_id: SSH connection ID for remote execution.
        :param kwargs: Additional arguments for subprocess.
        :return: Command output.
        """
        full_command = f"hbase {command}"
        self.log.info("Executing HBase command: %s", full_command)
        
        if ssh_conn_id:
            # Use SSH to execute command on remote server
            try:
                from airflow.providers.ssh.hooks.ssh import SSHHook
            except (AttributeError, ImportError) as e:
                if "DSSKey" in str(e) or "paramiko" in str(e):
                    self.log.warning("SSH provider has compatibility issues with current paramiko version. Using local execution.")
                    ssh_conn_id = None
                else:
                    raise
            
            if ssh_conn_id:  # If SSH is still available after import check
                ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                
                # Get hbase_home and java_home from SSH connection extra
                ssh_conn = ssh_hook.get_connection(ssh_conn_id)
                hbase_home = None
                java_home = None
                environment = {}
                if ssh_conn.extra_dejson:
                    hbase_home = ssh_conn.extra_dejson.get('hbase_home')
                    java_home = ssh_conn.extra_dejson.get('java_home')
                
                # Use full path if hbase_home is provided
                if hbase_home:
                    full_command = full_command.replace('hbase ', f'{hbase_home}/bin/hbase ')
                
                # Set JAVA_HOME if provided - add it to the command
                if java_home:
                    full_command = f'JAVA_HOME={java_home} {full_command}'
                
                self.log.info("Executing via SSH: %s", full_command)
                with ssh_hook.get_conn() as ssh_client:
                    exit_status, stdout, stderr = ssh_hook.exec_ssh_client_command(
                        ssh_client=ssh_client,
                        command=full_command,
                        get_pty=False,
                        environment=None
                    )
                    if exit_status != 0:
                        self.log.error("SSH command failed with exit code %d: %s", exit_status, stderr.decode())
                        raise RuntimeError(f"SSH command failed: {stderr.decode()}")
                    return stdout.decode()
        
        if not ssh_conn_id:
            # Execute locally
            try:
                result = subprocess.run(
                    full_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    check=True,
                    **kwargs
                )
                self.log.info("Command executed successfully")
                return result.stdout
            except subprocess.CalledProcessError as e:
                self.log.error("Command failed with return code %d: %s", e.returncode, e.stderr)
                raise

    def close(self) -> None:
        """Close HBase connection."""
        if self._connection:
            self._connection.close()
            self._connection = None