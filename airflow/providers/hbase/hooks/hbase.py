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
from enum import Enum
from typing import Any

import happybase

from airflow.hooks.base import BaseHook
from airflow.providers.hbase.auth import AuthenticatorFactory
from airflow.providers.hbase.hooks.hbase_strategy import HBaseStrategy, ThriftStrategy, SSHStrategy
from airflow.providers.ssh.hooks.ssh import SSHHook


class ConnectionMode(Enum):
    """HBase connection modes."""
    THRIFT = "thrift"
    SSH = "ssh"


class HBaseHook(BaseHook):
    """
    Wrapper for connection to interact with HBase.

    This hook provides basic functionality to connect to HBase
    and perform operations on tables via Thrift or SSH.
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
        self._connection_mode = None  # 'thrift' or 'ssh'
        self._strategy = None

    def _get_connection_mode(self) -> ConnectionMode:
        """Determine connection mode based on configuration."""
        if self._connection_mode is None:
            conn = self.get_connection(self.hbase_conn_id)
            self.log.info("Connection extra: %s", conn.extra_dejson)
            # Check if SSH connection is configured
            if conn.extra_dejson and conn.extra_dejson.get("connection_mode") == ConnectionMode.SSH.value:
                self._connection_mode = ConnectionMode.SSH
                self.log.info("Using SSH connection mode")
            else:
                self._connection_mode = ConnectionMode.THRIFT
                self.log.info("Using Thrift connection mode")
        return self._connection_mode

    def _get_strategy(self) -> HBaseStrategy:
        """Get appropriate strategy based on connection mode."""
        if self._strategy is None:
            if self._get_connection_mode() == ConnectionMode.SSH:
                ssh_hook = SSHHook(ssh_conn_id=self._get_ssh_conn_id())
                self._strategy = SSHStrategy(self.hbase_conn_id, ssh_hook, self.log)
            else:
                connection = self.get_conn()
                self._strategy = ThriftStrategy(connection, self.log)
        return self._strategy

    def _get_ssh_conn_id(self) -> str:
        """Get SSH connection ID from HBase connection extra."""
        conn = self.get_connection(self.hbase_conn_id)
        ssh_conn_id = conn.extra_dejson.get("ssh_conn_id") if conn.extra_dejson else None
        if not ssh_conn_id:
            raise ValueError("SSH connection ID must be specified in extra parameters")
        return ssh_conn_id

    def get_conn(self) -> happybase.Connection:
        """Return HBase connection (Thrift mode only)."""
        if self._get_connection_mode() == ConnectionMode.SSH:
            raise RuntimeError(
                "get_conn() is not available in SSH mode. Use execute_hbase_command() instead.")

        if self._connection is None:
            conn = self.get_connection(self.hbase_conn_id)

            connection_args = {
                "host": conn.host or "localhost",
                "port": conn.port or 9090,
            }

            # Setup authentication
            auth_method = conn.extra_dejson.get("auth_method", "simple") if conn.extra_dejson else "simple"
            authenticator = AuthenticatorFactory.create(auth_method)
            auth_kwargs = authenticator.authenticate(conn.extra_dejson or {})
            connection_args.update(auth_kwargs)

            self.log.info("Connecting to HBase at %s:%s with %s authentication",
                          connection_args["host"], connection_args["port"], auth_method)
            self._connection = happybase.Connection(**connection_args)

        return self._connection

    def get_table(self, table_name: str) -> happybase.Table:
        """
        Get HBase table object (Thrift mode only).

        :param table_name: Name of the table to get.
        :return: HBase table object.
        """
        if self._get_connection_mode() == ConnectionMode.SSH:
            raise RuntimeError(
                "get_table() is not available in SSH mode. Use SSH-specific methods instead.")
        connection = self.get_conn()
        return connection.table(table_name)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in HBase.

        :param table_name: Name of the table to check.
        :return: True if table exists, False otherwise.
        """
        return self._get_strategy().table_exists(table_name)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """
        Create HBase table.

        :param table_name: Name of the table to create.
        :param families: Dictionary of column families and their configuration.
        """
        self._get_strategy().create_table(table_name, families)
        self.log.info("Created table %s", table_name)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """
        Delete HBase table.

        :param table_name: Name of the table to delete.
        :param disable: Whether to disable table before deletion.
        """
        self._get_strategy().delete_table(table_name, disable)
        self.log.info("Deleted table %s", table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """
        Put data into HBase table.

        :param table_name: Name of the table.
        :param row_key: Row key for the data.
        :param data: Dictionary of column:value pairs to insert.
        """
        self._get_strategy().put_row(table_name, row_key, data)
        self.log.info("Put row %s into table %s", row_key, table_name)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """
        Get row from HBase table.

        :param table_name: Name of the table.
        :param row_key: Row key to retrieve.
        :param columns: List of columns to retrieve (optional).
        :return: Dictionary of column:value pairs.
        """
        return self._get_strategy().get_row(table_name, row_key, columns)

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
        return self._get_strategy().scan_table(table_name, row_start, row_stop, columns, limit)

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """
        Insert multiple rows in batch.

        :param table_name: Name of the table.
        :param rows: List of dictionaries with 'row_key' and data columns.
        """
        self._get_strategy().batch_put_rows(table_name, rows)
        self.log.info("Batch put %d rows into table %s", len(rows), table_name)

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """
        Get multiple rows in batch.

        :param table_name: Name of the table.
        :param row_keys: List of row keys to retrieve.
        :param columns: List of columns to retrieve.
        :return: List of row data dictionaries.
        """
        return self._get_strategy().batch_get_rows(table_name, row_keys, columns)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """
        Delete row or specific columns from HBase table.

        :param table_name: Name of the table.
        :param row_key: Row key to delete.
        :param columns: List of columns to delete (if None, deletes entire row).
        """
        self._get_strategy().delete_row(table_name, row_key, columns)
        self.log.info("Deleted row %s from table %s", row_key, table_name)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """
        Get column families for a table.

        :param table_name: Name of the table.
        :return: Dictionary of column families and their properties.
        """
        return self._get_strategy().get_table_families(table_name)

    def get_openlineage_database_info(self, connection):
        """
        Return HBase specific information for OpenLineage.
        
        :param connection: HBase connection object.
        :return: DatabaseInfo object or None if OpenLineage not available.
        """
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
        """
        Return custom UI field behaviour for HBase connection.
        
        :return: Dictionary defining UI field behaviour.
        """
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

    def execute_hbase_command(self, command: str, **kwargs) -> str:
        """
        Execute HBase shell command.

        :param command: HBase command to execute (without 'hbase' prefix).
        :param kwargs: Additional arguments for subprocess.
        :return: Command output.
        """
        conn = self.get_connection(self.hbase_conn_id)
        ssh_conn_id = conn.extra_dejson.get("ssh_conn_id") if conn.extra_dejson else None
        if not ssh_conn_id:
            raise ValueError("SSH connection ID must be specified in extra parameters")

        full_command = f"hbase {command}"
        self.log.info("Executing HBase command: %s", full_command)

        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

        # Get hbase_home and java_home from SSH connection extra
        ssh_conn = ssh_hook.get_connection(ssh_conn_id)
        hbase_home = None
        java_home = None
        if ssh_conn.extra_dejson:
            hbase_home = ssh_conn.extra_dejson.get('hbase_home')
            java_home = ssh_conn.extra_dejson.get('java_home')

        if not java_home:
            raise ValueError(
                f"java_home must be specified in SSH connection '{ssh_conn_id}' extra parameters")

        # Use full path if hbase_home is provided
        if hbase_home:
            full_command = full_command.replace('hbase ', f'{hbase_home}/bin/hbase ')

        # Add JAVA_HOME export to command
        full_command = f"export JAVA_HOME={java_home} && {full_command}"

        self.log.info("Executing via SSH with Kerberos: %s", full_command)
        with ssh_hook.get_conn() as ssh_client:
            exit_status, stdout, stderr = ssh_hook.exec_ssh_client_command(
                ssh_client=ssh_client,
                command=full_command,
                get_pty=False,
                environment={"JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"}
            )
            if exit_status != 0:
                self.log.error("SSH command failed: %s", stderr.decode())
                raise RuntimeError(f"SSH command failed: {stderr.decode()}")
            return stdout.decode()

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """
        Create backup set.
        
        :param backup_set_name: Name of the backup set to create.
        :param tables: List of table names to include in the backup set.
        :return: Command output.
        """
        return self._get_strategy().create_backup_set(backup_set_name, tables)

    def list_backup_sets(self) -> str:
        """
        List backup sets.
        
        :return: Command output with list of backup sets.
        """
        return self._get_strategy().list_backup_sets()

    def create_full_backup(
        self,
        backup_path: str,
        tables: list[str] | None = None,
        backup_set_name: str | None = None,
        workers: int | None = None,
    ) -> str:
        """
        Create full backup.
        
        :param backup_path: Path where backup will be stored.
        :param tables: List of tables to backup (mutually exclusive with backup_set_name).
        :param backup_set_name: Name of backup set to use (mutually exclusive with tables).
        :param workers: Number of parallel workers.
        :return: Backup ID.
        """
        return self._get_strategy().create_full_backup(backup_path, backup_set_name, tables, workers)

    def create_incremental_backup(
        self,
        backup_path: str,
        tables: list[str] | None = None,
        backup_set_name: str | None = None,
        workers: int | None = None,
    ) -> str:
        """
        Create incremental backup.
        
        :param backup_path: Path where backup will be stored.
        :param tables: List of tables to backup (mutually exclusive with backup_set_name).
        :param backup_set_name: Name of backup set to use (mutually exclusive with tables).
        :param workers: Number of parallel workers.
        :return: Backup ID.
        """
        return self._get_strategy().create_incremental_backup(backup_path, backup_set_name, tables, workers)

    def get_backup_history(
        self,
        backup_set_name: str | None = None,
    ) -> str:
        """
        Get backup history.
        
        :param backup_set_name: Name of backup set to get history for.
        :return: Command output with backup history.
        """
        return self._get_strategy().get_backup_history(backup_set_name)

    def restore_backup(
        self,
        backup_path: str,
        backup_id: str,
        tables: list[str] | None = None,
        overwrite: bool = False,
    ) -> str:
        """
        Restore backup.
        
        :param backup_path: Path where backup is stored.
        :param backup_id: Backup ID to restore.
        :param tables: List of tables to restore (optional).
        :param overwrite: Whether to overwrite existing tables.
        :return: Command output.
        """
        return self._get_strategy().restore_backup(backup_path, backup_id, tables, overwrite)

    def describe_backup(self, backup_id: str) -> str:
        """
        Describe backup.
        
        :param backup_id: ID of the backup to describe.
        :return: Command output.
        """
        return self._get_strategy().describe_backup(backup_id)

    def delete_backup_set(self, backup_set_name: str) -> str:
        """
        Delete HBase backup set.

        :param backup_set_name: Name of the backup set to delete.
        :return: Command output.
        """
        command = f"backup set remove {backup_set_name}"
        return self.execute_hbase_command(command)

    def delete_backup(
        self,
        backup_path: str,
        backup_ids: list[str],
    ) -> str:
        """
        Delete HBase backup.

        :param backup_path: Path where backup is stored.
        :param backup_ids: List of backup IDs to delete.
        :return: Command output.
        """
        backup_ids_str = ",".join(backup_ids)
        command = f"backup delete {backup_path} {backup_ids_str}"
        return self.execute_hbase_command(command)

    def merge_backups(
        self,
        backup_path: str,
        backup_ids: list[str],
    ) -> str:
        """
        Merge HBase backups.

        :param backup_path: Path where backups are stored.
        :param backup_ids: List of backup IDs to merge.
        :return: Command output.
        """
        backup_ids_str = ",".join(backup_ids)
        command = f"backup merge {backup_path} {backup_ids_str}"
        return self.execute_hbase_command(command)

    def close(self) -> None:
        """Close HBase connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
