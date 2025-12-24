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
"""HBase connection strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import happybase

from airflow.providers.ssh.hooks.ssh import SSHHook


class HBaseStrategy(ABC):
    """Abstract base class for HBase connection strategies."""

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        pass

    @abstractmethod
    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table."""
        pass

    @abstractmethod
    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """Delete table."""
        pass

    @abstractmethod
    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row data."""
        pass

    @abstractmethod
    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row data."""
        pass

    @abstractmethod
    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row or specific columns."""
        pass

    @abstractmethod
    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """Get column families for a table."""
        pass

    @abstractmethod
    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows in batch."""
        pass

    @abstractmethod
    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Insert multiple rows in batch."""
        pass

    @abstractmethod
    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table."""
        pass


class ThriftStrategy(HBaseStrategy):
    """HBase strategy using Thrift protocol."""

    def __init__(self, connection: happybase.Connection, logger):
        self.connection = connection
        self.log = logger

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists via Thrift."""
        return table_name.encode() in self.connection.tables()

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table via Thrift."""
        self.connection.create_table(table_name, families)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """Delete table via Thrift."""
        if disable:
            self.connection.disable_table(table_name)
        self.connection.delete_table(table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row via Thrift."""
        table = self.connection.table(table_name)
        table.put(row_key, data)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row via Thrift."""
        table = self.connection.table(table_name)
        return table.row(row_key, columns=columns)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row via Thrift."""
        table = self.connection.table(table_name)
        table.delete(row_key, columns=columns)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """Get column families via Thrift."""
        table = self.connection.table(table_name)
        return table.families()

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows via Thrift."""
        table = self.connection.table(table_name)
        return [dict(data) for key, data in table.rows(row_keys, columns=columns)]

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Insert multiple rows via Thrift."""
        table = self.connection.table(table_name)
        with table.batch() as batch:
            for row in rows:
                row_key = row.pop('row_key')
                batch.put(row_key, row)

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table via Thrift."""
        table = self.connection.table(table_name)
        return list(table.scan(
            row_start=row_start,
            row_stop=row_stop,
            columns=columns,
            limit=limit
        ))


class SSHStrategy(HBaseStrategy):
    """HBase strategy using SSH + HBase shell commands."""

    def __init__(self, hbase_conn_id: str, ssh_hook: SSHHook, logger):
        self.hbase_conn_id = hbase_conn_id
        self.ssh_hook = ssh_hook
        self.log = logger

    def _execute_hbase_command(self, command: str) -> str:
        """Execute HBase shell command via SSH."""
        from airflow.hooks.base import BaseHook
        
        conn = BaseHook.get_connection(self.hbase_conn_id)
        ssh_conn_id = conn.extra_dejson.get("ssh_conn_id") if conn.extra_dejson else None
        if not ssh_conn_id:
            raise ValueError("SSH connection ID must be specified in extra parameters")

        full_command = f"hbase {command}"
        self.log.info("Executing HBase command: %s", full_command)

        # Get hbase_home and java_home from SSH connection extra
        ssh_conn = self.ssh_hook.get_connection(ssh_conn_id)
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
        with SSHHook(ssh_conn_id=ssh_conn_id).get_conn() as ssh_client:
            exit_status, stdout, stderr = SSHHook(ssh_conn_id=ssh_conn_id).exec_ssh_client_command(
                ssh_client=ssh_client,
                command=full_command,
                get_pty=False,
                environment={"JAVA_HOME": java_home}
            )
            if exit_status != 0:
                self.log.error("SSH command failed: %s", stderr.decode())
                raise RuntimeError(f"SSH command failed: {stderr.decode()}")
            return stdout.decode()

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists via SSH."""
        try:
            result = self._execute_hbase_command(f"shell <<< \"list\"")
            return table_name in result
        except Exception:
            return False

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table via SSH."""
        families_str = ", ".join([f"'{name}'" for name in families.keys()])
        command = f"create '{table_name}', {families_str}"
        self._execute_hbase_command(f"shell <<< \"{command}\"")

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """Delete table via SSH."""
        if disable:
            self._execute_hbase_command(f"shell <<< \"disable '{table_name}'\"")
        self._execute_hbase_command(f"shell <<< \"drop '{table_name}'\"")

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row via SSH."""
        puts = []
        for col, val in data.items():
            puts.append(f"put '{table_name}', '{row_key}', '{col}', '{val}'")
        command = "; ".join(puts)
        self._execute_hbase_command(f"shell <<< \"{command}\"")

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row via SSH."""
        command = f"get '{table_name}', '{row_key}'"
        if columns:
            cols_str = "', '".join(columns)
            command = f"get '{table_name}', '{row_key}', '{cols_str}'"
        result = self._execute_hbase_command(f"shell <<< \"{command}\"")
        # TODO: Parse result - this is a simplified implementation
        return {}

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row via SSH."""
        if columns:
            cols_str = "', '".join(columns)
            command = f"delete '{table_name}', '{row_key}', '{cols_str}'"
        else:
            command = f"deleteall '{table_name}', '{row_key}'"
        self._execute_hbase_command(f"shell <<< \"{command}\"")

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """Get column families via SSH."""
        command = f"describe '{table_name}'"
        result = self._execute_hbase_command(f"shell <<< \"{command}\"")
        # TODO: Parse result - this is a simplified implementation
        # For now return empty dict, should parse HBase describe output
        return {}

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows via SSH."""
        results = []
        for row_key in row_keys:
            row_data = self.get_row(table_name, row_key, columns)
            results.append(row_data)
        return results

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Insert multiple rows via SSH."""
        puts = []
        for row in rows:
            row_key = row.pop('row_key')
            for col, val in row.items():
                puts.append(f"put '{table_name}', '{row_key}', '{col}', '{val}'")
        command = "; ".join(puts)
        self._execute_hbase_command(f"shell <<< \"{command}\"")

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table via SSH."""
        command = f"scan '{table_name}'"
        if limit:
            command += f", {{LIMIT => {limit}}}"
        result = self._execute_hbase_command(f"shell <<< \"{command}\"")
        # TODO: Parse result - this is a simplified implementation
        return []