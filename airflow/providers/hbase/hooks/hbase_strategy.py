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

import time
import concurrent.futures
from abc import ABC, abstractmethod
from typing import Any

import happybase

from airflow.providers.ssh.hooks.ssh import SSHHook


class HBaseStrategy(ABC):
    """Abstract base class for HBase connection strategies."""

    @staticmethod
    def _create_chunks(rows: list, chunk_size: int) -> list[list]:
        """Split rows into chunks of specified size."""
        if not rows:
            return []
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        return [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

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
    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 200, max_workers: int = 4) -> None:
        """Insert multiple rows in batch with chunking and parallel processing."""
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

    @abstractmethod
    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set."""
        pass

    @abstractmethod
    def list_backup_sets(self) -> str:
        """List backup sets."""
        pass

    @abstractmethod
    def create_full_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create full backup."""
        pass

    @abstractmethod
    def create_incremental_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create incremental backup."""
        pass

    @abstractmethod
    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history."""
        pass

    @abstractmethod
    def describe_backup(self, backup_id: str) -> str:
        """Describe backup."""
        pass

    @abstractmethod
    def restore_backup(self, backup_root: str, backup_id: str, tables: list[str] | None = None, overwrite: bool = False) -> str:
        """Restore backup."""
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

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 200, max_workers: int = 1) -> None:
        """Insert multiple rows via Thrift with chunking (single-threaded only)."""

        # Single-threaded processing for ThriftStrategy
        data_size = sum(len(str(row)) for row in rows)
        self.log.info(f"Processing {len(rows)} rows, ~{data_size} bytes (single-threaded)")

        try:
            table = self.connection.table(table_name)
            with table.batch(batch_size=batch_size) as batch:
                for row in rows:
                    if 'row_key' in row:
                        row_key = row.get('row_key')
                        row_data = {k: v for k, v in row.items() if k != 'row_key'}
                        batch.put(row_key, row_data)

            # Small backpressure
            time.sleep(0.05)

        except Exception as e:
            self.log.error(f"Batch processing failed: {e}")
            raise

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

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set - not supported in Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def list_backup_sets(self) -> str:
        """List backup sets - not supported in Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_full_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create full backup - not supported in Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_incremental_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create incremental backup - not supported in Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history - not supported in Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def describe_backup(self, backup_id: str) -> str:
        """Describe backup - not supported in Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def restore_backup(self, backup_root: str, backup_id: str, tables: list[str] | None = None, overwrite: bool = False) -> str:
        """Restore backup - not supported in Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")


class PooledThriftStrategy(HBaseStrategy):
    """HBase strategy using connection pool."""

    def __init__(self, pool, logger):
        self.pool = pool
        self.log = logger

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists via pooled connection."""
        with self.pool.connection() as connection:
            return table_name.encode() in connection.tables()

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table via pooled connection."""
        with self.pool.connection() as connection:
            connection.create_table(table_name, families)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """Delete table via pooled connection."""
        with self.pool.connection() as connection:
            if disable:
                connection.disable_table(table_name)
            connection.delete_table(table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row via pooled connection."""
        with self.pool.connection() as connection:
            table = connection.table(table_name)
            table.put(row_key, data)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row via pooled connection."""
        with self.pool.connection() as connection:
            table = connection.table(table_name)
            return table.row(row_key, columns=columns)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row via pooled connection."""
        with self.pool.connection() as connection:
            table = connection.table(table_name)
            table.delete(row_key, columns=columns)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """Get column families via pooled connection."""
        with self.pool.connection() as connection:
            table = connection.table(table_name)
            return table.families()

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows via pooled connection."""
        with self.pool.connection() as connection:
            table = connection.table(table_name)
            return [dict(data) for key, data in table.rows(row_keys, columns=columns)]

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 200, max_workers: int = 4) -> None:
        """Insert multiple rows via pooled connection with chunking and parallel processing."""

        # Ensure pool size is adequate for parallel processing
        if hasattr(self.pool, '_size') and self.pool._size < max_workers:
            self.log.warning(f"Pool size ({self.pool._size}) < max_workers ({max_workers}). Consider increasing pool size.")

        def process_chunk(chunk):
            """Process a single chunk of rows using pooled connection."""
            # Calculate data size for monitoring
            data_size = sum(len(str(row)) for row in chunk)
            self.log.info(f"Processing chunk: {len(chunk)} rows, ~{data_size} bytes")

            try:
                with self.pool.connection() as connection:  # Get dedicated connection from pool
                    table = connection.table(table_name)
                    with table.batch(batch_size=batch_size) as batch:
                        for row in chunk:
                            if 'row_key' in row:
                                row_key = row.get('row_key')
                                row_data = {k: v for k, v in row.items() if k != 'row_key'}
                                batch.put(row_key, row_data)

                # Backpressure: small pause between chunks
                time.sleep(0.1)

            except Exception as e:
                self.log.error(f"Chunk processing failed: {e}")
                raise

        # Split rows into chunks for parallel processing
        chunk_size = max(1, len(rows) // max_workers)
        chunks = self._create_chunks(rows, chunk_size)

        self.log.info(f"Processing {len(rows)} rows in {len(chunks)} chunks with {max_workers} workers")

        # Process chunks in parallel using connection pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
            for future in futures:
                future.result()  # Propagate exceptions

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table via pooled connection."""
        with self.pool.connection() as connection:
            table = connection.table(table_name)
            return list(table.scan(
                row_start=row_start,
                row_stop=row_stop,
                columns=columns,
                limit=limit
            ))

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set - not supported in pooled Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def list_backup_sets(self) -> str:
        """List backup sets - not supported in pooled Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_full_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create full backup - not supported in pooled Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_incremental_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create incremental backup - not supported in pooled Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history - not supported in pooled Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def describe_backup(self, backup_id: str) -> str:
        """Describe backup - not supported in pooled Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def restore_backup(self, backup_root: str, backup_id: str, tables: list[str] | None = None, overwrite: bool = False) -> str:
        """Restore backup - not supported in pooled Thrift mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")


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
        # Mask sensitive data in command logging
        safe_command = self._mask_sensitive_command_parts(full_command)
        self.log.info("Executing HBase command: %s", safe_command)

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

        # Log safe version of final command
        safe_final_command = self._mask_sensitive_command_parts(full_command)
        self.log.info("Executing via SSH: %s", safe_final_command)
        with SSHHook(ssh_conn_id=ssh_conn_id).get_conn() as ssh_client:
            exit_status, stdout, stderr = SSHHook(ssh_conn_id=ssh_conn_id).exec_ssh_client_command(
                ssh_client=ssh_client,
                command=full_command,
                get_pty=False,
                environment={"JAVA_HOME": java_home}
            )
            if exit_status != 0:
                # Mask sensitive data in error messages
                safe_stderr = self._mask_sensitive_data_in_output(stderr.decode())
                self.log.error("SSH command failed: %s", safe_stderr)
                raise RuntimeError(f"SSH command failed: {safe_stderr}")
            return stdout.decode()

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists via SSH."""
        try:
            result = self._execute_hbase_command(f"shell <<< \"list\"")
            # Parse table list more carefully - look for exact table name
            lines = result.split('\n')
            for line in lines:
                if line.strip() == table_name:
                    return True
            return False
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
        self._execute_hbase_command(f"shell <<< \"{command}\"")
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
        self._execute_hbase_command(f"shell <<< \"{command}\"")
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

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 1000, max_workers: int = 1) -> None:
        """Insert multiple rows via SSH with chunking."""
        # SSH strategy processes sequentially due to shell limitations
        chunks = self._create_chunks(rows, batch_size)

        for chunk in chunks:
            puts = []
            for row in chunk:
                if 'row_key' in row:
                    row_key = row.get('row_key')
                    row_data = {k: v for k, v in row.items() if k != 'row_key'}
                    for col, val in row_data.items():
                        puts.append(f"put '{table_name}', '{row_key}', '{col}', '{val}'")

            if puts:
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

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set via SSH."""
        tables_str = ",".join(tables)
        command = f"backup set add {backup_set_name} {tables_str}"
        return self._execute_hbase_command(command)

    def list_backup_sets(self) -> str:
        """List backup sets via SSH."""
        command = "backup set list"
        return self._execute_hbase_command(command)

    def create_full_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create full backup via SSH."""
        command = f"backup create full {backup_root}"

        if backup_set_name:
            command += f" -s {backup_set_name}"
        elif tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"

        if workers:
            command += f" -w {workers}"

        return self._execute_hbase_command(command)

    def create_incremental_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create incremental backup via SSH."""
        command = f"backup create incremental {backup_root}"

        if backup_set_name:
            command += f" -s {backup_set_name}"
        elif tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"

        if workers:
            command += f" -w {workers}"

        return self._execute_hbase_command(command)

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history via SSH."""
        command = "backup history"
        if backup_set_name:
            command += f" -s {backup_set_name}"
        return self._execute_hbase_command(command)

    def describe_backup(self, backup_id: str) -> str:
        """Describe backup via SSH."""
        command = f"backup describe {backup_id}"
        return self._execute_hbase_command(command)

    def restore_backup(self, backup_root: str, backup_id: str, tables: list[str] | None = None, overwrite: bool = False) -> str:
        """Restore backup via SSH."""
        command = f"restore {backup_root} {backup_id}"

        if tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"

        if overwrite:
            command += " -o"

        return self._execute_hbase_command(command)

    def _mask_sensitive_command_parts(self, command: str) -> str:
        """
        Mask sensitive parts in HBase commands for logging.

        :param command: Original command string.
        :return: Command with sensitive parts masked.
        """
        import re

        # Mask potential keytab paths
        command = re.sub(r'(/[\w/.-]*\.keytab)', '***KEYTAB_PATH***', command)

        # Mask potential passwords in commands
        command = re.sub(r'(password[=:]\s*[^\s]+)', 'password=***MASKED***', command, flags=re.IGNORECASE)

        # Mask potential tokens
        command = re.sub(r'(token[=:]\s*[^\s]+)', 'token=***MASKED***', command, flags=re.IGNORECASE)

        # Mask JAVA_HOME paths that might contain sensitive info
        command = re.sub(r'(JAVA_HOME=[^\s]+)', 'JAVA_HOME=***MASKED***', command)

        return command

    def _mask_sensitive_data_in_output(self, output: str) -> str:
        """
        Mask sensitive data in command output for logging.

        :param output: Original output string.
        :return: Output with sensitive data masked.
        """
        import re

        # Mask potential file paths that might contain sensitive info
        output = re.sub(r'(/[\w/.-]*\.keytab)', '***KEYTAB_PATH***', output)

        # Mask potential passwords
        output = re.sub(r'(password[=:]\s*[^\s]+)', 'password=***MASKED***', output, flags=re.IGNORECASE)

        # Mask potential authentication tokens
        output = re.sub(r'(token[=:]\s*[^\s]+)', 'token=***MASKED***', output, flags=re.IGNORECASE)

        return output
