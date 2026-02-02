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
"""HBase Administration Hook for backup/restore operations."""

from __future__ import annotations

import logging
import re
import subprocess
from typing import TYPE_CHECKING

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models import Connection

logger = logging.getLogger(__name__)


class HBaseAdministrationHook(BaseHook):
    """
    Hook for HBase administrative operations (backup/restore).
    
    This hook will use HBase Admin client API for backup/restore operations.
    Currently not implemented - placeholder for future implementation.
    
    :param hbase_conn_id: Connection ID for HBase.
    """

    conn_name_attr = "hbase_conn_id"
    default_conn_name = "hbase_default"
    conn_type = "hbase"
    hook_name = "HBase Administration"

    def __init__(self, hbase_conn_id: str = default_conn_name, hbase_cmd: str = "hbase") -> None:
        super().__init__()
        self.hbase_conn_id = hbase_conn_id
        self.hbase_cmd = hbase_cmd
        self._connection: Connection | None = None

    def get_conn(self) -> Connection:
        """Get HBase connection."""
        if self._connection is None:
            self._connection = self.get_connection(self.hbase_conn_id)
        return self._connection

    def _execute_hbase_command(self, command: str) -> str:
        """Execute HBase CLI command.
        
        :param command: HBase command to execute (e.g., "backup create full /backup -t table1")
        :return: Command output
        """
        conn = self.get_conn()
        
        # Get JAVA_HOME from connection extra if provided
        java_home = None
        if conn.extra_dejson:
            java_home = conn.extra_dejson.get('java_home')
            hbase_home = conn.extra_dejson.get('hbase_home')
            if hbase_home:
                self.hbase_cmd = f"{hbase_home}/bin/hbase"
        
        full_command = f"{self.hbase_cmd} {command}"
        
        env = None
        if java_home:
            import os
            env = os.environ.copy()
            env['JAVA_HOME'] = java_home
        
        logger.info(f"Executing HBase command: {self._mask_sensitive(full_command)}")
        
        try:
            result = subprocess.run(
                full_command,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
                env=env
            )
            logger.info(f"Command completed successfully")
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with exit code {e.returncode}: {e.stderr}")
            raise RuntimeError(f"HBase command failed: {e.stderr}") from e
    
    def _mask_sensitive(self, text: str) -> str:
        """Mask sensitive information in logs."""
        # Mask potential paths that might contain sensitive info
        text = re.sub(r'(/[\w/.-]*\.keytab)', '***KEYTAB***', text)
        return text

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """
        Create backup set.
        
        :param backup_set_name: Name of the backup set.
        :param tables: List of tables to include in backup set.
        :return: Result message.
        """
        tables_str = ",".join(tables)
        command = f"backup set add {backup_set_name} {tables_str}"
        return self._execute_hbase_command(command)

    def list_backup_sets(self) -> str:
        """
        List all backup sets.
        
        :return: List of backup sets.
        """
        command = "backup set list"
        return self._execute_hbase_command(command)

    def create_full_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """
        Create full backup.
        
        :param backup_root: Root directory for backup.
        :param backup_set_name: Name of backup set to backup.
        :param tables: List of tables to backup (alternative to backup_set_name).
        :param workers: Number of workers for backup operation.
        :return: Backup ID.
        """
        command = f"backup create full {backup_root}"
        
        if backup_set_name:
            command += f" -s {backup_set_name}"
        elif tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"
        else:
            raise ValueError("Either backup_set_name or tables must be provided")
        
        if workers:
            command += f" -w {workers}"
        
        return self._execute_hbase_command(command)

    def create_incremental_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """
        Create incremental backup.
        
        :param backup_root: Root directory for backup.
        :param backup_set_name: Name of backup set to backup.
        :param tables: List of tables to backup (alternative to backup_set_name).
        :param workers: Number of workers for backup operation.
        :return: Backup ID.
        """
        command = f"backup create incremental {backup_root}"
        
        if backup_set_name:
            command += f" -s {backup_set_name}"
        elif tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"
        else:
            raise ValueError("Either backup_set_name or tables must be provided")
        
        if workers:
            command += f" -w {workers}"
        
        return self._execute_hbase_command(command)

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """
        Get backup history.
        
        :param backup_set_name: Name of backup set (optional).
        :return: Backup history.
        """
        command = "backup history"
        if backup_set_name:
            command += f" -s {backup_set_name}"
        return self._execute_hbase_command(command)

    def describe_backup(self, backup_id: str) -> str:
        """
        Describe backup.
        
        :param backup_id: Backup ID.
        :return: Backup description.
        """
        command = f"backup describe {backup_id}"
        return self._execute_hbase_command(command)

    def restore_backup(
        self,
        backup_root: str,
        backup_id: str,
        tables: list[str] | None = None,
        overwrite: bool = False,
    ) -> str:
        """
        Restore backup.
        
        :param backup_root: Root directory where backup is stored.
        :param backup_id: Backup ID to restore.
        :param tables: List of tables to restore (optional).
        :param overwrite: Whether to overwrite existing tables.
        :return: Result message.
        """
        command = f"restore {backup_root} {backup_id}"
        
        if tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"
        
        if overwrite:
            command += " -o"
        
        return self._execute_hbase_command(command)
