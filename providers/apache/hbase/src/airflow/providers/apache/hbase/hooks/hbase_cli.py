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
import os
import shlex
import subprocess

from airflow.providers.apache.hbase.version_compat import BaseHook, Connection

logger = logging.getLogger(__name__)


class HBaseCLIHook(BaseHook):
    """
    Hook for HBase administrative operations (backup/restore).

    This hook executes HBase CLI commands for backup/restore operations.

    **Requirements:**

    - HBase CLI tools must be installed on the Airflow worker

    :param hbase_conn_id: Connection ID for HBase.
    :param hbase_cmd: HBase command name
        (default: 'hbase' or HBASE_CMD env var)
    :param java_home: Java home directory
        (default: JAVA_HOME env var or '/usr/lib/jvm/java-8-openjdk-amd64')
    :param hbase_home: HBase installation directory
        (default: HBASE_HOME env var or '/usr/lib/hbase')
    """

    conn_name_attr = "hbase_conn_id"
    default_conn_name = "hbase_default"
    conn_type = "hbase"
    hook_name = "HBase Administration"

    def __init__(
        self,
        hbase_conn_id: str = default_conn_name,
        hbase_cmd: str | None = None,
        java_home: str | None = None,
        hbase_home: str | None = None,
    ) -> None:
        super().__init__()
        self.hbase_conn_id = hbase_conn_id
        self.hbase_cmd = hbase_cmd or os.getenv("HBASE_CMD", "hbase")
        self.java_home: str = java_home or os.getenv("JAVA_HOME") or "/usr/lib/jvm/java-8-openjdk-amd64"
        self.hbase_home: str = hbase_home or os.getenv("HBASE_HOME") or "/usr/lib/hbase"
        self._connection: Connection | None = None
        self._process: subprocess.Popen | None = None

    def get_conn(self) -> Connection:
        """Get HBase connection."""
        if self._connection is None:
            self._connection = self.get_connection(self.hbase_conn_id)
        return self._connection

    def _build_hbase_command(self, args: list[str]) -> list[str]:
        """
        Build HBase command as list.

        :param args: HBase command arguments
        :return: Full command as list
        """
        hbase_bin = f"{self.hbase_home}/bin/{self.hbase_cmd}"
        return [hbase_bin, *args]

    def _prepare_command(self, args: list[str]) -> tuple[list[str], dict[str, str]]:
        """
        Prepare full command and environment for execution.

        :param args: HBase command arguments as list
        :return: Tuple of (full_command, env)
        """
        conn = self.get_conn()
        extra = conn.extra_dejson if conn.extra else {}

        java_home = extra.get("java_home") or self.java_home
        hbase_home = extra.get("hbase_home") or self.hbase_home
        hbase_bin = f"{hbase_home}/bin/{self.hbase_cmd}"
        full_command = [hbase_bin, *args]

        env = os.environ.copy()
        env["JAVA_HOME"] = java_home
        env["HBASE_HOME"] = hbase_home
        env["HBASE_OPTS"] = env.get("HBASE_OPTS", "") + " -Dslf4j.internal.verbosity=warn"

        logger.info("Executing HBase command: %s", " ".join(full_command))

        return full_command, env

    def _execute_hbase_command(self, args: list[str]) -> str:
        """
        Execute HBase CLI command.

        :param args: HBase command arguments as list
        :return: Command output
        """
        full_command, env = self._prepare_command(args)

        try:
            result = subprocess.run(full_command, capture_output=True, text=True, check=True, env=env)
            logger.info("Command completed successfully")
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip() or e.stdout.strip() or "Unknown error"
            logger.error("Command failed with exit code %d", e.returncode)
            logger.error("Error output: %s", error_msg)
            raise RuntimeError(f"HBase command failed (exit code {e.returncode}): {error_msg}") from e
        except OSError as e:
            raise RuntimeError(f"Failed to execute HBase command: {e}") from e

    def _execute_hbase_command_stream(self, args: list[str]) -> str:
        """
        Execute long-running HBase CLI command with real-time log streaming.

        Uses Popen for backup/restore operations that may run for hours.

        :param args: HBase command arguments as list
        :return: Command output
        """
        full_command, env = self._prepare_command(args)

        try:
            self._process = subprocess.Popen(
                full_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1,
                universal_newlines=True,
                env=env,
            )
        except OSError as e:
            raise RuntimeError(f"Failed to execute HBase command: {e}") from e

        output_lines: list[str] = []
        process = self._process
        if process is None or process.stdout is None:
            raise RuntimeError("HBase CLI process is not running")
        for line in iter(process.stdout.readline, ""):
            stripped_line = line.strip()
            if stripped_line:
                logger.info(stripped_line)
                output_lines.append(stripped_line)

        returncode = self._process.wait()
        self._process = None

        if returncode:
            raise RuntimeError(
                f"HBase command failed (exit code {returncode}): {' '.join(output_lines[-5:])}"
            )

        logger.info("Command completed successfully")
        return "\n".join(output_lines)

    def on_kill(self) -> None:
        """Kill running HBase process."""
        if self._process and self._process.poll() is None:
            logger.info("Killing HBase process")
            self._process.kill()

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """
        Create backup set.

        :param backup_set_name: Name of the backup set.
        :param tables: List of tables to include in backup set.
        :return: Result message.
        """
        cmd = ["backup", "set", "add", backup_set_name, ",".join(tables)]
        return self._execute_hbase_command(cmd)

    def list_backup_sets(self) -> str:
        """
        List all backup sets.

        :return: List of backup sets.
        """
        cmd = ["backup", "set", "list"]
        return self._execute_hbase_command(cmd)

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
        cmd = ["backup", "create", "full", backup_root]

        if backup_set_name:
            cmd += ["-s", backup_set_name]
        elif tables:
            cmd += ["-t", ",".join(tables)]
        else:
            raise ValueError("Either backup_set_name or tables must be provided")

        if workers:
            cmd += ["-w", str(workers)]

        return self._execute_hbase_command_stream(cmd)

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
        cmd = ["backup", "create", "incremental", backup_root]

        if backup_set_name:
            cmd += ["-s", backup_set_name]
        elif tables:
            cmd += ["-t", ",".join(tables)]
        else:
            raise ValueError("Either backup_set_name or tables must be provided")

        if workers:
            cmd += ["-w", str(workers)]

        return self._execute_hbase_command_stream(cmd)

    def get_backup_history(
        self,
        backup_set_name: str | None = None,
        backup_path: str | None = None,
    ) -> str:
        """
        Get backup history.

        :param backup_set_name: Name of backup set (optional).
        :param backup_path: HDFS backup root path to filter history (optional).
        :return: Backup history.
        """
        cmd = ["backup", "history"]
        if backup_set_name:
            cmd += ["-s", backup_set_name]
        if backup_path:
            cmd += ["-p", backup_path]
        return self._execute_hbase_command(cmd)

    def describe_backup(self, backup_id: str) -> str:
        """
        Describe backup.

        :param backup_id: Backup ID.
        :return: Backup description.
        """
        cmd = ["backup", "describe", backup_id]
        return self._execute_hbase_command(cmd)

    def restore_backup(
        self,
        backup_root: str,
        backup_id: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        overwrite: bool = False,
    ) -> str:
        """
        Restore backup.

        :param backup_root: Root directory where backup is stored.
        :param backup_id: Backup ID to restore.
        :param backup_set_name: Name of backup set to restore (optional).
        :param tables: List of tables to restore (optional, alternative to backup_set_name).
        :param overwrite: Whether to overwrite existing tables.
        :return: Result message.
        """
        cmd = ["restore", backup_root, backup_id]

        if backup_set_name:
            cmd += ["-s", backup_set_name]
        elif tables:
            cmd += ["-t", ",".join(tables)]

        if overwrite:
            cmd.append("-o")

        return self._execute_hbase_command_stream(cmd)

    def execute_command(self, command: str | list[str]) -> str:
        """
        Execute arbitrary HBase CLI command.

        This method allows executing any HBase CLI command that is not wrapped
        in a specialized method.

        :param command: HBase command to execute. Can be either:
            - String: "backup create full /backup -t table1" (will be parsed)
            - List: ["backup", "create", "full", "/backup", "-t", "table1"] (used directly)
        :return: Command output
        """
        if isinstance(command, str):
            args = shlex.split(command)
        else:
            args = command

        if not args:
            raise ValueError("Command must not be empty")

        return self._execute_hbase_command(args)
