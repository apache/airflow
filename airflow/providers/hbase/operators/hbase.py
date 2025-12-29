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
"""HBase operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.hbase.hooks.hbase import HBaseHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HBasePutOperator(BaseOperator):
    """
    Operator to put data into HBase table.
    
    :param table_name: Name of the HBase table.
    :param row_key: Row key for the data.
    :param data: Dictionary of column:value pairs to insert.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "row_key", "data")

    def __init__(
        self,
        table_name: str,
        row_key: str,
        data: dict[str, Any],
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.row_key = row_key
        self.data = data
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        hook.put_row(self.table_name, self.row_key, self.data)


class HBaseCreateTableOperator(BaseOperator):
    """
    Operator to create HBase table.
    
    :param table_name: Name of the table to create.
    :param families: Dictionary of column families and their configuration.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "families")

    def __init__(
        self,
        table_name: str,
        families: dict[str, dict],
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.families = families
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        if not hook.table_exists(self.table_name):
            hook.create_table(self.table_name, self.families)
        else:
            self.log.info("Table %s already exists", self.table_name)


class HBaseDeleteTableOperator(BaseOperator):
    """
    Operator to delete HBase table.
    
    :param table_name: Name of the table to delete.
    :param disable: Whether to disable table before deletion.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name",)

    def __init__(
        self,
        table_name: str,
        disable: bool = True,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.disable = disable
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        if hook.table_exists(self.table_name):
            hook.delete_table(self.table_name, self.disable)
        else:
            self.log.info("Table %s does not exist", self.table_name)


class HBaseScanOperator(BaseOperator):
    """
    Operator to scan HBase table.
    
    :param table_name: Name of the table to scan.
    :param row_start: Start row key for scan.
    :param row_stop: Stop row key for scan.
    :param columns: List of columns to retrieve.
    :param limit: Maximum number of rows to return.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "row_start", "row_stop", "columns")

    def __init__(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.row_start = row_start
        self.row_stop = row_stop
        self.columns = columns
        self.limit = limit
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> list:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        results = hook.scan_table(
            table_name=self.table_name,
            row_start=self.row_start,
            row_stop=self.row_stop,
            columns=self.columns,
            limit=self.limit
        )
        # Convert bytes to strings for JSON serialization
        serializable_results = []
        for row_key, data in results:
            row_dict = {"row_key": row_key.decode('utf-8') if isinstance(row_key, bytes) else row_key}
            for col, val in data.items():
                col_str = col.decode('utf-8') if isinstance(col, bytes) else col
                val_str = val.decode('utf-8') if isinstance(val, bytes) else val
                row_dict[col_str] = val_str
            serializable_results.append(row_dict)
        return serializable_results


class HBaseBatchPutOperator(BaseOperator):
    """
    Operator to insert multiple rows into HBase table in batch with optimization.
    
    :param table_name: Name of the table.
    :param rows: List of dictionaries with 'row_key' and data columns.
    :param batch_size: Number of rows per batch chunk (default: 1000).
    :param max_workers: Number of parallel workers (default: 4).
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "rows")

    def __init__(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        batch_size: int = 1000,
        max_workers: int = 4,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.rows = rows
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        hook.batch_put_rows(self.table_name, self.rows, self.batch_size, self.max_workers)


class HBaseBatchGetOperator(BaseOperator):
    """
    Operator to get multiple rows from HBase table in batch.
    
    :param table_name: Name of the table.
    :param row_keys: List of row keys to retrieve.
    :param columns: List of columns to retrieve.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "row_keys", "columns")

    def __init__(
        self,
        table_name: str,
        row_keys: list[str],
        columns: list[str] | None = None,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.row_keys = row_keys
        self.columns = columns
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> list:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        results = hook.batch_get_rows(self.table_name, self.row_keys, self.columns)
        # Convert bytes to strings for JSON serialization
        serializable_results = []
        for data in results:
            row_dict = {}
            for col, val in data.items():
                col_str = col.decode('utf-8') if isinstance(col, bytes) else col
                val_str = val.decode('utf-8') if isinstance(val, bytes) else val
                row_dict[col_str] = val_str
            serializable_results.append(row_dict)
        return serializable_results


class HBaseBackupSetOperator(BaseOperator):
    """
    Operator to manage HBase backup sets.
    
    :param action: Action to perform (add, list, describe, delete).
    :param backup_set_name: Name of the backup set.
    :param tables: List of tables to add to backup set (for 'add' action).
    :param hbase_conn_id: The connection ID to use for HBase connection.
    :param ssh_conn_id: SSH connection ID for remote execution.
    """

    template_fields: Sequence[str] = ("backup_set_name", "tables")

    def __init__(
        self,
        action: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        ssh_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.action = action
        self.backup_set_name = backup_set_name
        self.tables = tables or []
        self.hbase_conn_id = hbase_conn_id
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        
        if self.action == "add":
            if not self.backup_set_name or not self.tables:
                raise ValueError("backup_set_name and tables are required for 'add' action")
            tables_str = " ".join(self.tables)
            command = f"backup set add {self.backup_set_name} {tables_str}"
        elif self.action == "list":
            command = "backup set list"
        elif self.action == "describe":
            if not self.backup_set_name:
                raise ValueError("backup_set_name is required for 'describe' action")
            command = f"backup set describe {self.backup_set_name}"
        elif self.action == "delete":
            if not self.backup_set_name:
                raise ValueError("backup_set_name is required for 'delete' action")
            command = f"backup set delete {self.backup_set_name}"
        else:
            raise ValueError(f"Unsupported action: {self.action}")
        
        return hook.execute_hbase_command(command, ssh_conn_id=self.ssh_conn_id)


class HBaseCreateBackupOperator(BaseOperator):
    """
    Operator to create HBase backup.
    
    :param backup_type: Type of backup ('full' or 'incremental').
    :param backup_path: HDFS path where backup will be stored.
    :param backup_set_name: Name of the backup set to backup.
    :param tables: List of tables to backup (alternative to backup_set_name).
    :param workers: Number of workers for backup operation.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    :param ssh_conn_id: SSH connection ID for remote execution.
    """

    template_fields: Sequence[str] = ("backup_path", "backup_set_name", "tables")

    def __init__(
        self,
        backup_type: str,
        backup_path: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int = 3,
        ignore_checksum: bool = False,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        ssh_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_type = backup_type
        self.backup_path = backup_path
        self.backup_set_name = backup_set_name
        self.tables = tables
        self.workers = workers
        self.ignore_checksum = ignore_checksum
        self.hbase_conn_id = hbase_conn_id
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        
        if hook.is_standalone_mode():
            raise ValueError(
                "HBase backup is not supported in standalone mode. "
                "Please configure HDFS for distributed mode."
            )
        
        if self.backup_type not in ["full", "incremental"]:
            raise ValueError("backup_type must be 'full' or 'incremental'")
        
        # Validate and adjust backup path based on HBase configuration
        validated_path = hook.validate_backup_path(self.backup_path)
        self.log.info("Using backup path: %s (original: %s)", validated_path, self.backup_path)
        
        command = f"backup create {self.backup_type} {validated_path}"
        
        if self.backup_set_name:
            command += f" -s {self.backup_set_name}"
        elif self.tables:
            tables_str = ",".join(self.tables)
            command += f" -t {tables_str}"
        else:
            raise ValueError("Either backup_set_name or tables must be specified")
        
        command += f" -w {self.workers}"
        
        if self.ignore_checksum:
            command += " -i"
        
        output = hook.execute_hbase_command(command, ssh_conn_id=self.ssh_conn_id)
        self.log.info("Backup command output: %s", output)
        return output


class HBaseRestoreOperator(BaseOperator):
    """
    Operator to restore HBase backup.
    
    :param backup_path: HDFS path where backup is stored.
    :param backup_id: ID of the backup to restore.
    :param backup_set_name: Name of the backup set to restore.
    :param tables: List of tables to restore (alternative to backup_set_name).
    :param overwrite: Whether to overwrite existing tables.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("backup_path", "backup_set_name", "tables")

    def __init__(
        self,
        backup_path: str,
        backup_id: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        overwrite: bool = False,
        ignore_checksum: bool = False,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        ssh_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_path = backup_path
        self.backup_id = backup_id
        self.backup_set_name = backup_set_name
        self.tables = tables
        self.overwrite = overwrite
        self.ignore_checksum = ignore_checksum
        self.hbase_conn_id = hbase_conn_id
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        
        if hook.is_standalone_mode():
            raise ValueError(
                "HBase backup restore is not supported in standalone mode. "
                "Please configure HDFS for distributed mode."
            )
        
        # Validate and adjust backup path based on HBase configuration
        validated_path = hook.validate_backup_path(self.backup_path)
        self.log.info("Using backup path: %s (original: %s)", validated_path, self.backup_path)
        
        command = f"restore {validated_path} {self.backup_id}"
        
        if self.backup_set_name:
            command += f" -s {self.backup_set_name}"
        elif self.tables:
            tables_str = ",".join(self.tables)
            command += f" -t {tables_str}"
        
        if self.overwrite:
            command += " -o"
        
        if self.ignore_checksum:
            command += " -i"
        
        return hook.execute_hbase_command(command, ssh_conn_id=self.ssh_conn_id)


class HBaseBackupHistoryOperator(BaseOperator):
    """
    Operator to get HBase backup history.
    
    :param backup_set_name: Name of the backup set to get history for.
    :param backup_path: HDFS path to get history for.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("backup_set_name", "backup_path")

    def __init__(
        self,
        backup_set_name: str | None = None,
        backup_path: str | None = None,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        ssh_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_set_name = backup_set_name
        self.backup_path = backup_path
        self.hbase_conn_id = hbase_conn_id
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        
        command = "backup history"
        
        if self.backup_set_name:
            command += f" -s {self.backup_set_name}"
        
        if self.backup_path:
            command += f" -p {self.backup_path}"
        
        return hook.execute_hbase_command(command, ssh_conn_id=self.ssh_conn_id)