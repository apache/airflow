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

from enum import Enum
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.hbase.hooks.hbase import HBaseHook
from airflow.providers.hbase.hooks.hbase_administration import HBaseAdministrationHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BackupSetAction(str, Enum):
    """Enum for HBase backup set actions."""

    ADD = "add"
    LIST = "list"
    DESCRIBE = "describe"
    DELETE = "delete"


class BackupType(str, Enum):
    """Enum for HBase backup types."""

    FULL = "full"
    INCREMENTAL = "incremental"


class IfExistsAction(str, Enum):
    """Enum for table existence handling."""

    IGNORE = "ignore"
    ERROR = "error"


class IfNotExistsAction(str, Enum):
    """Enum for table non-existence handling."""

    IGNORE = "ignore"
    ERROR = "error"


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
    :param if_exists: Action to take if table already exists.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "families")

    def __init__(
        self,
        table_name: str,
        families: dict[str, dict],
        if_exists: IfExistsAction = IfExistsAction.IGNORE,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.families = families
        self.if_exists = if_exists
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        if not hook.table_exists(self.table_name):
            hook.create_table(self.table_name, self.families)
        else:
            if self.if_exists == IfExistsAction.ERROR:
                raise ValueError(f"Table {self.table_name} already exists")
            self.log.info("Table %s already exists", self.table_name)


class HBaseDeleteTableOperator(BaseOperator):
    """
    Operator to delete HBase table.
    
    :param table_name: Name of the table to delete.
    :param disable: Whether to disable table before deletion.
    :param if_not_exists: Action to take if table does not exist.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name",)

    def __init__(
        self,
        table_name: str,
        disable: bool = True,
        if_not_exists: IfNotExistsAction = IfNotExistsAction.IGNORE,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.disable = disable
        self.if_not_exists = if_not_exists
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        if hook.table_exists(self.table_name):
            hook.delete_table(self.table_name, self.disable)
        else:
            if self.if_not_exists == IfNotExistsAction.ERROR:
                raise ValueError(f"Table {self.table_name} does not exist")
            self.log.info("Table %s does not exist", self.table_name)


class HBaseScanOperator(BaseOperator):
    """
    Operator to scan HBase table.
    
    :param table_name: Name of the table to scan.
    :param row_start: Start row key for scan.
    :param row_stop: Stop row key for scan.
    :param columns: List of columns to retrieve.
    :param limit: Maximum number of rows to return.
    :param encoding: Encoding to use for decoding bytes (default: 'utf-8').
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
        encoding: str = 'utf-8',
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.row_start = row_start
        self.row_stop = row_stop
        self.columns = columns
        self.limit = limit
        self.encoding = encoding
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
            row_dict = {"row_key": row_key.decode(self.encoding) if isinstance(row_key, bytes) else row_key}
            for col, val in data.items():
                col_str = col.decode(self.encoding) if isinstance(col, bytes) else col
                val_str = val.decode(self.encoding) if isinstance(val, bytes) else val
                row_dict[col_str] = val_str
            serializable_results.append(row_dict)
        return serializable_results


class HBaseBatchPutOperator(BaseOperator):
    """
    Operator to insert multiple rows into HBase table in batch with optimization.
    
    :param table_name: Name of the table.
    :param rows: List of dictionaries with 'row_key' and data columns.
    :param batch_size: Number of rows per batch chunk (default: 200).
    :param max_workers: Number of parallel workers (default: 4).
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "rows")

    def __init__(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        batch_size: int = 200,
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
    :param encoding: Encoding to use for decoding bytes (default: 'utf-8').
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "row_keys", "columns")

    def __init__(
        self,
        table_name: str,
        row_keys: list[str],
        columns: list[str] | None = None,
        encoding: str = 'utf-8',
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.row_keys = row_keys
        self.columns = columns
        self.encoding = encoding
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
                col_str = col.decode(self.encoding) if isinstance(col, bytes) else col
                val_str = val.decode(self.encoding) if isinstance(val, bytes) else val
                row_dict[col_str] = val_str
            serializable_results.append(row_dict)
        return serializable_results


class HBaseBackupSetOperator(BaseOperator):
    """
    Operator to manage HBase backup sets.
    
    :param action: Action to perform.
    :param backup_set_name: Name of the backup set.
    :param tables: List of tables to add to backup set (for 'add' action).
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("backup_set_name", "tables")

    def __init__(
        self,
        action: BackupSetAction,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.action = action
        self.backup_set_name = backup_set_name
        self.tables = tables or []
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseAdministrationHook(hbase_conn_id=self.hbase_conn_id)
        
        if not isinstance(self.action, BackupSetAction):
            raise ValueError(f"Unsupported action: {self.action}")
        
        if self.action == BackupSetAction.ADD:
            if not self.backup_set_name or not self.tables:
                raise ValueError("backup_set_name and tables are required for 'add' action")
            return hook.create_backup_set(self.backup_set_name, self.tables)
        elif self.action == BackupSetAction.LIST:
            return hook.list_backup_sets()
        elif self.action == BackupSetAction.DESCRIBE:
            if not self.backup_set_name:
                raise ValueError("backup_set_name is required for 'describe' action")
            # Note: describe not implemented in hook yet
            raise NotImplementedError("Backup set describe not yet implemented")
        elif self.action == BackupSetAction.DELETE:
            if not self.backup_set_name:
                raise ValueError("backup_set_name is required for 'delete' action")
            # Note: delete not implemented in hook yet
            raise NotImplementedError("Backup set delete not yet implemented")


class HBaseCreateBackupOperator(BaseOperator):
    """
    Operator to create HBase backup.
    
    :param backup_type: Type of backup.
    :param backup_path: HDFS path where backup will be stored.
    :param backup_set_name: Name of the backup set to backup.
    :param tables: List of tables to backup (alternative to backup_set_name).
    :param workers: Number of workers for backup operation.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("backup_path", "backup_set_name", "tables")

    def __init__(
        self,
        backup_type: BackupType,
        backup_path: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int = 3,
        ignore_checksum: bool = False,
        hbase_conn_id: str = HBaseHook.default_conn_name,
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

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseAdministrationHook(hbase_conn_id=self.hbase_conn_id)
        
        if not isinstance(self.backup_type, BackupType):
            raise ValueError("backup_type must be 'full' or 'incremental'")
        
        if not self.backup_set_name and not self.tables:
            raise ValueError("Either backup_set_name or tables must be specified")
        
        if self.backup_type == BackupType.FULL:
            output = hook.create_full_backup(
                backup_root=self.backup_path,
                backup_set_name=self.backup_set_name,
                tables=self.tables,
                workers=self.workers
            )
        else:  # INCREMENTAL
            output = hook.create_incremental_backup(
                backup_root=self.backup_path,
                backup_set_name=self.backup_set_name,
                tables=self.tables,
                workers=self.workers
            )
        
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

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseAdministrationHook(hbase_conn_id=self.hbase_conn_id)
        
        return hook.restore_backup(
            backup_root=self.backup_path,
            backup_id=self.backup_id,
            tables=self.tables,
            overwrite=self.overwrite
        )


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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_set_name = backup_set_name
        self.backup_path = backup_path
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        hook = HBaseAdministrationHook(hbase_conn_id=self.hbase_conn_id)
        
        return hook.get_backup_history(backup_set_name=self.backup_set_name)