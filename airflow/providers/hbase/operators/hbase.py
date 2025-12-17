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
    Operator to insert multiple rows into HBase table in batch.
    
    :param table_name: Name of the table.
    :param rows: List of dictionaries with 'row_key' and data columns.
    :param hbase_conn_id: The connection ID to use for HBase connection.
    """

    template_fields: Sequence[str] = ("table_name", "rows")

    def __init__(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        hbase_conn_id: str = HBaseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.rows = rows
        self.hbase_conn_id = hbase_conn_id

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        hook = HBaseHook(hbase_conn_id=self.hbase_conn_id)
        hook.batch_put_rows(self.table_name, self.rows)


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