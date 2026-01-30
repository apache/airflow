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
"""HBase Thrift2 client implementation."""

from __future__ import annotations

import logging
import ssl
import time
from pathlib import Path
from typing import Any

import thriftpy2
from thriftpy2.rpc import make_client
from thriftpy2.transport.base import TTransportException


# Load Thrift2 definitions
THRIFT2_FILE = Path(__file__).parent.parent / "thrift_definitions" / "hbase_thrift2.thrift"
hbase_thrift2 = thriftpy2.load(str(THRIFT2_FILE), module_name="hbase_thrift2_thrift")

logger = logging.getLogger(__name__)


class HBaseThrift2Client:
    """Lightweight HBase Thrift2 client."""

    def __init__(self, host: str, port: int = 9090, timeout: int = 30000, 
                 ssl_context: ssl.SSLContext | None = None,
                 retry_max_attempts: int = 3,
                 retry_delay: float = 1.0,
                 retry_backoff_factor: float = 2.0):
        """Initialize Thrift2 client.
        
        Args:
            host: HBase Thrift2 server host
            port: HBase Thrift2 server port (default 9090 for Arenadata/Apache HBase)
            timeout: Connection timeout in milliseconds
            ssl_context: SSL context for secure connections (optional)
            retry_max_attempts: Maximum number of connection attempts
            retry_delay: Initial delay between retry attempts in seconds
            retry_backoff_factor: Multiplier for delay after each failed attempt
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.retry_max_attempts = retry_max_attempts
        self.retry_delay = retry_delay
        self.retry_backoff_factor = retry_backoff_factor
        self._client = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        """Open connection to Thrift2 server with retry logic."""
        last_exception = None
        
        for attempt in range(self.retry_max_attempts):
            try:
                # Build connection parameters
                kwargs = {
                    'timeout': self.timeout
                }
                
                # Add SSL if context provided
                if self.ssl_context:
                    kwargs['ssl_context'] = self.ssl_context
                
                self._client = make_client(
                    hbase_thrift2.THBaseService,
                    host=self.host,
                    port=self.port,
                    **kwargs
                )
                logger.info("Successfully connected to HBase Thrift2 at %s:%s", self.host, self.port)
                return
                
            except (ConnectionError, TimeoutError, TTransportException, OSError) as e:
                last_exception = e
                if attempt == self.retry_max_attempts - 1:  # Last attempt
                    logger.error("All %d connection attempts failed. Last error: %s", self.retry_max_attempts, e)
                    raise e

                wait_time = self.retry_delay * (self.retry_backoff_factor ** attempt)
                logger.warning(
                    "Connection attempt %d/%d failed: %s. Retrying in %.1fs...",
                    attempt + 1, self.retry_max_attempts, e, wait_time
                )
                time.sleep(wait_time)
        
        # This should never be reached, but just in case
        if last_exception:
            raise last_exception

    def close(self):
        """Close connection."""
        if self._client:
            self._client.close()
            self._client = None

    def list_tables(self) -> list[str]:
        """List all tables."""
        table_names = self._client.getTableNamesByPattern(regex=None, includeSysTables=False)
        return [tn.qualifier.decode() for tn in table_names]

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        table_name_obj = hbase_thrift2.TTableName(
            ns=b"default",
            qualifier=table_name.encode()
        )
        return self._client.tableExists(table_name_obj)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table.
        
        Args:
            table_name: Name of the table
            families: Dictionary of column families
        """
        table_name_obj = hbase_thrift2.TTableName(
            ns=b"default",
            qualifier=table_name.encode()
        )

        column_families = []
        for family_name in families.keys():
            col_desc = hbase_thrift2.TColumnFamilyDescriptor(
                name=family_name.encode()
            )
            column_families.append(col_desc)

        table_desc = hbase_thrift2.TTableDescriptor(
            tableName=table_name_obj,
            columns=column_families
        )

        self._client.createTable(table_desc, None)

    def delete_table(self, table_name: str) -> None:
        """Delete table.
        
        Args:
            table_name: Name of the table
        """
        table_name_obj = hbase_thrift2.TTableName(
            ns=b"default",
            qualifier=table_name.encode()
        )
        
        # Disable table first
        self._client.disableTable(table_name_obj)
        # Delete table
        self._client.deleteTable(table_name_obj)

    def put(self, table_name: str, row_key: str, data: dict[str, str]) -> None:
        """Put data into table.
        
        Args:
            table_name: Name of the table
            row_key: Row key
            data: Dictionary of column:value pairs (format: "family:qualifier": "value")
        """
        column_values = []
        for column, value in data.items():
            family, qualifier = column.split(":", 1)
            col_val = hbase_thrift2.TColumnValue(
                family=family.encode(),
                qualifier=qualifier.encode(),
                value=value.encode() if isinstance(value, str) else value
            )
            column_values.append(col_val)

        tput = hbase_thrift2.TPut(
            row=row_key.encode(),
            columnValues=column_values
        )

        # Use table name as bytes, not TTableName object
        self._client.put(table_name.encode(), tput)

    def put_multiple(self, table_name: str, puts: list[tuple[str, dict[str, str]]]) -> None:
        """Put multiple rows in batch.
        
        Args:
            table_name: Name of the table
            puts: List of (row_key, data) tuples
        """
        tputs = []
        for row_key, data in puts:
            column_values = []
            for column, value in data.items():
                family, qualifier = column.split(":", 1)
                col_val = hbase_thrift2.TColumnValue(
                    family=family.encode(),
                    qualifier=qualifier.encode(),
                    value=value.encode() if isinstance(value, str) else value
                )
                column_values.append(col_val)

            tput = hbase_thrift2.TPut(
                row=row_key.encode(),
                columnValues=column_values
            )
            tputs.append(tput)

        self._client.putMultiple(table_name.encode(), tputs)

    def get(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row from table.
        
        Args:
            table_name: Name of the table
            row_key: Row key
            columns: List of columns to retrieve (format: "family:qualifier")
            
        Returns:
            Dictionary with row data
        """
        tget = hbase_thrift2.TGet(row=row_key.encode())

        if columns:
            tget.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = hbase_thrift2.TColumn(
                    family=family.encode(),
                    qualifier=qualifier.encode()
                )
                tget.columns.append(tcol)

        result = self._client.get(table_name.encode(), tget)
        return self._parse_result(result)

    def get_multiple(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows in batch.
        
        Args:
            table_name: Name of the table
            row_keys: List of row keys
            columns: List of columns to retrieve
            
        Returns:
            List of row data dictionaries
        """
        tgets = []
        for row_key in row_keys:
            tget = hbase_thrift2.TGet(row=row_key.encode())
            
            if columns:
                tget.columns = []
                for column in columns:
                    family, qualifier = column.split(":", 1)
                    tcol = hbase_thrift2.TColumn(
                        family=family.encode(),
                        qualifier=qualifier.encode()
                    )
                    tget.columns.append(tcol)
            
            tgets.append(tget)

        results = self._client.getMultiple(table_name.encode(), tgets)
        return [self._parse_result(r) for r in results]

    def delete(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row or columns.
        
        Args:
            table_name: Name of the table
            row_key: Row key
            columns: List of columns to delete (if None, deletes entire row)
        """
        tdelete = hbase_thrift2.TDelete(row=row_key.encode())

        if columns:
            tdelete.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = hbase_thrift2.TColumn(
                    family=family.encode(),
                    qualifier=qualifier.encode()
                )
                tdelete.columns.append(tcol)

        self._client.deleteSingle(table_name.encode(), tdelete)

    def delete_multiple(self, table_name: str, deletes: list[tuple[str, list[str] | None]]) -> None:
        """Delete multiple rows in batch.
        
        Args:
            table_name: Name of the table
            deletes: List of (row_key, columns) tuples
        """
        tdeletes = []
        for row_key, columns in deletes:
            tdelete = hbase_thrift2.TDelete(row=row_key.encode())
            
            if columns:
                tdelete.columns = []
                for column in columns:
                    family, qualifier = column.split(":", 1)
                    tcol = hbase_thrift2.TColumn(
                        family=family.encode(),
                        qualifier=qualifier.encode()
                    )
                    tdelete.columns.append(tcol)
            
            tdeletes.append(tdelete)

        self._client.deleteMultiple(table_name.encode(), tdeletes)

    def scan(
        self,
        table_name: str,
        start_row: str | None = None,
        stop_row: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Scan table.
        
        Args:
            table_name: Name of the table
            start_row: Start row key
            stop_row: Stop row key
            columns: List of columns to retrieve
            limit: Maximum number of rows
            
        Returns:
            List of row data dictionaries
        """
        tscan = hbase_thrift2.TScan()

        if start_row:
            tscan.startRow = start_row.encode()
        if stop_row:
            tscan.stopRow = stop_row.encode()
        if limit:
            tscan.limit = limit

        if columns:
            tscan.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = hbase_thrift2.TColumn(
                    family=family.encode(),
                    qualifier=qualifier.encode()
                )
                tscan.columns.append(tcol)

        scanner_id = self._client.openScanner(table_name.encode(), tscan)

        try:
            results = []
            while True:
                rows = self._client.getScannerRows(scanner_id, limit or 100)
                if not rows:
                    break

                for row in rows:
                    results.append(self._parse_result(row))

                if limit and len(results) >= limit:
                    break

            return results
        finally:
            self._client.closeScanner(scanner_id)

    def _parse_result(self, result) -> dict[str, Any]:
        """Parse Thrift2 result to dictionary.
        
        Args:
            result: TResult object from Thrift2
            
        Returns:
            Dictionary with parsed data
        """
        if not result or not result.columnValues:
            return {}

        parsed = {
            'row': result.row.decode() if result.row else None,
            'columns': {}
        }

        for col_val in result.columnValues:
            family = col_val.family.decode()
            qualifier = col_val.qualifier.decode()
            value = col_val.value
            timestamp = col_val.timestamp if hasattr(col_val, 'timestamp') else None

            column_name = f"{family}:{qualifier}"
            parsed['columns'][column_name] = {
                'value': value,
                'timestamp': timestamp
            }

        return parsed
