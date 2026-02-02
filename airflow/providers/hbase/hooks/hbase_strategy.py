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

from airflow.providers.hbase.client import HBaseThrift2Client
from airflow.providers.hbase.thrift2_pool import Thrift2ConnectionPool


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
    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch."""
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


class Thrift2Strategy(HBaseStrategy):
    """HBase strategy using Thrift2 protocol."""

    def __init__(self, client: HBaseThrift2Client, logger):
        self.client = client
        self.log = logger

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists via Thrift2."""
        return self.client.table_exists(table_name)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table via Thrift2."""
        self.client.create_table(table_name, families)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """Delete table via Thrift2."""
        self.client.delete_table(table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row via Thrift2."""
        self.client.put(table_name, row_key, data)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row via Thrift2."""
        result = self.client.get(table_name, row_key, columns)
        if not result or 'columns' not in result:
            return {}
        return {col: data['value'] for col, data in result['columns'].items()}

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row via Thrift2."""
        self.client.delete(table_name, row_key, columns)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """Get column families via Thrift2."""
        return {}

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows via Thrift2 using batch API."""
        results = self.client.get_multiple(table_name, row_keys, columns)
        converted = []
        for result in results:
            if result and 'columns' in result:
                converted.append({col: data['value'] for col, data in result['columns'].items()})
        return converted

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 200, max_workers: int = 1) -> None:
        """Insert multiple rows via Thrift2 with batch API (single-threaded)."""
        if max_workers > 1:
            self.log.warning("Thrift2 doesn't support parallel processing (no connection pool). Using single thread.")
            max_workers = 1
        
        def process_chunk(chunk):
            """Process chunk using batch API."""
            chunk_size = sum(len(str(row)) for row in chunk)
            self.log.info(f"Processing chunk: {len(chunk)} rows, ~{chunk_size} bytes")
            
            try:
                puts = []
                for row in chunk:
                    if 'row_key' in row:
                        row_key = row.get('row_key')
                        row_data = {k: v for k, v in row.items() if k != 'row_key'}
                        puts.append((row_key, row_data))
                
                if puts:
                    self.client.put_multiple(table_name, puts)
                
                time.sleep(0.1)
            except Exception as e:
                self.log.error(f"Chunk processing failed: {e}")
                raise

        chunks = self._create_chunks(rows, batch_size)
        self.log.info(f"Processing {len(rows)} rows in {len(chunks)} chunks (batch_size={batch_size})")
        
        for chunk in chunks:
            process_chunk(chunk)

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table via Thrift2."""
        results = self.client.scan(table_name, row_start, row_stop, columns, limit)
        return [(r['row'], {col: data['value'] for col, data in r['columns'].items()}) for r in results]

    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch - not yet implemented."""
        raise NotImplementedError("batch_delete_rows not yet implemented for Thrift2Strategy")

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def list_backup_sets(self) -> str:
        """List backup sets - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_full_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create full backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_incremental_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create incremental backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def describe_backup(self, backup_id: str) -> str:
        """Describe backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def restore_backup(self, backup_root: str, backup_id: str, tables: list[str] | None = None, overwrite: bool = False) -> str:
        """Restore backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")


class PooledThrift2Strategy(HBaseStrategy):
    """HBase strategy using Thrift2 connection pool."""

    def __init__(self, pool: Thrift2ConnectionPool, logger):
        self.pool = pool
        self.log = logger

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists via pooled Thrift2."""
        with self.pool.connection() as client:
            return client.table_exists(table_name)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table via pooled Thrift2."""
        with self.pool.connection() as client:
            client.create_table(table_name, families)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """Delete table via pooled Thrift2."""
        with self.pool.connection() as client:
            client.delete_table(table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row via pooled Thrift2."""
        with self.pool.connection() as client:
            client.put(table_name, row_key, data)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row via pooled Thrift2."""
        with self.pool.connection() as client:
            result = client.get(table_name, row_key, columns)
            if not result or 'columns' not in result:
                return {}
            return {col: data['value'] for col, data in result['columns'].items()}

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row via pooled Thrift2."""
        with self.pool.connection() as client:
            client.delete(table_name, row_key, columns)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """Get column families via pooled Thrift2."""
        return {}

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows via pooled Thrift2."""
        with self.pool.connection() as client:
            results = client.get_multiple(table_name, row_keys, columns)
            converted = []
            for result in results:
                if result and 'columns' in result:
                    converted.append({col: data['value'] for col, data in result['columns'].items()})
            return converted

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 200, max_workers: int = 4) -> None:
        """Insert multiple rows via pooled Thrift2 with parallel processing."""
        if hasattr(self.pool, 'size') and self.pool.size < max_workers:
            self.log.warning(f"Pool size ({self.pool.size}) < max_workers ({max_workers}). Consider increasing pool size.")

        def process_chunk(chunk):
            """Process chunk using pooled connection."""
            chunk_size = sum(len(str(row)) for row in chunk)
            self.log.info(f"Processing chunk: {len(chunk)} rows, ~{chunk_size} bytes")
            
            try:
                with self.pool.connection() as client:
                    puts = []
                    for row in chunk:
                        if 'row_key' in row:
                            row_key = row.get('row_key')
                            row_data = {k: v for k, v in row.items() if k != 'row_key'}
                            puts.append((row_key, row_data))
                    
                    if puts:
                        client.put_multiple(table_name, puts)
                
                time.sleep(0.1)
            except Exception as e:
                self.log.error(f"Chunk processing failed: {e}")
                raise

        chunk_size = max(1, len(rows) // max_workers) if max_workers > 1 else batch_size
        chunks = self._create_chunks(rows, chunk_size)
        
        self.log.info(f"Processing {len(rows)} rows in {len(chunks)} chunks with {max_workers} workers (batch_size={batch_size})")
        
        if max_workers > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
                for future in futures:
                    future.result()
        else:
            for chunk in chunks:
                process_chunk(chunk)

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table via pooled Thrift2."""
        with self.pool.connection() as client:
            results = client.scan(table_name, row_start, row_stop, columns, limit)
            return [(r['row'], {col: data['value'] for col, data in r['columns'].items()}) for r in results]

    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch - not yet implemented."""
        raise NotImplementedError("batch_delete_rows not yet implemented for PooledThrift2Strategy")

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def list_backup_sets(self) -> str:
        """List backup sets - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_full_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create full backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_incremental_backup(self, backup_root: str, backup_set_name: str | None = None, tables: list[str] | None = None, workers: int | None = None) -> str:
        """Create incremental backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def describe_backup(self, backup_id: str) -> str:
        """Describe backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def restore_backup(self, backup_root: str, backup_id: str, tables: list[str] | None = None, overwrite: bool = False) -> str:
        """Restore backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")
