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

import concurrent.futures
import os
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.providers.apache.hbase.client import HBaseThrift2Client
    from airflow.providers.apache.hbase.thrift2_pool import Thrift2ConnectionPool

# Delay between batch operations to avoid overwhelming HBase
BATCH_DELAY = float(os.getenv("HBASE_BATCH_DELAY", "0.1"))

# Maximum chunk payload size in bytes (default 64 MB)
MAX_CHUNK_BYTES = int(os.getenv("HBASE_MAX_CHUNK_BYTES", str(64 * 1024 * 1024)))


class HBaseStrategy(ABC):
    """Abstract base class for HBase connection strategies."""

    @staticmethod
    def _create_chunks(rows: list, chunk_size: int) -> list[list]:
        """Split rows into chunks of specified size."""
        if not rows:
            return []
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        return [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]

    @staticmethod
    def _convert_scan_result(result: dict) -> tuple[str, dict[str, Any]]:
        """Convert scan result to tuple format."""
        return (result["row"], {col: data["value"] for col, data in result["columns"].items()})

    @staticmethod
    def _convert_get_result(result: dict | None) -> dict[str, Any]:
        """Convert get result to dict format."""
        if not result or "columns" not in result:
            return {}
        return {col: data["value"] for col, data in result["columns"].items()}

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""

    @abstractmethod
    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table."""

    @abstractmethod
    def delete_table(self, table_name: str) -> None:
        """Delete table."""

    @abstractmethod
    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row data."""

    @abstractmethod
    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row data."""

    @abstractmethod
    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row or specific columns."""

    @abstractmethod
    def batch_get_rows(
        self, table_name: str, row_keys: list[str], columns: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Get multiple rows in batch."""

    @abstractmethod
    def batch_put_rows(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        batch_size: int = 200,
        max_workers: int | None = None,
    ) -> None:
        """Insert multiple rows in batch with chunking and parallel processing."""

    @abstractmethod
    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch."""

    @abstractmethod
    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table."""

    @abstractmethod
    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set."""

    @abstractmethod
    def list_backup_sets(self) -> str:
        """List backup sets."""

    @abstractmethod
    def create_full_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """Create full backup."""

    @abstractmethod
    def create_incremental_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """Create incremental backup."""

    @abstractmethod
    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history."""

    @abstractmethod
    def describe_backup(self, backup_id: str) -> str:
        """Describe backup."""

    @abstractmethod
    def restore_backup(
        self,
        backup_root: str,
        backup_id: str,
        tables: list[str] | None = None,
        overwrite: bool = False,
    ) -> str:
        """Restore backup."""


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

    def delete_table(self, table_name: str) -> None:
        """Delete table via Thrift2."""
        self.client.delete_table(table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put row via Thrift2."""
        self.client.put(table_name, row_key, data)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row via Thrift2."""
        result = self.client.get(table_name, row_key, columns)
        return self._convert_get_result(result)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row via Thrift2."""
        self.client.delete(table_name, row_key, columns)

    def batch_get_rows(
        self, table_name: str, row_keys: list[str], columns: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Get multiple rows via Thrift2 using batch API."""
        results = self.client.get_multiple(table_name, row_keys, columns)
        return [self._convert_get_result(result) for result in results if result]

    def batch_put_rows(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        batch_size: int = 200,
        max_workers: int | None = None,
    ) -> None:
        """Insert multiple rows via Thrift2 with batch API (single-threaded)."""
        # Thrift2 doesn't support parallel processing
        if max_workers and max_workers > 1:
            self.log.warning(
                "Thrift2 doesn't support parallel processing (no connection pool). Ignoring max_workers."
            )

        def process_chunk(chunk):
            """Process chunk using batch API."""
            chunk_bytes = sum(len(str(row)) for row in chunk)
            if chunk_bytes > MAX_CHUNK_BYTES:
                self.log.warning(
                    "Chunk payload ~%d bytes exceeds limit %d bytes. Consider reducing batch_size.",
                    chunk_bytes,
                    MAX_CHUNK_BYTES,
                )
            self.log.info("Processing chunk: %d rows, ~%d bytes", len(chunk), chunk_bytes)

            try:
                puts = []
                for row in chunk:
                    # Handle both dict and tuple formats
                    if isinstance(row, tuple) and len(row) == 2:
                        # Format: (row_key, {col: val, ...})
                        row_key, row_data = row
                        puts.append((row_key, row_data))
                    elif isinstance(row, dict) and "row_key" in row:
                        # Format: {"row_key": "key", "col": "val", ...}
                        row_key = row.get("row_key")
                        row_data = {k: v for k, v in row.items() if k != "row_key"}
                        puts.append((row_key, row_data))
                    else:
                        self.log.warning("Unknown row format: %s, %s", type(row), row)

                if puts:
                    self.client.put_multiple(table_name, puts)
                else:
                    self.log.warning("No puts prepared - check row format")

                time.sleep(BATCH_DELAY)
            except Exception as e:
                self.log.error("Chunk processing failed: %s", e)
                raise

        chunks = self._create_chunks(rows, batch_size)
        self.log.info("Processing %d rows in %d chunks (batch_size=%d)", len(rows), len(chunks), batch_size)

        for chunk in chunks:
            process_chunk(chunk)

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table via Thrift2."""
        results = self.client.scan(table_name, row_start, row_stop, columns, limit)
        return [self._convert_scan_result(r) for r in results]

    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch via Thrift2."""

        def process_chunk(chunk):
            """Process chunk using batch delete API."""
            self.log.info("Deleting chunk: %d rows", len(chunk))
            try:
                # Convert row_keys to (row_key, None) tuples for delete_multiple
                deletes = [(row_key, None) for row_key in chunk]
                self.client.delete_multiple(table_name, deletes)
                time.sleep(BATCH_DELAY)
            except Exception as e:
                self.log.error("Chunk deletion failed: %s", e)
                raise

        chunks = self._create_chunks(row_keys, batch_size)
        self.log.info("Deleting %d rows in %d chunks (batch_size=%d)", len(row_keys), len(chunks), batch_size)

        for chunk in chunks:
            process_chunk(chunk)

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def list_backup_sets(self) -> str:
        """List backup sets - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_full_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """Create full backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_incremental_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """Create incremental backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def describe_backup(self, backup_id: str) -> str:
        """Describe backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def restore_backup(
        self,
        backup_root: str,
        backup_id: str,
        tables: list[str] | None = None,
        overwrite: bool = False,
    ) -> str:
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

    def delete_table(self, table_name: str) -> None:
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
            return self._convert_get_result(result)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row via pooled Thrift2."""
        with self.pool.connection() as client:
            client.delete(table_name, row_key, columns)

    def batch_get_rows(
        self, table_name: str, row_keys: list[str], columns: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Get multiple rows via pooled Thrift2."""
        with self.pool.connection() as client:
            results = client.get_multiple(table_name, row_keys, columns)
            return [self._convert_get_result(result) for result in results if result]

    def batch_put_rows(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        batch_size: int = 200,
        max_workers: int | None = None,
    ) -> None:
        """Insert multiple rows via pooled Thrift2 with parallel processing."""
        max_workers = max_workers or 4
        if hasattr(self.pool, "size") and self.pool.size < max_workers:
            self.log.warning(
                "Pool size (%d) < max_workers (%d). Consider increasing pool size.",
                self.pool.size,
                max_workers,
            )

        def process_chunk(chunk):
            """Process chunk using pooled connection."""
            chunk_bytes = sum(len(str(row)) for row in chunk)
            if chunk_bytes > MAX_CHUNK_BYTES:
                self.log.warning(
                    "Chunk payload ~%d bytes exceeds limit %d bytes. Consider reducing batch_size.",
                    chunk_bytes,
                    MAX_CHUNK_BYTES,
                )
            self.log.info("Processing chunk: %d rows, ~%d bytes", len(chunk), chunk_bytes)

            try:
                with self.pool.connection() as client:
                    puts = []
                    skipped = 0
                    for row in chunk:
                        if "row_key" in row:
                            row_key = row.get("row_key")
                            row_data = {k: v for k, v in row.items() if k != "row_key"}
                            puts.append((row_key, row_data))
                        else:
                            skipped += 1

                    if skipped:
                        self.log.warning("Skipped %d rows missing 'row_key' field", skipped)

                    if puts:
                        client.put_multiple(table_name, puts)

                time.sleep(BATCH_DELAY)
            except Exception as e:
                self.log.error("Chunk processing failed: %s", e)
                raise

        chunks = self._create_chunks(rows, batch_size)

        self.log.info(
            "Processing %d rows in %d chunks with %d workers (batch_size=%d)",
            len(rows),
            len(chunks),
            max_workers,
            batch_size,
        )

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
        limit: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan table via pooled Thrift2."""
        with self.pool.connection() as client:
            results = client.scan(table_name, row_start, row_stop, columns, limit)
            return [self._convert_scan_result(r) for r in results]

    def batch_delete_rows(
        self, table_name: str, row_keys: list[str], batch_size: int = 200, max_workers: int = 4
    ) -> None:
        """Delete multiple rows in batch via pooled Thrift2 with parallel processing."""
        if hasattr(self.pool, "size") and self.pool.size < max_workers:
            self.log.warning(
                "Pool size (%d) < max_workers (%d). Consider increasing pool size.",
                self.pool.size,
                max_workers,
            )

        def process_chunk(chunk):
            """Process chunk using pooled connection."""
            self.log.info("Deleting chunk: %d rows", len(chunk))
            try:
                with self.pool.connection() as client:
                    deletes = [(row_key, None) for row_key in chunk]
                    client.delete_multiple(table_name, deletes)
                time.sleep(BATCH_DELAY)
            except Exception as e:
                self.log.error("Chunk deletion failed: %s", e)
                raise

        chunks = self._create_chunks(row_keys, batch_size)

        self.log.info(
            "Deleting %d rows in %d chunks with %d workers (batch_size=%d)",
            len(row_keys),
            len(chunks),
            max_workers,
            batch_size,
        )

        if max_workers > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
                for future in futures:
                    future.result()
        else:
            for chunk in chunks:
                process_chunk(chunk)

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """Create backup set - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def list_backup_sets(self) -> str:
        """List backup sets - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_full_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """Create full backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def create_incremental_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """Create incremental backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """Get backup history - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def describe_backup(self, backup_id: str) -> str:
        """Describe backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")

    def restore_backup(
        self,
        backup_root: str,
        backup_id: str,
        tables: list[str] | None = None,
        overwrite: bool = False,
    ) -> str:
        """Restore backup - not supported in Thrift2 mode."""
        raise NotImplementedError("Backup operations require SSH connection mode")
