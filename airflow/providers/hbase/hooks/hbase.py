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
"""HBase hook module."""

from __future__ import annotations

import os
import time
from functools import wraps
from typing import Any

from thriftpy2.transport.base import TTransportException

from airflow.hooks.base import BaseHook
from airflow.providers.hbase.client import HBaseThrift2Client
from airflow.providers.hbase.hooks.hbase_strategy import HBaseStrategy, Thrift2Strategy, PooledThrift2Strategy
from airflow.providers.hbase.thrift2_pool import get_or_create_thrift2_pool
from airflow.providers.hbase.thrift2_ssl import create_ssl_context as create_thrift2_ssl_context


def retry_on_connection_error(max_attempts: int = 3, delay: float = 1.0, backoff_factor: float = 2.0):
    """Decorator for retrying connection operations with exponential backoff.

    Args:
        max_attempts: Maximum number of connection attempts
        delay: Initial delay between attempts in seconds
        backoff_factor: Multiplier for delay after each failed attempt
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(self, *args, **kwargs)
                except (ConnectionError, TimeoutError, TTransportException, OSError) as e:
                    last_exception = e
                    if attempt == max_attempts - 1:  # Last attempt
                        self.log.error("All %d connection attempts failed. Last error: %s", max_attempts, e)
                        raise e

                    wait_time = delay * (backoff_factor ** attempt)
                    self.log.warning(
                        "Connection attempt %d/%d failed: %s. Retrying in %.1fs...",
                        attempt + 1, max_attempts, e, wait_time
                    )
                    time.sleep(wait_time)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


class HBaseHook(BaseHook):
    """
    Wrapper for connection to interact with HBase via Thrift2 protocol.

    This hook provides functionality to connect to HBase and perform operations on tables.
    """

    conn_name_attr = "hbase_conn_id"
    default_conn_name = "hbase_default"
    conn_type = "hbase"
    hook_name = "HBase"

    def __init__(self, hbase_conn_id: str = default_conn_name) -> None:
        """
        Initialize HBase hook.

        :param hbase_conn_id: The connection ID to use for HBase connection.
        """
        super().__init__()
        self.hbase_conn_id = hbase_conn_id
        self._strategy = None
        self._temp_cert_files: list[str] = []

    def _get_strategy(self) -> HBaseStrategy:
        """Get Thrift2 strategy (single or pooled)."""
        if self._strategy is None:
            conn = self.get_connection(self.hbase_conn_id)
            host = conn.host or "localhost"
            port = conn.port or 9090
            timeout = conn.extra_dejson.get("timeout", 30000) if conn.extra_dejson else 30000
            
            # Setup SSL if configured
            ssl_context = None
            if conn.extra_dejson and conn.extra_dejson.get("use_ssl", False):
                ssl_context, temp_files = create_thrift2_ssl_context(conn.extra_dejson)
                self._temp_cert_files.extend(temp_files)
                self.log.info("SSL/TLS enabled for Thrift2 connection")
            
            pool_config = self._get_pool_config(conn.extra_dejson or {})
            
            if pool_config.get('enabled', False):
                # Use connection pool for parallel processing
                pool_size = pool_config.get('size', 10)
                pool = get_or_create_thrift2_pool(
                    self.hbase_conn_id, 
                    pool_size, 
                    host, 
                    port, 
                    timeout,
                    ssl_context
                )
                self._strategy = PooledThrift2Strategy(pool, self.log)
            else:
                # Use single connection
                client = HBaseThrift2Client(host=host, port=port, timeout=timeout, ssl_context=ssl_context)
                client.open()
                self._strategy = Thrift2Strategy(client, self.log)
        return self._strategy

    def _get_pool_config(self, extra_config: dict[str, Any]) -> dict[str, Any]:
        """Get connection pool configuration from connection extra."""
        pool_config = extra_config.get('connection_pool', {})
        return {
            'enabled': pool_config.get('enabled', False),
            'size': pool_config.get('size', 10),
            'timeout': pool_config.get('timeout', 30),
        }

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in HBase."""
        return self._get_strategy().table_exists(table_name)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create HBase table."""
        self._get_strategy().create_table(table_name, families)
        self.log.info("Created table %s", table_name)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """Delete HBase table."""
        self._get_strategy().delete_table(table_name, disable)
        self.log.info("Deleted table %s", table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """Put data into HBase table."""
        self._get_strategy().put_row(table_name, row_key, data)
        self.log.info("Put row %s into table %s", row_key, table_name)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row from HBase table."""
        return self._get_strategy().get_row(table_name, row_key, columns)

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan HBase table."""
        return self._get_strategy().scan_table(table_name, row_start, row_stop, columns, limit)

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 200, max_workers: int = 1) -> None:
        """Insert multiple rows in batch."""
        self._get_strategy().batch_put_rows(table_name, rows, batch_size, max_workers)
        self.log.info("Batch put %d rows into table %s (batch_size=%d, workers=%d)", 
                     len(rows), table_name, batch_size, max_workers)

    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch."""
        self._get_strategy().batch_delete_rows(table_name, row_keys, batch_size)
        self.log.info("Batch deleted %d rows from table %s (batch_size=%d)", 
                     len(row_keys), table_name, batch_size)

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows in batch."""
        return self._get_strategy().batch_get_rows(table_name, row_keys, columns)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row or specific columns from HBase table."""
        self._get_strategy().delete_row(table_name, row_key, columns)
        self.log.info("Deleted row %s from table %s", row_key, table_name)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """Get column families for a table."""
        return self._get_strategy().get_table_families(table_name)

    def get_openlineage_database_info(self, connection):
        """Return HBase specific information for OpenLineage."""
        try:
            from airflow.providers.openlineage.sqlparser import DatabaseInfo
            return DatabaseInfo(
                scheme="hbase",
                authority=f"{connection.host}:{connection.port or 9090}",
                database="default",
            )
        except ImportError:
            return None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for HBase connection."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "host": "HBase Thrift2 Server Host",
                "port": "HBase Thrift2 Server Port",
            },
            "placeholders": {
                "host": "localhost",
                "port": "9090",
                "extra": '''{
  "use_ssl": false,
  "ssl_verify_mode": "CERT_REQUIRED",
  "ssl_ca_secret": "hbase/ca-cert",
  "ssl_cert_secret": "hbase/client-cert",
  "ssl_key_secret": "hbase/client-key",
  "timeout": 30000,
  "connection_pool": {
    "enabled": false,
    "size": 10,
    "timeout": 30
  }
}'''
            },
        }

    def close(self) -> None:
        """Close HBase connection and cleanup temporary files."""
        if self._strategy:
            # Don't close pooled strategies - they manage their own lifecycle
            if not isinstance(self._strategy, PooledThrift2Strategy):
                if hasattr(self._strategy, 'client') and self._strategy.client:
                    self._strategy.client.close()
        self._cleanup_temp_files()

    def _cleanup_temp_files(self) -> None:
        """Clean up temporary certificate files."""
        for temp_file in self._temp_cert_files:
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
                    self.log.debug("Cleaned up temporary file: %s", temp_file)
            except Exception as e:
                self.log.warning("Failed to cleanup temporary file %s: %s", temp_file, e)
        self._temp_cert_files.clear()
