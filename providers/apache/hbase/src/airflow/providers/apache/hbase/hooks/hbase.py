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

import json
import os
from typing import Any

from airflow.providers.apache.hbase.client import HBaseThrift2Client
from airflow.providers.apache.hbase.hooks.hbase_strategy import (
    HBaseStrategy,
    PooledThrift2Strategy,
    Thrift2Strategy,
)
from airflow.providers.apache.hbase.thrift2_pool import get_or_create_thrift2_pool
from airflow.providers.apache.hbase.version_compat import BaseHook


class HBaseThriftHook(BaseHook):
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
        self._strategy: HBaseStrategy | None = None

    def _get_strategy(self) -> HBaseStrategy:
        """Get Thrift2 strategy (single or pooled)."""
        if self._strategy is None:
            conn = self.get_connection(self.hbase_conn_id)
            host = conn.host or "localhost"
            port = conn.port or 9090
            timeout = conn.extra_dejson.get("timeout", 30000) if conn.extra_dejson else 30000

            # Get retry configuration
            retry_config = self._get_retry_config(conn.extra_dejson or {})

            # Get SSL options if configured
            ssl_options = self._get_ssl_options(conn.extra_dejson or {})
            use_http = conn.extra_dejson.get("use_http", False) if conn.extra_dejson else False
            namespace = conn.extra_dejson.get("namespace", "default") if conn.extra_dejson else "default"

            if ssl_options:
                self.log.info(
                    "SSL/TLS enabled for Thrift2 connection with options: %s",
                    {k: v for k, v in ssl_options.items() if k != "key_file"},
                )

            pool_config = self._get_pool_config(conn.extra_dejson or {})

            if pool_config.get("enabled", False):
                # Use connection pool for parallel processing
                pool_size = pool_config.get("size", 10)
                pool = get_or_create_thrift2_pool(
                    self.hbase_conn_id,
                    pool_size,
                    host,
                    port,
                    timeout,
                    ssl_options,
                    namespace=namespace,
                    use_http=use_http,
                    borrow_timeout=float(pool_config.get("timeout", 30)),
                    **retry_config,
                )
                self._strategy = PooledThrift2Strategy(pool, self.log)
            else:
                # Use single connection
                client = HBaseThrift2Client(
                    host=host,
                    port=port,
                    timeout=timeout,
                    ssl_options=ssl_options,
                    namespace=namespace,
                    use_http=use_http,
                    **retry_config,
                )
                client.open()
                self._strategy = Thrift2Strategy(client, self.log)
        return self._strategy

    def _get_ssl_options(self, extra_config: dict[str, Any]) -> dict[str, Any] | None:
        """
        Get SSL options from connection extra.

        Supports two formats:

        #. Nested: ``{"ssl_options": {"ca_certs": "...", ...}}``
        #. Flat: ``{"ca_certs": "...", "cert_file": "...", ...}``
        """
        if "ssl_options" in extra_config:
            return extra_config["ssl_options"]

        if any(key in extra_config for key in ["ca_certs", "cert_file", "key_file"]):
            ssl_options = {}
            for key in ["ca_certs", "cert_file", "key_file", "validate"]:
                if key in extra_config:
                    ssl_options[key] = extra_config[key]
            return ssl_options

        return None

    def _get_pool_config(self, extra_config: dict[str, Any]) -> dict[str, Any]:
        """Get connection pool configuration from connection extra."""
        pool_config = extra_config.get("connection_pool", {})
        return {
            "enabled": pool_config.get("enabled", False),
            "size": pool_config.get("size", 10),
            "timeout": pool_config.get("timeout", 30),
        }

    def _get_retry_config(self, extra_config: dict[str, Any]) -> dict[str, Any]:
        """Get retry configuration from connection extra."""
        return {
            "retry_max_attempts": extra_config.get("retry_max_attempts", 3),
            "retry_delay": extra_config.get("retry_delay", 1.0),
            "retry_backoff_factor": extra_config.get("retry_backoff_factor", 2.0),
        }

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in HBase."""
        return self._get_strategy().table_exists(table_name)

    def test_connection(self) -> tuple[bool, str]:
        """Verify HBase Thrift2 connectivity with a read-only probe."""
        try:
            self.table_exists("airflow_connection_test")
        except Exception as err:
            return False, f"HBase Thrift2 connection test failed: {err}"
        return True, "HBase Thrift2 connection test succeeded."

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create HBase table."""
        self._get_strategy().create_table(table_name, families)
        self.log.info("Created table %s", table_name)

    def delete_table(self, table_name: str) -> None:
        """Delete HBase table."""
        self._get_strategy().delete_table(table_name)
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
        limit: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Scan HBase table."""
        return self._get_strategy().scan_table(table_name, row_start, row_stop, columns, limit)

    def batch_put_rows(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        batch_size: int = 200,
        max_workers: int = 4,
    ) -> None:
        """Insert multiple rows in batch."""
        self._get_strategy().batch_put_rows(table_name, rows, batch_size, max_workers)
        self.log.info(
            "Batch put %d rows into table %s (batch_size=%d, workers=%d)",
            len(rows),
            table_name,
            batch_size,
            max_workers,
        )

    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch."""
        self._get_strategy().batch_delete_rows(table_name, row_keys, batch_size)
        self.log.info(
            "Batch deleted %d rows from table %s (batch_size=%d)", len(row_keys), table_name, batch_size
        )

    def batch_get_rows(
        self, table_name: str, row_keys: list[str], columns: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Get multiple rows in batch."""
        return self._get_strategy().batch_get_rows(table_name, row_keys, columns)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row or specific columns from HBase table."""
        self._get_strategy().delete_row(table_name, row_key, columns)
        self.log.info("Deleted row %s from table %s", row_key, table_name)

    def get_openlineage_database_info(self, connection):
        """Return HBase specific information for OpenLineage."""
        try:
            from airflow.providers.openlineage.sqlparser import (
                DatabaseInfo,
            )

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
        # Load extra placeholder from JSON file
        json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ui_field_behaviour.json")
        with open(json_path, encoding="utf-8") as f:
            extra_placeholder = json.load(f)

        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "host": "HBase Thrift2 Server Host",
                "port": "HBase Thrift2 Server Port",
            },
            "placeholders": {
                "host": "localhost",
                "port": "9090",
                "extra": json.dumps(extra_placeholder, indent=2),
            },
        }

    def close(self) -> None:
        """Close HBase connection."""
        if self._strategy:
            if isinstance(self._strategy, PooledThrift2Strategy):
                self._strategy.pool.close_all()
            elif hasattr(self._strategy, "client") and self._strategy.client:
                self._strategy.client.close()
            self._strategy = None
