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
import re
import ssl
import tempfile
import time
from enum import Enum
from functools import wraps
from typing import Any

import happybase
from thriftpy2.transport.base import TTransportException

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.hbase.auth import AuthenticatorFactory
from airflow.providers.hbase.client import HBaseThrift2Client
from airflow.providers.hbase.connection_pool import get_or_create_pool
from airflow.providers.hbase.hooks.hbase_strategy import HBaseStrategy, ThriftStrategy, Thrift2Strategy, PooledThriftStrategy, PooledThrift2Strategy
from airflow.providers.hbase.ssl_connection import create_ssl_connection
from airflow.providers.hbase.thrift2_pool import get_or_create_thrift2_pool
from airflow.providers.hbase.thrift2_ssl import create_ssl_context as create_thrift2_ssl_context


class ConnectionMode(Enum):
    """HBase connection modes."""
    THRIFT = "thrift"
    THRIFT2 = "thrift2"


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
    Wrapper for connection to interact with HBase.

    This hook provides basic functionality to connect to HBase
    and perform operations on tables via Thrift or SSH.
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
        self._connection = None
        self._connection_mode = None  # 'thrift' or 'ssh'
        self._strategy = None
        self._temp_cert_files: list[str] = []

    def _get_connection_mode(self) -> ConnectionMode:
        """Determine connection mode based on configuration."""
        if self._connection_mode is None:
            conn = self.get_connection(self.hbase_conn_id)
            connection_mode = conn.extra_dejson.get("connection_mode") if conn.extra_dejson else None
            self.log.info("Connection mode: %s", connection_mode or "thrift (default)")
            
            if conn.extra_dejson and conn.extra_dejson.get("connection_mode") == ConnectionMode.THRIFT2.value:
                self._connection_mode = ConnectionMode.THRIFT2
                self.log.info("Using Thrift2 connection mode")
            else:
                self._connection_mode = ConnectionMode.THRIFT
                self.log.info("Using Thrift connection mode")
        return self._connection_mode

    def _get_strategy(self) -> HBaseStrategy:
        """Get appropriate strategy based on connection mode."""
        if self._strategy is None:
            mode = self._get_connection_mode()
            
            if mode == ConnectionMode.THRIFT2:
                # Create Thrift2 client or pool
                conn = self.get_connection(self.hbase_conn_id)
                host = conn.host or "localhost"
                port = conn.port or 9090  # Default Thrift2 port for Arenadata/Apache HBase
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
            else:  # THRIFT (default)
                conn = self.get_connection(self.hbase_conn_id)
                pool_config = self._get_pool_config(conn.extra_dejson or {})

                if pool_config.get('enabled', False):
                    connection_args = self._get_connection_args()
                    pool_size = pool_config.get('size', 10)
                    pool = get_or_create_pool(self.hbase_conn_id, pool_size, **connection_args)
                    self._strategy = PooledThriftStrategy(pool, self.log)
                else:
                    connection = self.get_conn()
                    self._strategy = ThriftStrategy(connection, self.log)
        return self._strategy


    def get_conn(self) -> happybase.Connection:
        """Return HBase connection (Thrift mode only)."""
        mode = self._get_connection_mode()
        if mode == ConnectionMode.THRIFT2:
            raise RuntimeError(
                "get_conn() is not available in Thrift2 mode. Use Hook methods directly.")

        if self._connection is None:
            conn = self.get_connection(self.hbase_conn_id)

            connection_args = {
                "host": conn.host or "localhost",
                "port": conn.port or 9090,
            }

            # Setup authentication
            auth_method = conn.extra_dejson.get("auth_method", "simple") if conn.extra_dejson else "simple"
            authenticator = AuthenticatorFactory.create(auth_method)
            auth_kwargs = authenticator.authenticate(conn.extra_dejson or {})
            connection_args.update(auth_kwargs)

            # Setup SSL/TLS if configured
            ssl_args = self._setup_ssl_connection(conn.extra_dejson or {})
            connection_args.update(ssl_args)

            # Get retry configuration from connection extra
            retry_config = self._get_retry_config(conn.extra_dejson or {})

            self.log.info("Connecting to HBase at %s:%s with %s authentication%s (retry: %d attempts)",
                          connection_args["host"], connection_args["port"], auth_method,
                          " (SSL)" if ssl_args else "", retry_config["max_attempts"])

            # Use retry logic for connection
            self._connection = self._connect_with_retry(conn.extra_dejson or {}, **connection_args)

        return self._connection

    def _get_pool_config(self, extra_config: dict[str, Any]) -> dict[str, Any]:
        """Get connection pool configuration from connection extra.

        Args:
            extra_config: Connection extra configuration

        Returns:
            Dictionary with pool configuration
        """
        pool_config = extra_config.get('connection_pool', {})
        return {
            'enabled': pool_config.get('enabled', False),
            'size': pool_config.get('size', 10),
            'timeout': pool_config.get('timeout', 30),
            'retry_delay': pool_config.get('retry_delay', 1.0)
        }

    def _get_connection_args(self) -> dict[str, Any]:
        """Get connection arguments for pool creation.

        Returns:
            Dictionary with connection arguments
        """
        conn = self.get_connection(self.hbase_conn_id)

        connection_args = {
            "host": conn.host or "localhost",
            "port": conn.port or 9090,
        }

        # Setup authentication
        auth_method = conn.extra_dejson.get("auth_method", "simple") if conn.extra_dejson else "simple"
        authenticator = AuthenticatorFactory.create(auth_method)
        auth_kwargs = authenticator.authenticate(conn.extra_dejson or {})
        connection_args.update(auth_kwargs)

        return connection_args

    def _get_retry_config(self, extra_config: dict[str, Any]) -> dict[str, Any]:
        """Get retry configuration from connection extra.

        Args:
            extra_config: Connection extra configuration

        Returns:
            Dictionary with retry configuration
        """
        return {
            "max_attempts": extra_config.get("retry_max_attempts", 3),
            "delay": extra_config.get("retry_delay", 1.0),
            "backoff_factor": extra_config.get("retry_backoff_factor", 2.0)
        }

    def _connect_with_retry(self, extra_config: dict[str, Any], **connection_args) -> happybase.Connection:
        """Connect to HBase with retry logic.

        Args:
            extra_config: Connection extra configuration
            **connection_args: Connection arguments for HappyBase

        Returns:
            Connected HappyBase connection
        """
        retry_config = self._get_retry_config(extra_config)
        max_attempts = retry_config["max_attempts"]
        delay = retry_config["delay"]
        backoff_factor = retry_config["backoff_factor"]
        
        last_exception = None
        
        for attempt in range(max_attempts):
            try:
                # Use custom SSL connection if SSL is configured
                if extra_config.get("use_ssl", False):
                    connection = create_ssl_connection(
                        host=connection_args["host"],
                        port=connection_args["port"],
                        ssl_config=extra_config,
                        **{k: v for k, v in connection_args.items() if k not in ['host', 'port']}
                    )
                else:
                    connection = happybase.Connection(**connection_args)

                # Test the connection by opening it
                connection.open()
                self.log.info("Successfully connected to HBase at %s:%s",
                             connection_args["host"], connection_args["port"])
                return connection
                
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

    def get_table(self, table_name: str) -> happybase.Table:
        """
        Get HBase table object (Thrift mode only).

        :param table_name: Name of the table to get.
        :return: HBase table object.
        """
        mode = self._get_connection_mode()
        if mode == ConnectionMode.THRIFT2:
            raise RuntimeError(
                "get_table() is not available in Thrift2 mode. Use Hook methods directly.")
        connection = self.get_conn()
        return connection.table(table_name)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in HBase.

        :param table_name: Name of the table to check.
        :return: True if table exists, False otherwise.
        """
        return self._get_strategy().table_exists(table_name)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """
        Create HBase table.

        :param table_name: Name of the table to create.
        :param families: Dictionary of column families and their configuration.
        """
        self._get_strategy().create_table(table_name, families)
        self.log.info("Created table %s", table_name)

    def delete_table(self, table_name: str, disable: bool = True) -> None:
        """
        Delete HBase table.

        :param table_name: Name of the table to delete.
        :param disable: Whether to disable table before deletion.
        """
        self._get_strategy().delete_table(table_name, disable)
        self.log.info("Deleted table %s", table_name)

    def put_row(self, table_name: str, row_key: str, data: dict[str, Any]) -> None:
        """
        Put data into HBase table.

        :param table_name: Name of the table.
        :param row_key: Row key for the data.
        :param data: Dictionary of column:value pairs to insert.
        """
        self._get_strategy().put_row(table_name, row_key, data)
        self.log.info("Put row %s into table %s", row_key, table_name)

    def get_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """
        Get row from HBase table.

        :param table_name: Name of the table.
        :param row_key: Row key to retrieve.
        :param columns: List of columns to retrieve (optional).
        :return: Dictionary of column:value pairs.
        """
        return self._get_strategy().get_row(table_name, row_key, columns)

    def scan_table(
        self,
        table_name: str,
        row_start: str | None = None,
        row_stop: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Scan HBase table.

        :param table_name: Name of the table.
        :param row_start: Start row key for scan.
        :param row_stop: Stop row key for scan.
        :param columns: List of columns to retrieve.
        :param limit: Maximum number of rows to return.
        :return: List of (row_key, data) tuples.
        """
        return self._get_strategy().scan_table(table_name, row_start, row_stop, columns, limit)

    def batch_put_rows(self, table_name: str, rows: list[dict[str, Any]], batch_size: int = 200, max_workers: int = 1) -> None:
        """Insert multiple rows in batch.

        :param table_name: Name of the table.
        :param rows: List of dictionaries with 'row_key' and data columns.
        :param batch_size: Number of rows per batch chunk.
        :param max_workers: Number of parallel workers.
        """
        self._get_strategy().batch_put_rows(table_name, rows, batch_size, max_workers)
        self.log.info("Batch put %d rows into table %s (batch_size=%d, workers=%d)", 
                     len(rows), table_name, batch_size, max_workers)

    def batch_delete_rows(self, table_name: str, row_keys: list[str], batch_size: int = 200) -> None:
        """Delete multiple rows in batch.

        :param table_name: Name of the table.
        :param row_keys: List of row keys to delete.
        :param batch_size: Number of rows per batch chunk.
        """
        self._get_strategy().batch_delete_rows(table_name, row_keys, batch_size)
        self.log.info("Batch deleted %d rows from table %s (batch_size=%d)", 
                     len(row_keys), table_name, batch_size)

    def batch_get_rows(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """
        Get multiple rows in batch.

        :param table_name: Name of the table.
        :param row_keys: List of row keys to retrieve.
        :param columns: List of columns to retrieve.
        :return: List of row data dictionaries.
        """
        return self._get_strategy().batch_get_rows(table_name, row_keys, columns)

    def delete_row(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """
        Delete row or specific columns from HBase table.

        :param table_name: Name of the table.
        :param row_key: Row key to delete.
        :param columns: List of columns to delete (if None, deletes entire row).
        """
        self._get_strategy().delete_row(table_name, row_key, columns)
        self.log.info("Deleted row %s from table %s", row_key, table_name)

    def get_table_families(self, table_name: str) -> dict[str, dict]:
        """
        Get column families for a table.

        :param table_name: Name of the table.
        :return: Dictionary of column families and their properties.
        """
        return self._get_strategy().get_table_families(table_name)

    def get_openlineage_database_info(self, connection):
        """
        Return HBase specific information for OpenLineage.

        :param connection: HBase connection object.
        :return: DatabaseInfo object or None if OpenLineage not available.
        """
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
        """
        Return custom UI field behaviour for HBase connection.

        :return: Dictionary defining UI field behaviour.
        """
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "host": "HBase Thrift Server Host",
                "port": "HBase Thrift Server Port",
            },
            "placeholders": {
                "host": "localhost",
                "port": "9090 (Thrift1) / 9091 (Thrift2)",
                "extra": '''{
  "connection_mode": "thrift",
  "auth_method": "simple",
  "use_ssl": false,
  "ssl_verify_mode": "CERT_REQUIRED",
  "ssl_ca_secret": "hbase/ca-cert",
  "ssl_cert_secret": "hbase/client-cert",
  "ssl_key_secret": "hbase/client-key",
  "ssl_port": 9091,
  "retry_max_attempts": 3,
  "retry_delay": 1.0,
  "retry_backoff_factor": 2.0,
  "connection_pool": {
    "enabled": false,
    "size": 10,
    "timeout": 30,
    "retry_delay": 1.0
  }
}'''
            },
        }

    def close(self) -> None:
        """Close HBase connection and cleanup temporary files."""
        if self._strategy:
            # Don't close pooled strategies - they manage their own lifecycle
            if not isinstance(self._strategy, (PooledThriftStrategy, PooledThrift2Strategy)):
                if hasattr(self._strategy, 'client') and self._strategy.client:
                    self._strategy.client.close()
        if self._connection:
            self._connection.close()
            self._connection = None
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

    def _setup_ssl_connection(self, extra_config: dict[str, Any]) -> dict[str, Any]:
        """
        Setup SSL/TLS connection parameters for Thrift.

        :param extra_config: Connection extra configuration.
        :return: Dictionary with SSL connection arguments.
        """
        ssl_args = {}

        if not extra_config.get("use_ssl", False):
            return ssl_args

        # Create SSL context
        ssl_context = ssl.create_default_context()

        # Configure SSL verification
        verify_mode = extra_config.get("ssl_verify_mode", "CERT_REQUIRED")
        if verify_mode == "CERT_NONE":
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        elif verify_mode == "CERT_OPTIONAL":
            ssl_context.verify_mode = ssl.CERT_OPTIONAL
        else:  # CERT_REQUIRED (default)
            ssl_context.verify_mode = ssl.CERT_REQUIRED

        # Load CA certificate from Variables (fallback for Secrets Backend)
        if extra_config.get("ssl_ca_secret"):
            ca_cert_content = Variable.get(extra_config["ssl_ca_secret"], None)
            if ca_cert_content:
                ca_cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
                ca_cert_file.write(ca_cert_content)
                ca_cert_file.close()
                ssl_context.load_verify_locations(cafile=ca_cert_file.name)
                self._temp_cert_files = [ca_cert_file.name]

        # Load client certificates from Variables (fallback for Secrets Backend)
        if extra_config.get("ssl_cert_secret") and extra_config.get("ssl_key_secret"):
            cert_content = Variable.get(extra_config["ssl_cert_secret"], None)
            key_content = Variable.get(extra_config["ssl_key_secret"], None)

            if cert_content and key_content:
                cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
                cert_file.write(cert_content)
                cert_file.close()

                key_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
                key_file.write(key_content)
                key_file.close()

                ssl_context.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)
                self._temp_cert_files.extend([cert_file.name, key_file.name])

        # Configure SSL protocols
        if extra_config.get("ssl_min_version"):
            min_version = getattr(ssl.TLSVersion, extra_config["ssl_min_version"], None)
            if min_version:
                ssl_context.minimum_version = min_version

        # For happybase, we need to use transport="framed" and protocol="compact" with SSL
        ssl_args["transport"] = "framed"
        ssl_args["protocol"] = "compact"

        # Store SSL context for potential future use
        self._ssl_context = ssl_context

        # Override port to SSL default if not specified
        if extra_config.get("ssl_port") and not extra_config.get("port_override"):
            ssl_args["port"] = extra_config.get("ssl_port")

        self.log.info("SSL/TLS enabled for Thrift connection")
        return ssl_args
