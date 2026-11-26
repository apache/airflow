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
import ssl as ssl_module
import time
from typing import Any

from thrift.protocol import TBinaryProtocol
from thrift.transport import THttpClient, TSocket, TSSLSocket, TTransport
from thrift.transport.TTransport import TTransportException

from airflow.providers.apache.hbase.connection_config import create_connection_config
from airflow.providers.apache.hbase.hbase_thrift2_generated import THBaseService, ttypes

logger = logging.getLogger(__name__)


class HBaseThrift2Client:
    """Lightweight HBase Thrift2 client."""

    def __init__(
        self,
        host: str,
        port: int = 9090,
        timeout: int = 30000,
        ssl_options: dict[str, Any] | None = None,
        namespace: str = "default",
        retry_max_attempts: int = 3,
        retry_delay: float = 1.0,
        retry_backoff_factor: float = 2.0,
        use_http: bool = False,
    ):
        """
        Initialize Thrift2 client.

        Args:
            host: HBase Thrift2 server host
            port: HBase Thrift2 server port
            timeout: Connection timeout in milliseconds
            ssl_options: SSL options dict with keys:
                ca_certs, cert_file, key_file, validate (optional)
            namespace: HBase namespace (default 'default')
            retry_max_attempts: Maximum number of connection attempts
            retry_delay: Initial delay between retry attempts in seconds
            retry_backoff_factor: Multiplier for delay after each failed attempt
            use_http: Use HTTP transport instead of binary socket
        """
        self.use_http = use_http
        self.config = create_connection_config(
            host=host,
            port=port,
            timeout=timeout,
            ssl_options=ssl_options,
            namespace=namespace,
            retry_max_attempts=retry_max_attempts,
            retry_delay=retry_delay,
            retry_backoff_factor=retry_backoff_factor,
        )
        self._client: THBaseService.Client | None = None
        self._transport: Any = None

        logger.debug("HBaseThrift2Client initialized with ssl_options: %s", ssl_options)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _create_ssl_context(self) -> ssl_module.SSLContext | None:
        """
        Create SSL context from connection config.

        Returns:
            Configured SSLContext or None if SSL is not configured.
        """
        if not self.config.ssl_options:
            return None

        validate = self.config.ssl_options.get("validate", True)
        ca_certs = self.config.ssl_options.get("ca_certs")

        if validate and ca_certs:
            ctx = ssl_module.create_default_context(cafile=ca_certs)
        elif validate:
            ctx = ssl_module.create_default_context()
        else:
            ctx = ssl_module.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl_module.CERT_NONE

        cert_file = self.config.ssl_options.get("cert_file")
        key_file = self.config.ssl_options.get("key_file")
        if cert_file:
            ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)

        return ctx

    def _create_socket(self) -> TSocket.TSocket | TSSLSocket.TSSLSocket:
        """
        Create socket (SSL or regular).

        Returns:
            Configured socket instance
        """
        if self.config.ssl_options:
            ssl_params = {
                "host": self.config.host,
                "port": self.config.port,
                "ssl_context": self._create_ssl_context(),
            }
            sock = TSSLSocket.TSSLSocket(**ssl_params)
        else:
            sock = TSocket.TSocket(self.config.host, self.config.port)

        sock.setTimeout(self.config.timeout)
        return sock

    def _setup_simple_transport(self, sock: TSocket.TSocket | TSSLSocket.TSSLSocket) -> None:
        """
        Set up simple transport without authentication.

        Args:
            sock: Base socket

        Raises:
            Exception: If both transport types fail
        """
        for transport_type in ["buffered", "framed"]:
            try:
                if transport_type == "buffered":
                    self._transport = TTransport.TBufferedTransport(sock)
                else:
                    self._transport = TTransport.TFramedTransport(sock)

                protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
                self._client = THBaseService.Client(protocol)
                self._transport.open()

                # Test connection
                client = self._client
                if client is None:
                    raise RuntimeError("HBase Thrift2 client is not initialized")
                client.getTableNamesByPattern(regex=None, includeSysTables=False)

                logger.info(
                    "Successfully connected to HBase Thrift2 at %s:%s (SSL: %s, Transport: %s)",
                    self.config.host,
                    self.config.port,
                    bool(self.config.ssl_options),
                    transport_type,
                )
                return
            except (TTransportException, OSError) as transport_error:
                logger.debug("Transport %s failed: %s", transport_type, transport_error)
                if hasattr(self, "_transport") and self._transport:
                    try:
                        self._transport.close()
                    except (TTransportException, OSError):
                        logger.debug("Failed to close transport during cleanup")
                if transport_type == "framed":
                    raise transport_error

    def _test_connection(self) -> None:
        """
        Test connection with a simple operation.

        Raises:
            Exception: If connection test fails
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        self._client.getTableNamesByPattern(regex=None, includeSysTables=False)

    def open(self) -> None:
        """
        Open connection to Thrift2 server with retry logic.

        Raises:
            ConnectionError: If connection fails after all retries
            TimeoutError: If connection times out
            RuntimeError: If Kerberos authentication fails
            OSError: If socket operation fails
        """
        last_exception = None

        for attempt in range(self.config.retry_max_attempts):
            try:
                if self.use_http:
                    # HTTP transport
                    scheme = "https" if self.config.ssl_options else "http"
                    uri = f"{scheme}://{self.config.host}:{self.config.port}"

                    http_client = THttpClient.THttpClient(uri, ssl_context=self._create_ssl_context())
                    http_client.setTimeout(self.config.timeout)

                    self._transport = http_client
                    protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
                    self._client = THBaseService.Client(protocol)
                    self._transport.open()
                    self._test_connection()
                    logger.info(
                        "Successfully connected to HBase Thrift2 at %s (HTTP, SSL: %s)",
                        uri,
                        bool(self.config.ssl_options),
                    )
                else:
                    # Socket transport
                    sock = self._create_socket()
                    self._setup_simple_transport(sock)

                return

            except (ConnectionError, TimeoutError, OSError, TTransportException) as e:
                last_exception = e
                if attempt == self.config.retry_max_attempts - 1:
                    logger.error(
                        "All %d connection attempts failed. Last error: %s", self.config.retry_max_attempts, e
                    )
                    raise e

                wait_time = self.config.retry_delay * (self.config.retry_backoff_factor**attempt)
                logger.warning(
                    "Connection attempt %d/%d failed: %s. Retrying in %.1fs...",
                    attempt + 1,
                    self.config.retry_max_attempts,
                    e,
                    wait_time,
                )
                time.sleep(wait_time)

        if last_exception:
            raise last_exception

    def close(self):
        """Close connection."""
        if hasattr(self, "_transport") and self._transport:
            self._transport.close()
            self._transport = None
        self._client = None

    def list_tables(self) -> list[str]:
        """List all tables."""
        if not self._client:
            raise RuntimeError("Client not connected")
        table_names = self._client.getTableNamesByPattern(regex=None, includeSysTables=False)
        return [tn.qualifier.decode() for tn in table_names]

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        if not self._client:
            raise RuntimeError("Client not connected")
        table_name_obj = ttypes.TTableName(ns=self.config.namespace.encode(), qualifier=table_name.encode())
        return self._client.tableExists(table_name_obj)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """
        Create table.

        Args:
            table_name: Name of the table
            families: Dictionary of column families
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        table_name_obj = ttypes.TTableName(ns=self.config.namespace.encode(), qualifier=table_name.encode())

        column_families = []
        for family_name in families.keys():
            col_desc = ttypes.TColumnFamilyDescriptor(name=family_name.encode())
            column_families.append(col_desc)

        table_desc = ttypes.TTableDescriptor(tableName=table_name_obj, columns=column_families)

        self._client.createTable(table_desc, None)

    def delete_table(self, table_name: str) -> None:
        """
        Delete table.

        Args:
            table_name: Name of the table
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        table_name_obj = ttypes.TTableName(ns=self.config.namespace.encode(), qualifier=table_name.encode())

        try:
            # Check if table is enabled before disabling
            if self._client.isTableEnabled(table_name_obj):
                logger.info("Disabling table %s", table_name)
                self._client.disableTable(table_name_obj)
            else:
                logger.info("Table %s is already disabled", table_name)

            # Delete table
            logger.info("Deleting table %s", table_name)
            self._client.deleteTable(table_name_obj)
            logger.info("Successfully deleted table %s", table_name)
        except Exception as e:
            logger.exception("Failed to delete table %s: %s", table_name, e)
            raise

    def put(self, table_name: str, row_key: str, data: dict[str, str]) -> None:
        """
        Put data into table.

        Args:
            table_name: Name of the table
            row_key: Row key
            data: Dictionary of column:value pairs (format: "family:qualifier": "value")
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        column_values = []
        for column, value in data.items():
            family, qualifier = column.split(":", 1)
            col_val = ttypes.TColumnValue(
                family=family.encode(),
                qualifier=qualifier.encode(),
                value=value.encode() if isinstance(value, str) else value,
            )
            column_values.append(col_val)

        tput = ttypes.TPut(row=row_key.encode(), columnValues=column_values)

        # Use table name as bytes, not TTableName object
        self._client.put(self._resolve_table_name(table_name), tput)

    def put_multiple(self, table_name: str, puts: list[tuple[str, dict[str, str]]]) -> None:
        """
        Put multiple rows in batch.

        Args:
            table_name: Name of the table
            puts: List of (row_key, data) tuples
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        tputs = []
        for row_key, data in puts:
            column_values = []
            for column, value in data.items():
                family, qualifier = column.split(":", 1)
                col_val = ttypes.TColumnValue(
                    family=family.encode(),
                    qualifier=qualifier.encode(),
                    value=value.encode() if isinstance(value, str) else value,
                )
                column_values.append(col_val)

            tput = ttypes.TPut(row=row_key.encode(), columnValues=column_values)
            tputs.append(tput)

        self._client.putMultiple(self._resolve_table_name(table_name), tputs)

    def get(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """
        Get row from table.

        Args:
            table_name: Name of the table
            row_key: Row key
            columns: List of columns to retrieve (format: "family:qualifier")

        Returns:
            Dictionary with row data
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        tget = ttypes.TGet(row=row_key.encode())

        if columns:
            tget.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = ttypes.TColumn(family=family.encode(), qualifier=qualifier.encode())
                tget.columns.append(tcol)

        result = self._client.get(self._resolve_table_name(table_name), tget)
        return self._parse_result(result)

    def get_multiple(
        self, table_name: str, row_keys: list[str], columns: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """
        Get multiple rows in batch.

        Args:
            table_name: Name of the table
            row_keys: List of row keys
            columns: List of columns to retrieve

        Returns:
            List of row data dictionaries
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        tgets = []
        for row_key in row_keys:
            tget = ttypes.TGet(row=row_key.encode())

            if columns:
                tget.columns = []
                for column in columns:
                    family, qualifier = column.split(":", 1)
                    tcol = ttypes.TColumn(family=family.encode(), qualifier=qualifier.encode())
                    tget.columns.append(tcol)

            tgets.append(tget)

        results = self._client.getMultiple(self._resolve_table_name(table_name), tgets)
        return [self._parse_result(r) for r in results]

    def delete(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """
        Delete row or columns.

        Args:
            table_name: Name of the table
            row_key: Row key
            columns: List of columns to delete (if None, deletes entire row)
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        tdelete = ttypes.TDelete(row=row_key.encode())

        if columns:
            tdelete.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = ttypes.TColumn(family=family.encode(), qualifier=qualifier.encode())
                tdelete.columns.append(tcol)

        self._client.deleteSingle(self._resolve_table_name(table_name), tdelete)

    def delete_multiple(self, table_name: str, deletes: list[tuple[str, list[str] | None]]) -> None:
        """
        Delete multiple rows in batch.

        Args:
            table_name: Name of the table
            deletes: List of (row_key, columns) tuples
        """
        if not self._client:
            raise RuntimeError("Client not connected")
        tdeletes = []
        for row_key, columns in deletes:
            tdelete = ttypes.TDelete(row=row_key.encode())

            if columns:
                tdelete.columns = []
                for column in columns:
                    family, qualifier = column.split(":", 1)
                    tcol = ttypes.TColumn(family=family.encode(), qualifier=qualifier.encode())
                    tdelete.columns.append(tcol)

            tdeletes.append(tdelete)

        self._client.deleteMultiple(self._resolve_table_name(table_name), tdeletes)

    def scan(
        self,
        table_name: str,
        start_row: str | None = None,
        stop_row: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Scan table.

        Args:
            table_name: Name of the table
            start_row: Start row key
            stop_row: Stop row key
            columns: List of columns to retrieve
            limit: Maximum number of rows

        Returns:
            List of row data dictionaries
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        tscan = ttypes.TScan()

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
                tcol = ttypes.TColumn(family=family.encode(), qualifier=qualifier.encode())
                tscan.columns.append(tcol)

        scanner_id = self._client.openScanner(self._resolve_table_name(table_name), tscan)

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

    def _resolve_table_name(self, table_name: str) -> bytes:
        """
        Resolve table name with namespace prefix.

        If table_name already contains ':' (fully qualified), it is used as-is.
        Otherwise, the configured namespace is prepended when it differs from 'default'.
        """
        if ":" in table_name:
            return table_name.encode()
        if self.config.namespace and self.config.namespace != "default":
            return f"{self.config.namespace}:{table_name}".encode()
        return table_name.encode()

    def _parse_result(self, result) -> dict[str, Any]:
        """
        Parse Thrift2 result to dictionary.

        Args:
            result: TResult object from Thrift2

        Returns:
            Dictionary with parsed data
        """
        if not result or not result.columnValues:
            return {}

        parsed: dict[str, Any] = {"row": result.row.decode() if result.row else None, "columns": {}}

        for col_val in result.columnValues:
            family = col_val.family.decode()
            qualifier = col_val.qualifier.decode()
            value = col_val.value
            timestamp = col_val.timestamp if hasattr(col_val, "timestamp") else None

            column_name = f"{family}:{qualifier}"
            parsed["columns"][column_name] = {"value": value, "timestamp": timestamp}

        return parsed
