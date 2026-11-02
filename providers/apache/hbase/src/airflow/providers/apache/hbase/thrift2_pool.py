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
"""Thrift2 connection pool implementation."""

from __future__ import annotations

import atexit
import logging
import os
import queue
import threading
from contextlib import contextmanager, suppress
from typing import Any

from airflow.providers.apache.hbase.client import HBaseThrift2Client
from airflow.providers.apache.hbase.connection_config import create_connection_config

logger = logging.getLogger(__name__)

# Pool connection timeout in seconds
POOL_CONNECTION_TIMEOUT = float(os.getenv("HBASE_POOL_CONNECTION_TIMEOUT", "30.0"))


class Thrift2ConnectionPool:
    """Connection pool for HBase Thrift2 clients."""

    def __init__(
        self,
        size: int,
        host: str,
        port: int = 9090,
        timeout: int = 30000,
        ssl_options: dict[str, Any] | None = None,
        namespace: str = "default",
        retry_max_attempts: int = 3,
        retry_delay: float = 1.0,
        retry_backoff_factor: float = 2.0,
        use_http: bool = False,
        borrow_timeout: float | None = None,
    ):
        """
        Initialize connection pool.

        Args:
            size: Pool size
            host: HBase Thrift2 server host
            port: HBase Thrift2 server port
            timeout: Connection timeout in milliseconds
            ssl_options: SSL options dict (optional)
            namespace: HBase namespace (default 'default')
            retry_max_attempts: Maximum number of connection attempts
            retry_delay: Initial delay between retry attempts in seconds
            retry_backoff_factor: Multiplier for delay after each failed attempt
            use_http: Use HTTP transport instead of binary socket
        """
        self.size = size
        self.borrow_timeout = borrow_timeout if borrow_timeout is not None else POOL_CONNECTION_TIMEOUT
        self.config = create_connection_config(
            host=host,
            port=port,
            timeout=timeout,
            ssl_options=ssl_options,
            namespace=namespace,
            retry_max_attempts=retry_max_attempts,
            retry_delay=retry_delay,
            retry_backoff_factor=retry_backoff_factor,
            use_http=use_http,
        )
        self._pool: queue.Queue[HBaseThrift2Client] = queue.Queue(maxsize=size)
        self._semaphore = threading.Semaphore(size)

    def __del__(self):
        """Cleanup connections when pool is garbage collected."""
        try:
            self.close_all()
        except Exception:
            logger.debug("Failed to close connections during pool cleanup", exc_info=True)

    def _create_connection(self) -> HBaseThrift2Client:
        """Create new Thrift2 client connection."""
        client = HBaseThrift2Client(
            host=self.config.host,
            port=self.config.port,
            timeout=self.config.timeout,
            ssl_options=self.config.ssl_options,
            namespace=self.config.namespace,
            retry_max_attempts=self.config.retry_max_attempts,
            retry_delay=self.config.retry_delay,
            retry_backoff_factor=self.config.retry_backoff_factor,
            use_http=self.config.use_http,
        )
        client.open()
        return client

    def _is_connection_alive(self, client: HBaseThrift2Client) -> bool:
        """Check if connection is alive by testing request to server."""
        try:
            if client._client is None:
                return False
            # Test connection with lightweight request
            client._client.getTableNamesByPattern(regex=None, includeSysTables=False)
            return True
        except Exception as e:
            logger.debug("Connection check failed: %s", e)
            return False

    @contextmanager
    def connection(self, timeout: float | None = None):
        """
        Get connection from pool.

        Args:
            timeout: Timeout in seconds to wait for available connection from pool.
                Defaults to the borrow_timeout configured on the pool.
                Warning: If pool is exhausted and timeout is too low, requests may fail.
                Consider increasing timeout or pool size for high-load scenarios.

        Yields:
            HBaseThrift2Client instance
        """
        if timeout is None:
            timeout = self.borrow_timeout
        client = None

        try:
            # Try to get from pool or create new
            try:
                client = self._pool.get_nowait()
            except queue.Empty:
                acquired = self._semaphore.acquire(blocking=False)  # Check limit
                if acquired:
                    logger.debug("Creating new connection")
                    try:
                        client = self._create_connection()
                    except Exception as e:
                        self._semaphore.release()
                        logger.error("Failed to create connection: %s", e)
                        acquired = False
                if not acquired:
                    logger.debug("Pool exhausted, waiting...")
                    client = self._pool.get(timeout=timeout)

            # Check if connection is alive, reconnect if needed
            if client and not self._is_connection_alive(client):
                logger.warning("Connection is dead, reconnecting...")
                with suppress(Exception):
                    client.close()
                client.open()

            yield client

        except Exception as e:
            logger.error("Connection error: %s", e)
            if client:
                with suppress(Exception):
                    client.close()
                self._semaphore.release()
            raise
        if client:
            self._pool.put(client)

    def close_all(self):
        """Close all connections in pool."""
        while not self._pool.empty():
            try:
                client = self._pool.get_nowait()
                client.close()
            except queue.Empty:
                break


# Global pool storage
_thrift2_pools: dict[str, Thrift2ConnectionPool] = {}
_pool_lock = threading.Lock()


def _cleanup_pools() -> None:
    """Cleanup all pools on exit."""
    with _pool_lock:
        for pool in _thrift2_pools.values():
            pool.close_all()
        _thrift2_pools.clear()


def get_or_create_thrift2_pool(
    conn_id: str,
    pool_size: int,
    host: str,
    port: int = 9090,
    timeout: int = 30000,
    ssl_options: dict[str, Any] | None = None,
    namespace: str = "default",
    retry_max_attempts: int = 3,
    retry_delay: float = 1.0,
    retry_backoff_factor: float = 2.0,
    use_http: bool = False,
    borrow_timeout: float | None = None,
) -> Thrift2ConnectionPool:
    """
    Get existing Thrift2 pool or create new one.

    Args:
        conn_id: Connection ID
        pool_size: Pool size
        host: HBase Thrift2 server host
        port: HBase Thrift2 server port
        timeout: Connection timeout in milliseconds
        ssl_options: SSL options dict (optional)
        namespace: HBase namespace (default 'default')
        retry_max_attempts: Maximum number of connection attempts
        retry_delay: Initial delay between retry attempts in seconds
        retry_backoff_factor: Multiplier for delay after each failed attempt
        use_http: Use HTTP transport instead of binary socket

    Returns:
        Thrift2ConnectionPool instance
    """
    with _pool_lock:
        if conn_id not in _thrift2_pools:
            _thrift2_pools[conn_id] = Thrift2ConnectionPool(
                size=pool_size,
                host=host,
                port=port,
                timeout=timeout,
                ssl_options=ssl_options,
                namespace=namespace,
                retry_max_attempts=retry_max_attempts,
                retry_delay=retry_delay,
                retry_backoff_factor=retry_backoff_factor,
                use_http=use_http,
                borrow_timeout=borrow_timeout,
            )
        return _thrift2_pools[conn_id]


# Register cleanup on exit
atexit.register(_cleanup_pools)
