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

import queue
import threading
from contextlib import contextmanager
from typing import Any

from airflow.providers.hbase.client import HBaseThrift2Client


class Thrift2ConnectionPool:
    """Connection pool for HBase Thrift2 clients."""

    def __init__(self, size: int, host: str, port: int = 9090, timeout: int = 30000, 
                 ssl_context=None, retry_max_attempts: int = 3, retry_delay: float = 1.0, 
                 retry_backoff_factor: float = 2.0):
        """Initialize connection pool.
        
        Args:
            size: Pool size
            host: HBase Thrift2 server host
            port: HBase Thrift2 server port (default 9090 for Arenadata/Apache HBase)
            timeout: Connection timeout in milliseconds
            ssl_context: SSL context for secure connections (optional)
            retry_max_attempts: Maximum number of connection attempts
            retry_delay: Initial delay between retry attempts in seconds
            retry_backoff_factor: Multiplier for delay after each failed attempt
        """
        self.size = size
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.retry_max_attempts = retry_max_attempts
        self.retry_delay = retry_delay
        self.retry_backoff_factor = retry_backoff_factor
        self._pool = queue.Queue(maxsize=size)
        self._lock = threading.Lock()
        self._created = 0

    def _create_connection(self) -> HBaseThrift2Client:
        """Create new Thrift2 client connection."""
        client = HBaseThrift2Client(
            host=self.host, 
            port=self.port, 
            timeout=self.timeout,
            ssl_context=self.ssl_context,
            retry_max_attempts=self.retry_max_attempts,
            retry_delay=self.retry_delay,
            retry_backoff_factor=self.retry_backoff_factor
        )
        client.open()
        return client

    def _is_connection_alive(self, client: HBaseThrift2Client) -> bool:
        """Check if connection is alive."""
        try:
            # Check if client has active connection
            return client._client is not None
        except Exception:
            return False

    @contextmanager
    def connection(self, timeout: float = 30.0):
        """Get connection from pool.
        
        Args:
            timeout: Timeout to wait for available connection
            
        Yields:
            HBaseThrift2Client instance
        """
        client = None
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            # Try to get from pool or create new
            try:
                client = self._pool.get_nowait()
            except queue.Empty:
                with self._lock:
                    if self._created < self.size:
                        self._created += 1
                        logger.debug(f"Creating new connection ({self._created}/{self.size})")
                        try:
                            client = self._create_connection()
                        except Exception as e:
                            self._created -= 1
                            logger.error(f"Failed to create connection: {e}")
                            raise
                    else:
                        logger.debug(f"Pool exhausted, waiting...")
                
                if client is None:
                    client = self._pool.get(timeout=timeout)
            
            # Check if connection is alive, reconnect if needed
            if not self._is_connection_alive(client):
                logger.warning("Connection is dead, reconnecting...")
                try:
                    client.close()
                except Exception:
                    pass
                client.open()
            
            yield client
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            if client:
                try:
                    client.close()
                except Exception:
                    pass
                with self._lock:
                    self._created -= 1
            raise
        else:
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


def get_or_create_thrift2_pool(
    conn_id: str, 
    pool_size: int, 
    host: str, 
    port: int = 9090, 
    timeout: int = 30000,
    ssl_context=None,
    retry_max_attempts: int = 3,
    retry_delay: float = 1.0,
    retry_backoff_factor: float = 2.0
) -> Thrift2ConnectionPool:
    """Get existing Thrift2 pool or create new one.
    
    Args:
        conn_id: Connection ID
        pool_size: Pool size
        host: HBase Thrift2 server host
        port: HBase Thrift2 server port
        timeout: Connection timeout in milliseconds
        ssl_context: SSL context for secure connections (optional)
        retry_max_attempts: Maximum number of connection attempts
        retry_delay: Initial delay between retry attempts in seconds
        retry_backoff_factor: Multiplier for delay after each failed attempt
        
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
                ssl_context=ssl_context,
                retry_max_attempts=retry_max_attempts,
                retry_delay=retry_delay,
                retry_backoff_factor=retry_backoff_factor
            )
        return _thrift2_pools[conn_id]
