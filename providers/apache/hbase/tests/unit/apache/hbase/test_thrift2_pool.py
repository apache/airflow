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

from __future__ import annotations

from unittest.mock import MagicMock, patch

from airflow.providers.apache.hbase.thrift2_pool import Thrift2ConnectionPool


class TestThrift2ConnectionPool:
    """Test Thrift2 connection pool."""

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_initialization(self, mock_client_class):
        """Test pool initialization."""
        pool = Thrift2ConnectionPool(size=5, host="localhost", port=9090, timeout=30000)

        assert pool.size == 5
        assert pool.config.host == "localhost"
        assert pool.config.port == 9090
        assert pool.config.timeout == 30000

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_get_connection_lazy_creation(self, mock_client_class):
        """Test lazy connection creation."""
        mock_client = MagicMock()
        mock_client._client = MagicMock()  # Simulate alive connection
        mock_client_class.return_value = mock_client

        pool = Thrift2ConnectionPool(size=2, host="localhost", port=9090)

        # Use context manager to get connection
        with pool.connection() as conn:
            mock_client.open.assert_called()
            assert conn == mock_client

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_with_ssl(self, mock_client_class):
        """Test pool with SSL options."""
        ssl_options = {"ca_certs": "/path/to/ca.crt", "validate": True}
        mock_client = MagicMock()
        mock_client._client = MagicMock()
        mock_client_class.return_value = mock_client

        pool = Thrift2ConnectionPool(
            size=2, host="localhost", port=9090, ssl_options=ssl_options, use_http=True
        )

        with pool.connection():
            # Verify SSL options were passed to client
            mock_client_class.assert_called_with(
                host="localhost",
                port=9090,
                timeout=30000,
                ssl_options=ssl_options,
                namespace="default",
                retry_max_attempts=3,
                retry_delay=1.0,
                retry_backoff_factor=2.0,
                use_http=True,
            )

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_with_retry_config(self, mock_client_class):
        """Test pool with custom retry configuration."""
        mock_client = MagicMock()
        mock_client._client = MagicMock()
        mock_client_class.return_value = mock_client

        pool = Thrift2ConnectionPool(
            size=2,
            host="localhost",
            port=9090,
            retry_max_attempts=5,
            retry_delay=2.0,
            retry_backoff_factor=3.0,
        )

        with pool.connection():
            # Verify retry config was passed to client
            mock_client_class.assert_called_with(
                host="localhost",
                port=9090,
                timeout=30000,
                ssl_options=None,
                namespace="default",
                retry_max_attempts=5,
                retry_delay=2.0,
                retry_backoff_factor=3.0,
                use_http=False,
            )

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_borrow_timeout_from_config(self, mock_client_class):
        """Test that borrow_timeout is stored and used as default in connection()."""
        pool = Thrift2ConnectionPool(
            size=1,
            host="localhost",
            port=9090,
            borrow_timeout=15.0,
        )

        assert pool.borrow_timeout == 15.0

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_borrow_timeout_defaults_to_global(self, mock_client_class):
        """Test that borrow_timeout falls back to POOL_CONNECTION_TIMEOUT when not set."""
        from airflow.providers.apache.hbase.thrift2_pool import POOL_CONNECTION_TIMEOUT

        pool = Thrift2ConnectionPool(
            size=1,
            host="localhost",
            port=9090,
        )

        assert pool.borrow_timeout == POOL_CONNECTION_TIMEOUT


class TestIsConnectionAlive:
    """Test _is_connection_alive uses correct Thrift2 API method"""

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_calls_correct_thrift2_method(self, mock_client_class):
        """Must use getTableNamesByPattern"""
        mock_client = MagicMock()
        mock_client._client = MagicMock()
        mock_client_class.return_value = mock_client

        pool = Thrift2ConnectionPool(size=1, host="localhost", port=9090)

        assert pool._is_connection_alive(mock_client) is True
        mock_client._client.getTableNamesByPattern.assert_called_once_with(regex=None, includeSysTables=False)

        # Dead connection
        mock_client._client.getTableNamesByPattern.side_effect = Exception("broken")
        assert pool._is_connection_alive(mock_client) is False

        # None client
        mock_client._client = None
        assert pool._is_connection_alive(mock_client) is False

    @patch("airflow.providers.apache.hbase.thrift2_pool.HBaseThrift2Client")
    def test_alive_skips_reconnect_dead_triggers_reconnect(self, mock_client_class):
        """Alive connection must not call open(), dead connection must call open() again"""
        mock_client = MagicMock()
        mock_client._client = MagicMock()
        mock_client_class.return_value = mock_client

        pool = Thrift2ConnectionPool(size=1, host="localhost", port=9090)

        # First borrow: creates connection (open called once)
        with pool.connection():
            pass
        assert mock_client.open.call_count == 1

        # Second borrow: alive, no extra open()
        with pool.connection():
            pass
        assert mock_client.open.call_count == 1

        # Simulate dead connection
        mock_client._client.getTableNamesByPattern.side_effect = Exception("dead")

        # Third borrow: dead, reconnect (open called again)
        with pool.connection():
            pass
        assert mock_client.open.call_count == 2
