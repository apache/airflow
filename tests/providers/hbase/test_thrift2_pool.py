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

from unittest.mock import MagicMock, patch

from airflow.providers.hbase.thrift2_pool import Thrift2ConnectionPool, get_or_create_thrift2_pool


class TestThrift2ConnectionPool:
    """Test Thrift2 connection pool."""

    @patch("airflow.providers.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_initialization(self, mock_client_class):
        """Test pool initialization."""
        pool = Thrift2ConnectionPool(
            size=5,
            host="localhost",
            port=9090,
            timeout=30000
        )
        
        assert pool.size == 5
        assert pool.host == "localhost"
        assert pool.port == 9090
        assert pool.timeout == 30000

    @patch("airflow.providers.hbase.thrift2_pool.HBaseThrift2Client")
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

    @patch("airflow.providers.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_with_ssl(self, mock_client_class):
        """Test pool with SSL context."""
        ssl_context = MagicMock()
        mock_client = MagicMock()
        mock_client._client = MagicMock()
        mock_client_class.return_value = mock_client
        
        pool = Thrift2ConnectionPool(
            size=2,
            host="localhost",
            port=9090,
            ssl_context=ssl_context
        )
        
        with pool.connection():
            # Verify SSL context was passed to client
            mock_client_class.assert_called_with(
                host="localhost",
                port=9090,
                timeout=30000,
                ssl_context=ssl_context,
                retry_max_attempts=3,
                retry_delay=1.0,
                retry_backoff_factor=2.0
            )

    @patch("airflow.providers.hbase.thrift2_pool.HBaseThrift2Client")
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
            retry_backoff_factor=3.0
        )
        
        with pool.connection():
            # Verify retry config was passed to client
            mock_client_class.assert_called_with(
                host="localhost",
                port=9090,
                timeout=30000,
                ssl_context=None,
                retry_max_attempts=5,
                retry_delay=2.0,
                retry_backoff_factor=3.0
            )
