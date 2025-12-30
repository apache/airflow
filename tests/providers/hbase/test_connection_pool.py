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
"""Tests for HBase connection pool."""

from unittest.mock import Mock, patch

from airflow.providers.hbase.connection_pool import create_connection_pool, get_or_create_pool, _pools


class TestHBaseConnectionPool:
    """Test HBase connection pool."""

    def setup_method(self):
        """Clear global pools before each test."""
        _pools.clear()

    @patch('airflow.providers.hbase.connection_pool.happybase.ConnectionPool')
    def test_create_connection_pool(self, mock_pool_class):
        """Test create_connection_pool function."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        pool = create_connection_pool(5, host='localhost', port=9090)
        
        mock_pool_class.assert_called_once_with(5, host='localhost', port=9090)
        assert pool == mock_pool

    @patch('airflow.providers.hbase.connection_pool.happybase.ConnectionPool')
    def test_create_connection_pool_with_kerberos(self, mock_pool_class):
        """Test create_connection_pool with Kerberos (no additional params)."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # Kerberos auth returns empty dict, kinit handles authentication
        pool = create_connection_pool(
            10, 
            host='localhost', 
            port=9090
        )
        
        mock_pool_class.assert_called_once_with(
            10,
            host='localhost', 
            port=9090
        )
        assert pool == mock_pool

    @patch('airflow.providers.hbase.connection_pool.happybase.ConnectionPool')
    def test_get_or_create_pool_reuses_existing(self, mock_pool_class):
        """Test that get_or_create_pool reuses existing pools."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # First call creates pool
        pool1 = get_or_create_pool('test_conn', 5, host='localhost', port=9090)
        
        # Second call reuses same pool
        pool2 = get_or_create_pool('test_conn', 5, host='localhost', port=9090)
        
        # Should be the same pool instance
        assert pool1 is pool2
        # ConnectionPool should only be called once
        mock_pool_class.assert_called_once_with(5, host='localhost', port=9090)

    @patch('airflow.providers.hbase.connection_pool.happybase.ConnectionPool')
    def test_get_or_create_pool_different_conn_ids(self, mock_pool_class):
        """Test that different conn_ids get different pools."""
        mock_pool1 = Mock()
        mock_pool2 = Mock()
        mock_pool_class.side_effect = [mock_pool1, mock_pool2]
        
        # Different connection IDs should get different pools
        pool1 = get_or_create_pool('conn1', 5, host='localhost', port=9090)
        pool2 = get_or_create_pool('conn2', 5, host='localhost', port=9090)
        
        assert pool1 is not pool2
        assert mock_pool_class.call_count == 2