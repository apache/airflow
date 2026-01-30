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

from airflow.models import Connection
from airflow.providers.hbase.hooks.hbase import HBaseHook


class TestHBaseHook:
    """Test HBase hook - Thrift2 only architecture."""

    def test_get_ui_field_behaviour(self):
        """Test get_ui_field_behaviour method."""
        result = HBaseHook.get_ui_field_behaviour()
        assert "hidden_fields" in result
        assert "relabeling" in result
        assert "placeholders" in result
        assert result["hidden_fields"] == ["schema"]
        assert result["relabeling"]["host"] == "HBase Thrift2 Server Host"
        assert result["placeholders"]["host"] == "localhost"

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_strategy_creation_single(self, mock_get_connection, mock_open):
        """Test strategy creation for single connection."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        strategy = hook._get_strategy()

        assert strategy is not None
        from airflow.providers.hbase.hooks.hbase_strategy import Thrift2Strategy
        assert isinstance(strategy, Thrift2Strategy)
        mock_open.assert_called_once()

    @patch("airflow.providers.hbase.hooks.hbase.get_or_create_thrift2_pool")
    @patch.object(HBaseHook, "get_connection")
    def test_strategy_creation_pooled(self, mock_get_connection, mock_pool):
        """Test strategy creation for pooled connection."""
        mock_conn = Connection(
            conn_id="hbase_pooled",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"connection_pool": {"enabled": true, "size": 5}}'
        )
        mock_get_connection.return_value = mock_conn
        mock_pool.return_value = MagicMock()

        hook = HBaseHook()
        strategy = hook._get_strategy()

        assert strategy is not None
        from airflow.providers.hbase.hooks.hbase_strategy import PooledThrift2Strategy
        assert isinstance(strategy, PooledThrift2Strategy)
        mock_pool.assert_called_once()

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_table_exists(self, mock_get_connection, mock_open):
        """Test table_exists method."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        
        # Mock the strategy's table_exists method
        with patch.object(hook._get_strategy(), 'table_exists', return_value=True) as mock_table_exists:
            result = hook.table_exists("test_table")
            assert result is True
            mock_table_exists.assert_called_once_with("test_table")

    def test_get_openlineage_database_info(self):
        """Test get_openlineage_database_info method."""
        hook = HBaseHook()
        mock_connection = MagicMock()
        mock_connection.host = "localhost"
        mock_connection.port = 9090

        result = hook.get_openlineage_database_info(mock_connection)

        if result:  # Only test if OpenLineage is available
            assert result.scheme == "hbase"
            assert result.authority == "localhost:9090"
            assert result.database == "default"


class TestRetryLogic:
    """Test retry logic functionality."""

    def test_get_retry_config_defaults(self):
        """Test _get_retry_config with default values."""
        hook = HBaseHook()
        config = hook._get_retry_config({})

        assert config["retry_max_attempts"] == 3
        assert config["retry_delay"] == 1.0
        assert config["retry_backoff_factor"] == 2.0

    def test_get_retry_config_custom_values(self):
        """Test _get_retry_config with custom values."""
        hook = HBaseHook()
        extra_config = {
            "retry_max_attempts": 5,
            "retry_delay": 2.5,
            "retry_backoff_factor": 1.5
        }
        config = hook._get_retry_config(extra_config)

        assert config["retry_max_attempts"] == 5
        assert config["retry_delay"] == 2.5
        assert config["retry_backoff_factor"] == 1.5

    def test_retry_in_client(self):
        """Test retry logic is applied in Thrift2 client."""
        from airflow.providers.hbase.client.thrift2_client import HBaseThrift2Client
        
        # Create client with retry config
        client = HBaseThrift2Client(
            host="localhost",
            port=9090,
            retry_max_attempts=3,
            retry_delay=0.01,
            retry_backoff_factor=1.0
        )
        
        # Verify retry parameters are set
        assert client.retry_max_attempts == 3
        assert client.retry_delay == 0.01
        assert client.retry_backoff_factor == 1.0


class TestHBaseHookMethods:
    """Test HBase hook methods."""

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_create_table(self, mock_get_connection, mock_open):
        """Test create_table method."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        
        with patch.object(hook._get_strategy(), 'create_table') as mock_create:
            hook.create_table("test_table", {"cf1": {}})
            mock_create.assert_called_once_with("test_table", {"cf1": {}})

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_delete_table(self, mock_get_connection, mock_open):
        """Test delete_table method."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        
        with patch.object(hook._get_strategy(), 'delete_table') as mock_delete:
            hook.delete_table("test_table", disable=True)
            mock_delete.assert_called_once_with("test_table", True)

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_put_row(self, mock_get_connection, mock_open):
        """Test put_row method."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        
        with patch.object(hook._get_strategy(), 'put_row') as mock_put:
            hook.put_row("test_table", "row1", {"cf1:col1": "value1"})
            mock_put.assert_called_once_with("test_table", "row1", {"cf1:col1": "value1"})

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_get_row(self, mock_get_connection, mock_open):
        """Test get_row method."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        
        with patch.object(hook._get_strategy(), 'get_row', return_value={"cf1:col1": "value1"}) as mock_get:
            result = hook.get_row("test_table", "row1")
            assert result == {"cf1:col1": "value1"}
            mock_get.assert_called_once_with("test_table", "row1", None)

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_scan_table(self, mock_get_connection, mock_open):
        """Test scan_table method."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        
        expected = [("row1", {"cf1:col1": "value1"})]
        with patch.object(hook._get_strategy(), 'scan_table', return_value=expected) as mock_scan:
            result = hook.scan_table("test_table", row_start="row1", limit=10)
            assert result == expected
            mock_scan.assert_called_once_with("test_table", "row1", None, None, 10)

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_batch_operations(self, mock_get_connection, mock_open):
        """Test batch operations."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        
        # Test batch_put_rows
        with patch.object(hook._get_strategy(), 'batch_put_rows') as mock_batch_put:
            rows = [{"row_key": "row1", "cf1:col1": "value1"}]
            hook.batch_put_rows("test_table", rows, batch_size=100, max_workers=2)
            mock_batch_put.assert_called_once_with("test_table", rows, 100, 2)
        
        # Test batch_get_rows
        with patch.object(hook._get_strategy(), 'batch_get_rows', return_value=[{"cf1:col1": "value1"}]) as mock_batch_get:
            result = hook.batch_get_rows("test_table", ["row1", "row2"])
            assert result == [{"cf1:col1": "value1"}]
            mock_batch_get.assert_called_once_with("test_table", ["row1", "row2"], None)
        
        # Test batch_delete_rows
        with patch.object(hook._get_strategy(), 'batch_delete_rows') as mock_batch_delete:
            hook.batch_delete_rows("test_table", ["row1", "row2"], batch_size=100)
            mock_batch_delete.assert_called_once_with("test_table", ["row1", "row2"], 100)

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_close(self, mock_get_connection, mock_open):
        """Test close method."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        strategy = hook._get_strategy()
        
        # Mock client close
        with patch.object(strategy.client, 'close') as mock_close:
            hook.close()
            mock_close.assert_called_once()


class TestSSLConfiguration:
    """Test SSL/TLS configuration."""

    @patch("airflow.providers.hbase.hooks.hbase.create_thrift2_ssl_context")
    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_ssl_enabled(self, mock_get_connection, mock_open, mock_ssl_context):
        """Test SSL is enabled when configured."""
        mock_conn = Connection(
            conn_id="hbase_ssl",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"use_ssl": true, "ssl_verify_mode": "CERT_REQUIRED"}'
        )
        mock_get_connection.return_value = mock_conn
        mock_ssl_context.return_value = (MagicMock(), [])

        hook = HBaseHook()
        hook._get_strategy()
        
        mock_ssl_context.assert_called_once()

    @patch("airflow.providers.hbase.client.thrift2_client.HBaseThrift2Client.open")
    @patch.object(HBaseHook, "get_connection")
    def test_ssl_disabled_by_default(self, mock_get_connection, mock_open):
        """Test SSL is disabled by default."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()
        strategy = hook._get_strategy()
        
        # Verify client was created without SSL
        assert strategy.client.ssl_context is None


class TestPoolConfiguration:
    """Test connection pool configuration."""

    def test_get_pool_config_defaults(self):
        """Test _get_pool_config with default values."""
        hook = HBaseHook()
        config = hook._get_pool_config({})

        assert config["enabled"] is False
        assert config["size"] == 10
        assert config["timeout"] == 30

    def test_get_pool_config_custom_values(self):
        """Test _get_pool_config with custom values."""
        hook = HBaseHook()
        extra_config = {
            "connection_pool": {
                "enabled": True,
                "size": 20,
                "timeout": 60
            }
        }
        config = hook._get_pool_config(extra_config)

        assert config["enabled"] is True
        assert config["size"] == 20
        assert config["timeout"] == 60
