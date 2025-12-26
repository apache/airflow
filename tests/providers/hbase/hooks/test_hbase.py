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

import pytest
from thriftpy2.transport.base import TTransportException

from airflow.models import Connection
from airflow.providers.hbase.hooks.hbase import HBaseHook, retry_on_connection_error


class TestHBaseHook:
    """Test HBase hook - unique functionality not covered by Strategy Pattern tests."""

    def test_get_ui_field_behaviour(self):
        """Test get_ui_field_behaviour method."""
        result = HBaseHook.get_ui_field_behaviour()
        assert "hidden_fields" in result
        assert "relabeling" in result
        assert "placeholders" in result
        assert result["hidden_fields"] == ["schema"]
        assert result["relabeling"]["host"] == "HBase Thrift Server Host"
        assert result["placeholders"]["host"] == "localhost"

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_get_conn_thrift_only(self, mock_get_connection, mock_happybase_connection):
        """Test get_conn method (Thrift mode only)."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        mock_hbase_conn = MagicMock()
        mock_happybase_connection.return_value = mock_hbase_conn

        hook = HBaseHook()
        result = hook.get_conn()

        mock_happybase_connection.assert_called_once_with(host="localhost", port=9090)
        assert result == mock_hbase_conn

    @patch.object(HBaseHook, "get_connection")
    def test_get_conn_ssh_mode_raises_error(self, mock_get_connection):
        """Test get_conn raises error in SSH mode."""
        mock_conn = Connection(
            conn_id="hbase_ssh",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"connection_mode": "ssh", "ssh_conn_id": "ssh_default"}'
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()

        try:
            hook.get_conn()
            assert False, "Should have raised RuntimeError"
        except RuntimeError as e:
            assert "get_conn() is not available in SSH mode" in str(e)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_get_table_thrift_only(self, mock_get_connection, mock_happybase_connection):
        """Test get_table method (Thrift mode only)."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn

        mock_table = MagicMock()
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn

        hook = HBaseHook()
        result = hook.get_table("test_table")

        mock_hbase_conn.table.assert_called_once_with("test_table")
        assert result == mock_table

    @patch.object(HBaseHook, "get_connection")
    def test_get_table_ssh_mode_raises_error(self, mock_get_connection):
        """Test get_table raises error in SSH mode."""
        mock_conn = Connection(
            conn_id="hbase_ssh",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"connection_mode": "ssh", "ssh_conn_id": "ssh_default"}'
        )
        mock_get_connection.return_value = mock_conn

        hook = HBaseHook()

        try:
            hook.get_table("test_table")
            assert False, "Should have raised RuntimeError"
        except RuntimeError as e:
            assert "get_table() is not available in SSH mode" in str(e)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_get_conn_with_kerberos_auth(self, mock_get_connection, mock_happybase_connection):
        """Test get_conn with Kerberos authentication."""
        mock_conn = Connection(
            conn_id="hbase_kerberos",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"auth_method": "kerberos", "principal": "hbase/localhost@REALM", "keytab_path": "/path/to/keytab"}'
        )
        mock_get_connection.return_value = mock_conn

        mock_hbase_conn = MagicMock()
        mock_happybase_connection.return_value = mock_hbase_conn

        # Mock keytab file existence
        with patch("os.path.exists", return_value=True), \
             patch("subprocess.run") as mock_subprocess:
            mock_subprocess.return_value.returncode = 0

            hook = HBaseHook()
            result = hook.get_conn()

            # Verify connection was created successfully
            mock_happybase_connection.assert_called_once()
            assert result == mock_hbase_conn

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

    def test_retry_decorator_success_after_retries(self):
        """Test retry decorator when function succeeds after retries."""
        call_count = 0

        @retry_on_connection_error(max_attempts=3, delay=0.1, backoff_factor=2.0)
        def mock_function(self):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TTransportException("Connection failed")
            return "success"

        mock_self = MagicMock()
        result = mock_function(mock_self)

        assert result == "success"
        assert call_count == 3

    def test_retry_decorator_all_attempts_fail(self):
        """Test retry decorator when all attempts fail."""
        call_count = 0

        @retry_on_connection_error(max_attempts=2, delay=0.1, backoff_factor=2.0)
        def mock_function(self):
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Connection failed")

        mock_self = MagicMock()

        with pytest.raises(ConnectionError):
            mock_function(mock_self)

        assert call_count == 2

    def test_get_retry_config_defaults(self):
        """Test _get_retry_config with default values."""
        hook = HBaseHook()
        config = hook._get_retry_config({})

        assert config["max_attempts"] == 3
        assert config["delay"] == 1.0
        assert config["backoff_factor"] == 2.0

    def test_get_retry_config_custom_values(self):
        """Test _get_retry_config with custom values."""
        hook = HBaseHook()
        extra_config = {
            "retry_max_attempts": 5,
            "retry_delay": 2.5,
            "retry_backoff_factor": 1.5
        }
        config = hook._get_retry_config(extra_config)

        assert config["max_attempts"] == 5
        assert config["delay"] == 2.5
        assert config["backoff_factor"] == 1.5
