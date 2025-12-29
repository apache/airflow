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

from airflow.models import Connection
from airflow.providers.hbase.hooks.hbase import HBaseHook, ConnectionMode


class TestHBaseHookStrategy:
    """Test HBase hook with Strategy Pattern."""

    @patch.object(HBaseHook, "get_connection")
    def test_connection_mode_thrift(self, mock_get_connection):
        """Test Thrift connection mode detection."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        hook = HBaseHook()
        assert hook._get_connection_mode() == ConnectionMode.THRIFT

    @patch.object(HBaseHook, "get_connection")
    def test_connection_mode_ssh(self, mock_get_connection):
        """Test SSH connection mode detection."""
        mock_conn = Connection(
            conn_id="hbase_ssh",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"connection_mode": "ssh", "ssh_conn_id": "ssh_default"}'
        )
        mock_get_connection.return_value = mock_conn
        
        hook = HBaseHook()
        assert hook._get_connection_mode() == ConnectionMode.SSH

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_table_exists(self, mock_get_connection, mock_happybase_connection):
        """Test table_exists with Thrift strategy."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.tables.return_value = [b"test_table", b"other_table"]
        mock_happybase_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook()
        assert hook.table_exists("test_table") is True
        assert hook.table_exists("non_existing_table") is False

    @patch.object(HBaseHook, "get_connection")
    def test_ssh_strategy_table_exists(self, mock_get_connection):
        """Test table_exists with SSH strategy."""
        # Mock HBase connection
        mock_hbase_conn = Connection(
            conn_id="hbase_ssh",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"connection_mode": "ssh", "ssh_conn_id": "ssh_default"}'
        )
        
        mock_get_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook("hbase_ssh")
        
        # Mock the SSH strategy's _execute_hbase_command method directly
        with patch.object(hook._get_strategy(), '_execute_hbase_command', return_value="test_table\nother_table\n"):
            assert hook.table_exists("test_table") is True
            assert hook.table_exists("non_existing_table") is False

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_create_table(self, mock_get_connection, mock_happybase_connection):
        """Test create_table with Thrift strategy."""
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
        families = {"cf1": {}, "cf2": {}}
        hook.create_table("test_table", families)
        
        mock_hbase_conn.create_table.assert_called_once_with("test_table", families)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_put_row(self, mock_get_connection, mock_happybase_connection):
        """Test put_row with Thrift strategy."""
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
        data = {"cf1:col1": "value1", "cf1:col2": "value2"}
        hook.put_row("test_table", "row1", data)
        
        mock_table.put.assert_called_once_with("row1", data)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_get_row(self, mock_get_connection, mock_happybase_connection):
        """Test get_row with Thrift strategy."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        mock_table = MagicMock()
        mock_table.row.return_value = {"cf1:col1": "value1"}
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook()
        result = hook.get_row("test_table", "row1")
        
        assert result == {"cf1:col1": "value1"}
        mock_table.row.assert_called_once_with("row1", columns=None)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_delete_row(self, mock_get_connection, mock_happybase_connection):
        """Test delete_row with Thrift strategy."""
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
        hook.delete_row("test_table", "row1")
        
        mock_table.delete.assert_called_once_with("row1", columns=None)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_get_table_families(self, mock_get_connection, mock_happybase_connection):
        """Test get_table_families with Thrift strategy."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        mock_table = MagicMock()
        mock_table.families.return_value = {"cf1": {}, "cf2": {}}
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook()
        result = hook.get_table_families("test_table")
        
        assert result == {"cf1": {}, "cf2": {}}
        mock_table.families.assert_called_once()

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_batch_get_rows(self, mock_get_connection, mock_happybase_connection):
        """Test batch_get_rows with Thrift strategy."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        mock_table = MagicMock()
        mock_table.rows.return_value = [
            (b"row1", {b"cf1:col1": b"value1"}),
            (b"row2", {b"cf1:col1": b"value2"})
        ]
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook()
        result = hook.batch_get_rows("test_table", ["row1", "row2"])
        
        assert len(result) == 2
        mock_table.rows.assert_called_once_with(["row1", "row2"], columns=None)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_batch_put_rows(self, mock_get_connection, mock_happybase_connection):
        """Test batch_put_rows with Thrift strategy."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        mock_table = MagicMock()
        mock_batch = MagicMock()
        mock_table.batch.return_value.__enter__.return_value = mock_batch
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook()
        rows = [
            {"row_key": "row1", "cf1:col1": "value1"},
            {"row_key": "row2", "cf1:col1": "value2"}
        ]
        hook.batch_put_rows("test_table", rows, batch_size=500, max_workers=2)
        
        # Verify batch was called with batch_size
        mock_table.batch.assert_called_with(batch_size=500)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_scan_table(self, mock_get_connection, mock_happybase_connection):
        """Test scan_table with Thrift strategy."""
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        mock_table = MagicMock()
        mock_table.scan.return_value = [
            (b"row1", {b"cf1:col1": b"value1"}),
            (b"row2", {b"cf1:col1": b"value2"})
        ]
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook()
        result = hook.scan_table("test_table", limit=10)
        
        assert len(result) == 2
        mock_table.scan.assert_called_once_with(
            row_start=None, row_stop=None, columns=None, limit=10
        )

    @patch.object(HBaseHook, "get_connection")
    def test_ssh_strategy_put_row(self, mock_get_connection):
        """Test put_row with SSH strategy."""
        # Mock HBase connection
        mock_hbase_conn = Connection(
            conn_id="hbase_ssh",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"connection_mode": "ssh", "ssh_conn_id": "ssh_default"}'
        )
        
        mock_get_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook("hbase_ssh")
        
        # Mock the SSH strategy's _execute_hbase_command method directly
        with patch.object(hook._get_strategy(), '_execute_hbase_command', return_value="") as mock_execute:
            data = {"cf1:col1": "value1", "cf1:col2": "value2"}
            hook.put_row("test_table", "row1", data)
            
            # Verify command was executed
            mock_execute.assert_called_once()

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_thrift_strategy_backup_raises_error(self, mock_get_connection, mock_happybase_connection):
        """Test backup operations raise NotImplementedError in Thrift mode."""
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
        
        # Test all backup operations raise NotImplementedError
        with pytest.raises(NotImplementedError, match="Backup operations require SSH connection mode"):
            hook.create_backup_set("test_set", ["table1"])
        
        with pytest.raises(NotImplementedError, match="Backup operations require SSH connection mode"):
            hook.list_backup_sets()
        
        with pytest.raises(NotImplementedError, match="Backup operations require SSH connection mode"):
            hook.create_full_backup("/backup/path", backup_set_name="test_set")
        
        with pytest.raises(NotImplementedError, match="Backup operations require SSH connection mode"):
            hook.create_incremental_backup("/backup/path", backup_set_name="test_set")
        
        with pytest.raises(NotImplementedError, match="Backup operations require SSH connection mode"):
            hook.get_backup_history("test_set")
        
        with pytest.raises(NotImplementedError, match="Backup operations require SSH connection mode"):
            hook.describe_backup("backup_123")
        
        with pytest.raises(NotImplementedError, match="Backup operations require SSH connection mode"):
            hook.restore_backup("/backup/path", "backup_123")

    @patch.object(HBaseHook, "get_connection")
    def test_ssh_strategy_backup_operations(self, mock_get_connection):
        """Test backup operations with SSH strategy."""
        mock_hbase_conn = Connection(
            conn_id="hbase_ssh",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"connection_mode": "ssh", "ssh_conn_id": "ssh_default"}'
        )
        
        mock_get_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook("hbase_ssh")
        
        # Mock the SSH strategy's _execute_hbase_command method
        with patch.object(hook._get_strategy(), '_execute_hbase_command') as mock_execute:
            # Test create_backup_set
            mock_execute.return_value = "Backup set created"
            result = hook.create_backup_set("test_set", ["table1", "table2"])
            assert result == "Backup set created"
            mock_execute.assert_called_with("backup set add test_set table1,table2")
            
            # Test list_backup_sets
            mock_execute.return_value = "test_set\nother_set"
            result = hook.list_backup_sets()
            assert result == "test_set\nother_set"
            mock_execute.assert_called_with("backup set list")
            
            # Test create_full_backup
            mock_execute.return_value = "backup_123"
            result = hook.create_full_backup("/backup/path", backup_set_name="test_set", workers=5)
            assert result == "backup_123"
            mock_execute.assert_called_with("backup create full /backup/path -s test_set -w 5")
            
            # Test create_incremental_backup
            result = hook.create_incremental_backup("/backup/path", tables=["table1"], workers=3)
            mock_execute.assert_called_with("backup create incremental /backup/path -t table1 -w 3")
            
            # Test get_backup_history
            mock_execute.return_value = "backup history"
            result = hook.get_backup_history(backup_set_name="test_set")
            assert result == "backup history"
            mock_execute.assert_called_with("backup history -s test_set")
            
            # Test describe_backup
            mock_execute.return_value = "backup details"
            result = hook.describe_backup("backup_123")
            assert result == "backup details"
            mock_execute.assert_called_with("backup describe backup_123")
            
            # Test restore_backup
            mock_execute.return_value = "restore completed"
            result = hook.restore_backup("/backup/path", "backup_123", tables=["table1"], overwrite=True)
            assert result == "restore completed"
            mock_execute.assert_called_with("restore /backup/path backup_123 -t table1 -o")

    def test_strategy_pattern_coverage(self):
        """Test that all strategy methods are covered."""
        from airflow.providers.hbase.hooks.hbase_strategy import HBaseStrategy
        
        # Get all abstract methods from HBaseStrategy
        abstract_methods = {
            name for name, method in HBaseStrategy.__dict__.items()
            if getattr(method, '__isabstractmethod__', False)
        }
        
        expected_methods = {
            'table_exists', 'create_table', 'delete_table', 'put_row',
            'get_row', 'delete_row', 'get_table_families', 'batch_get_rows',
            'batch_put_rows', 'scan_table', 'create_backup_set', 'list_backup_sets',
            'create_full_backup', 'create_incremental_backup', 'get_backup_history',
            'describe_backup', 'restore_backup'
        }
        
        assert abstract_methods == expected_methods