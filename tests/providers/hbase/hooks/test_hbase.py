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
from airflow.providers.hbase.hooks.hbase import HBaseHook


class TestHBaseHook:
    """Test HBase hook."""

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_get_conn(self, mock_get_connection, mock_happybase_connection):
        """Test get_conn method."""
        # Mock connection
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        # Mock happybase connection
        mock_hbase_conn = MagicMock()
        mock_happybase_connection.return_value = mock_hbase_conn
        
        # Test
        hook = HBaseHook()
        result = hook.get_conn()
        
        # Assertions
        mock_happybase_connection.assert_called_once_with(host="localhost", port=9090)
        assert result == mock_hbase_conn

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_table_exists(self, mock_get_connection, mock_happybase_connection):
        """Test table_exists method."""
        # Mock connection
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        # Mock happybase connection
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.tables.return_value = [b"test_table", b"other_table"]
        mock_happybase_connection.return_value = mock_hbase_conn
        
        # Test
        hook = HBaseHook()
        
        # Test existing table
        assert hook.table_exists("test_table") is True
        
        # Test non-existing table
        assert hook.table_exists("non_existing_table") is False

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_create_table(self, mock_get_connection, mock_happybase_connection):
        """Test create_table method."""
        # Mock connection
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        # Mock happybase connection
        mock_hbase_conn = MagicMock()
        mock_happybase_connection.return_value = mock_hbase_conn
        
        # Test
        hook = HBaseHook()
        families = {"cf1": {}, "cf2": {}}
        hook.create_table("test_table", families)
        
        # Assertions
        mock_hbase_conn.create_table.assert_called_once_with("test_table", families)

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_put_row(self, mock_get_connection, mock_happybase_connection):
        """Test put_row method."""
        # Mock connection
        mock_conn = Connection(
            conn_id="hbase_default",
            conn_type="hbase",
            host="localhost",
            port=9090,
        )
        mock_get_connection.return_value = mock_conn
        
        # Mock happybase connection and table
        mock_table = MagicMock()
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn
        
        # Test
        hook = HBaseHook()
        data = {"cf1:col1": "value1", "cf1:col2": "value2"}
        hook.put_row("test_table", "row1", data)
        
        # Assertions
        mock_hbase_conn.table.assert_called_once_with("test_table")
        mock_table.put.assert_called_once_with("row1", data)

    def test_get_ui_field_behaviour(self):
        """Test get_ui_field_behaviour method."""
        result = HBaseHook.get_ui_field_behaviour()
        assert "hidden_fields" in result
        assert "relabeling" in result
        assert "placeholders" in result

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_batch_put_rows(self, mock_get_connection, mock_happybase_connection):
        """Test batch_put_rows method."""
        mock_conn = Connection(conn_id="hbase_default", conn_type="hbase", host="localhost", port=9090)
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
        hook.batch_put_rows("test_table", rows)
        
        mock_table.batch.assert_called_once()

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_batch_get_rows(self, mock_get_connection, mock_happybase_connection):
        """Test batch_get_rows method."""
        mock_conn = Connection(conn_id="hbase_default", conn_type="hbase", host="localhost", port=9090)
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
        mock_table.rows.assert_called_once()

    @patch("airflow.providers.hbase.hooks.hbase.happybase.Connection")
    @patch.object(HBaseHook, "get_connection")
    def test_delete_row(self, mock_get_connection, mock_happybase_connection):
        """Test delete_row method."""
        mock_conn = Connection(conn_id="hbase_default", conn_type="hbase", host="localhost", port=9090)
        mock_get_connection.return_value = mock_conn
        
        mock_table = MagicMock()
        mock_hbase_conn = MagicMock()
        mock_hbase_conn.table.return_value = mock_table
        mock_happybase_connection.return_value = mock_hbase_conn
        
        hook = HBaseHook()
        hook.delete_row("test_table", "row1")
        
        mock_table.delete.assert_called_once_with("row1", columns=None)

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_create_backup_set(self, mock_subprocess_run):
        """Test create_backup_set method."""
        mock_result = MagicMock()
        mock_result.stdout = "Backup set created successfully"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.create_backup_set("test_backup_set", ["table1", "table2"])
        
        expected_cmd = ["hbase", "backup", "set", "add", "test_backup_set", "table1", "table2"]
        mock_subprocess_run.assert_called_once_with(expected_cmd, capture_output=True, text=True, check=True)
        assert result == "Backup set created successfully"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_list_backup_sets(self, mock_subprocess_run):
        """Test list_backup_sets method."""
        mock_result = MagicMock()
        mock_result.stdout = "test_backup_set\nother_backup_set"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.list_backup_sets()
        
        expected_cmd = ["hbase", "backup", "set", "list"]
        mock_subprocess_run.assert_called_once_with(expected_cmd, capture_output=True, text=True, check=True)
        assert result == "test_backup_set\nother_backup_set"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_create_full_backup(self, mock_subprocess_run):
        """Test create_full_backup method."""
        mock_result = MagicMock()
        mock_result.stdout = "backup_20240101_123456"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.create_full_backup("hdfs://test/backup", "test_backup_set", 5)
        
        expected_cmd = [
            "hbase", "backup", "create", "full", 
            "hdfs://test/backup", "-s", "test_backup_set", "-w", "5"
        ]
        mock_subprocess_run.assert_called_once_with(expected_cmd, capture_output=True, text=True, check=True)
        assert result == "backup_20240101_123456"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_create_incremental_backup(self, mock_subprocess_run):
        """Test create_incremental_backup method."""
        mock_result = MagicMock()
        mock_result.stdout = "backup_20240101_234567"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.create_incremental_backup("hdfs://test/backup", "test_backup_set", 3)
        
        expected_cmd = [
            "hbase", "backup", "create", "incremental", 
            "hdfs://test/backup", "-s", "test_backup_set", "-w", "3"
        ]
        mock_subprocess_run.assert_called_once_with(expected_cmd, capture_output=True, text=True, check=True)
        assert result == "backup_20240101_234567"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_backup_history(self, mock_subprocess_run):
        """Test backup_history method."""
        mock_result = MagicMock()
        mock_result.stdout = "backup_20240101_123456\nbackup_20240101_234567"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.backup_history("test_backup_set")
        
        expected_cmd = ["hbase", "backup", "history", "-s", "test_backup_set"]
        mock_subprocess_run.assert_called_once_with(expected_cmd, capture_output=True, text=True, check=True)
        assert result == "backup_20240101_123456\nbackup_20240101_234567"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_describe_backup(self, mock_subprocess_run):
        """Test describe_backup method."""
        mock_result = MagicMock()
        mock_result.stdout = "Backup ID: backup_123\nTables: table1, table2"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.describe_backup("backup_123")
        
        expected_cmd = ["hbase", "backup", "describe", "backup_123"]
        mock_subprocess_run.assert_called_once_with(expected_cmd, capture_output=True, text=True, check=True)
        assert result == "Backup ID: backup_123\nTables: table1, table2"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_restore_backup(self, mock_subprocess_run):
        """Test restore_backup method."""
        mock_result = MagicMock()
        mock_result.stdout = "Restore completed successfully"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.restore_backup("hdfs://test/backup", "backup_123", "test_backup_set")
        
        expected_cmd = [
            "hbase", "restore", 
            "hdfs://test/backup", "backup_123", "-s", "test_backup_set"
        ]
        mock_subprocess_run.assert_called_once_with(expected_cmd, capture_output=True, text=True, check=True)
        assert result == "Restore completed successfully"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_execute_hbase_command(self, mock_subprocess_run):
        """Test execute_hbase_command method."""
        mock_result = MagicMock()
        mock_result.stdout = "Command executed successfully"
        mock_subprocess_run.return_value = mock_result
        
        hook = HBaseHook()
        result = hook.execute_hbase_command("backup set list")
        
        mock_subprocess_run.assert_called_once_with(
            "hbase backup set list",
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        assert result == "Command executed successfully"

    @patch("airflow.providers.hbase.hooks.hbase.subprocess.run")
    def test_execute_hbase_command_failure(self, mock_subprocess_run):
        """Test execute_hbase_command method with failure."""
        import subprocess
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="hbase backup set list", stderr="Command failed"
        )
        
        hook = HBaseHook()
        
        with pytest.raises(subprocess.CalledProcessError):
            hook.execute_hbase_command("backup set list")