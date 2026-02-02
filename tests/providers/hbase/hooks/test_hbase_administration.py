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

from airflow.providers.hbase.hooks.hbase_administration import HBaseAdministrationHook


class TestHBaseAdministrationHook:
    """Test HBase Administration Hook."""

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_create_backup_set(self, mock_run, mock_get_conn):
        """Test create backup set."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Backup set created successfully")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.create_backup_set("test_set", ["table1", "table2"])
        
        assert "successfully" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup set add test_set table1,table2" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_list_backup_sets(self, mock_run, mock_get_conn):
        """Test list backup sets."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="test_set1\ntest_set2")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.list_backup_sets()
        
        assert "test_set1" in result
        assert "test_set2" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup set list" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_create_full_backup_with_set(self, mock_run, mock_get_conn):
        """Test create full backup with backup set."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.create_full_backup("/backup", backup_set_name="test_set")
        
        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup create full /backup -s test_set" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_create_full_backup_with_tables(self, mock_run, mock_get_conn):
        """Test create full backup with tables."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.create_full_backup("/backup", tables=["table1", "table2"])
        
        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup create full /backup -t table1,table2" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_create_full_backup_with_workers(self, mock_run, mock_get_conn):
        """Test create full backup with workers."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.create_full_backup("/backup", tables=["table1"], workers=4)
        
        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup create full /backup -t table1 -w 4" in call_args

    def test_create_full_backup_no_tables_or_set(self):
        """Test create full backup without tables or set raises error."""
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        
        with pytest.raises(ValueError, match="Either backup_set_name or tables must be provided"):
            hook.create_full_backup("/backup")

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_create_incremental_backup(self, mock_run, mock_get_conn):
        """Test create incremental backup."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567891")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.create_incremental_backup("/backup", backup_set_name="test_set")
        
        assert "backup_1234567891" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup create incremental /backup -s test_set" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_get_backup_history(self, mock_run, mock_get_conn):
        """Test get backup history."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890\nbackup_1234567891")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.get_backup_history()
        
        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup history" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_get_backup_history_with_set(self, mock_run, mock_get_conn):
        """Test get backup history with backup set."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.get_backup_history(backup_set_name="test_set")
        
        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup history -s test_set" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_describe_backup(self, mock_run, mock_get_conn):
        """Test describe backup."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Backup ID: backup_1234567890\nTables: table1")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.describe_backup("backup_1234567890")
        
        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "backup describe backup_1234567890" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_restore_backup(self, mock_run, mock_get_conn):
        """Test restore backup."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Restore completed successfully")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.restore_backup("/backup", "backup_1234567890")
        
        assert "successfully" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "restore /backup backup_1234567890" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_restore_backup_with_tables(self, mock_run, mock_get_conn):
        """Test restore backup with specific tables."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Restore completed successfully")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.restore_backup("/backup", "backup_1234567890", tables=["table1"])
        
        assert "successfully" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "restore /backup backup_1234567890 -t table1" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_restore_backup_with_overwrite(self, mock_run, mock_get_conn):
        """Test restore backup with overwrite."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Restore completed successfully")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        result = hook.restore_backup("/backup", "backup_1234567890", overwrite=True)
        
        assert "successfully" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "restore /backup backup_1234567890 -o" in call_args

    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_command_failure(self, mock_run):
        """Test command failure raises RuntimeError."""
        mock_run.side_effect = Exception("Command failed")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default")
        
        with pytest.raises(Exception):
            hook.list_backup_sets()

    @patch("airflow.providers.hbase.hooks.hbase_administration.HBaseAdministrationHook.get_connection")
    @patch("airflow.providers.hbase.hooks.hbase_administration.subprocess.run")
    def test_custom_hbase_cmd(self, mock_run, mock_get_conn):
        """Test custom hbase command path."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="test_set1")
        
        hook = HBaseAdministrationHook(hbase_conn_id="hbase_default", hbase_cmd="/opt/hbase/bin/hbase")
        hook.list_backup_sets()
        
        call_args = mock_run.call_args[0][0]
        assert "/opt/hbase/bin/hbase" in call_args
