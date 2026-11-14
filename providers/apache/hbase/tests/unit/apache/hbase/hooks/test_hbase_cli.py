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

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.apache.hbase.hooks.hbase_cli import HBaseCLIHook


class TestHBaseCLIHook:
    """Test HBase Administration Hook."""

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_create_backup_set(self, mock_run, mock_get_conn):
        """Test create backup set."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Backup set created successfully")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.create_backup_set("test_set", ["table1", "table2"])

        assert "successfully" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-5:] == ["backup", "set", "add", "test_set", "table1,table2"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_list_backup_sets(self, mock_run, mock_get_conn):
        """Test list backup sets."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="test_set1\ntest_set2")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.list_backup_sets()

        assert "test_set1" in result
        assert "test_set2" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-3:] == ["backup", "set", "list"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_create_full_backup_with_set(self, mock_popen, mock_get_conn):
        """Test create full backup with backup set."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["backup_1234567890\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.create_full_backup("/backup", backup_set_name="test_set")

        assert "backup_1234567890" in result
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert call_args[-6:] == ["backup", "create", "full", "/backup", "-s", "test_set"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_create_full_backup_with_tables(self, mock_popen, mock_get_conn):
        """Test create full backup with tables."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["backup_1234567890\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.create_full_backup("/backup", tables=["table1", "table2"])

        assert "backup_1234567890" in result
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert call_args[-6:] == ["backup", "create", "full", "/backup", "-t", "table1,table2"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_create_full_backup_with_workers(self, mock_popen, mock_get_conn):
        """Test create full backup with workers."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["backup_1234567890\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.create_full_backup("/backup", tables=["table1"], workers=4)

        assert "backup_1234567890" in result
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert call_args[-8:] == ["backup", "create", "full", "/backup", "-t", "table1", "-w", "4"]

    def test_create_full_backup_no_tables_or_set(self):
        """Test create full backup without tables or set raises error."""
        hook = HBaseCLIHook(hbase_conn_id="hbase_default")

        with pytest.raises(ValueError, match="Either backup_set_name or tables must be provided"):
            hook.create_full_backup("/backup")

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_create_incremental_backup(self, mock_popen, mock_get_conn):
        """Test create incremental backup."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["backup_1234567891\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.create_incremental_backup("/backup", backup_set_name="test_set")

        assert "backup_1234567891" in result
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert call_args[-6:] == ["backup", "create", "incremental", "/backup", "-s", "test_set"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_get_backup_history(self, mock_run, mock_get_conn):
        """Test get backup history."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890\nbackup_1234567891")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.get_backup_history()

        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-2:] == ["backup", "history"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_get_backup_history_with_set(self, mock_run, mock_get_conn):
        """Test get backup history with backup set."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.get_backup_history(backup_set_name="test_set")

        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-4:] == ["backup", "history", "-s", "test_set"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_get_backup_history_with_path(self, mock_run, mock_get_conn):
        """Test get backup history with backup path."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.get_backup_history(backup_path="/tmp/backup")

        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-4:] == ["backup", "history", "-p", "/tmp/backup"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_get_backup_history_with_set_and_path(self, mock_run, mock_get_conn):
        """Test get backup history with both backup set and path."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="backup_1234567890")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.get_backup_history(backup_set_name="test_set", backup_path="/tmp/backup")

        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-6:] == ["backup", "history", "-s", "test_set", "-p", "/tmp/backup"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_describe_backup(self, mock_run, mock_get_conn):
        """Test describe backup."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Backup ID: backup_1234567890\nTables: table1")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.describe_backup("backup_1234567890")

        assert "backup_1234567890" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-3:] == ["backup", "describe", "backup_1234567890"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_restore_backup(self, mock_popen, mock_get_conn):
        """Test restore backup."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["Restore completed successfully\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.restore_backup("/backup", "backup_1234567890")

        assert "successfully" in result
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert call_args[-3:] == ["restore", "/backup", "backup_1234567890"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_restore_backup_with_tables(self, mock_popen, mock_get_conn):
        """Test restore backup with specific tables."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["Restore completed successfully\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.restore_backup("/backup", "backup_1234567890", tables=["table1"])

        assert "successfully" in result
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert call_args[-5:] == ["restore", "/backup", "backup_1234567890", "-t", "table1"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_restore_backup_with_overwrite(self, mock_popen, mock_get_conn):
        """Test restore backup with overwrite."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["Restore completed successfully\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.restore_backup("/backup", "backup_1234567890", overwrite=True)

        assert "successfully" in result
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert call_args[-4:] == ["restore", "/backup", "backup_1234567890", "-o"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_restore_backup_with_backup_set(self, mock_popen, mock_get_conn):
        """Test restore backup with backup_set_name passes -s flag."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["Restore completed successfully\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.restore_backup("/backup", "backup_123", backup_set_name="test_set")

        assert "successfully" in result
        call_args = mock_popen.call_args[0][0]
        assert call_args[-5:] == ["restore", "/backup", "backup_123", "-s", "test_set"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.Popen")
    def test_restore_backup_set_takes_priority_over_tables(self, mock_popen, mock_get_conn):
        """Test that backup_set_name takes priority over tables when both provided."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ["Restore completed\n", ""]
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        hook.restore_backup("/backup", "backup_123", backup_set_name="my_set", tables=["table1"])

        call_args = mock_popen.call_args[0][0]
        assert "-s" in call_args
        assert "my_set" in call_args
        assert "-t" not in call_args

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_command_failure(self, mock_run, mock_get_conn):
        """Test command failure raises RuntimeError."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.side_effect = subprocess.CalledProcessError(1, "hbase", stderr="Command failed")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")

        with pytest.raises(RuntimeError, match="HBase command failed"):
            hook.list_backup_sets()

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_custom_hbase_cmd(self, mock_run, mock_get_conn):
        """Test custom hbase command path."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="test_set1")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default", hbase_cmd="/opt/hbase/bin/hbase")
        hook.list_backup_sets()

        call_args = mock_run.call_args[0][0]
        # Check that the command starts with the custom hbase path
        assert call_args[0].endswith("/opt/hbase/bin/hbase")

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_execute_command(self, mock_run, mock_get_conn):
        """Test execute arbitrary command."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Command executed successfully")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        result = hook.execute_command("backup set list")

        assert "successfully" in result
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[-3:] == ["backup", "set", "list"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_execute_command_with_custom_hbase_cmd(self, mock_run, mock_get_conn):
        """Test execute arbitrary command with custom hbase path."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="Output")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default", hbase_cmd="/custom/hbase")
        hook.execute_command("version")

        call_args = mock_run.call_args[0][0]
        # Check that the command starts with the custom hbase path and ends with version
        assert call_args[0].endswith("/custom/hbase")
        assert call_args[-1] == "version"

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_execute_command_shlex_quoted_args(self, mock_run, mock_get_conn):
        """Test that shlex correctly parses quoted arguments with spaces."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="OK")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        hook.execute_command('backup create full "/backup dir/my backup" -t table1')

        call_args = mock_run.call_args[0][0]
        assert call_args[-6:] == ["backup", "create", "full", "/backup dir/my backup", "-t", "table1"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_execute_command_list_input(self, mock_run, mock_get_conn):
        """Test that list input is passed directly without shlex parsing."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="OK")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        hook.execute_command(["backup", "set", "list"])

        call_args = mock_run.call_args[0][0]
        assert call_args[-3:] == ["backup", "set", "list"]

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_java_home_from_extra_overrides_constructor(self, mock_run, mock_get_conn):
        """Test that java_home from connection Extra overrides constructor/env value."""
        mock_get_conn.return_value = MagicMock(
            extra_dejson={"java_home": "/usr/lib/jvm/java-17", "hbase_home": "/opt/hbase"}
        )
        mock_run.return_value = MagicMock(returncode=0, stdout="OK")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default", java_home="/usr/lib/jvm/java-8")
        hook.list_backup_sets()

        env = mock_run.call_args[1]["env"]
        assert env["JAVA_HOME"] == "/usr/lib/jvm/java-17"

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_java_home_fallback_to_constructor_when_not_in_extra(self, mock_run, mock_get_conn):
        """Test that java_home falls back to constructor value when not in Extra."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="OK")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default", java_home="/usr/lib/jvm/java-8")
        hook.list_backup_sets()

        env = mock_run.call_args[1]["env"]
        assert env["JAVA_HOME"] == "/usr/lib/jvm/java-8"

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_hbase_home_from_extra_overrides_constructor(self, mock_run, mock_get_conn):
        """Test that hbase_home from connection Extra overrides constructor value."""
        mock_get_conn.return_value = MagicMock(
            extra_dejson={"hbase_home": "/custom/hbase"}
        )
        mock_run.return_value = MagicMock(returncode=0, stdout="OK")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default", hbase_home="/fallback/hbase")
        hook.list_backup_sets()

        env = mock_run.call_args[1]["env"]
        call_args = mock_run.call_args[0][0]
        assert env["HBASE_HOME"] == "/custom/hbase"
        assert call_args[0] == "/custom/hbase/bin/hbase"

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.subprocess.run")
    def test_hbase_home_fallback_to_constructor_when_not_in_extra(self, mock_run, mock_get_conn):
        """Test that hbase_home falls back to constructor value when not in Extra."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        mock_run.return_value = MagicMock(returncode=0, stdout="OK")

        hook = HBaseCLIHook(hbase_conn_id="hbase_default", hbase_home="/fallback/hbase")
        hook.list_backup_sets()

        env = mock_run.call_args[1]["env"]
        call_args = mock_run.call_args[0][0]
        assert env["HBASE_HOME"] == "/fallback/hbase"
        assert call_args[0] == "/fallback/hbase/bin/hbase"

    @patch("airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook.get_connection")
    @pytest.mark.parametrize("command", ["", "   ", []])
    def test_execute_command_empty_input(self, mock_get_conn, command):
        """Test that empty or whitespace-only commands raise ValueError."""
        mock_get_conn.return_value = MagicMock(extra_dejson={})
        hook = HBaseCLIHook(hbase_conn_id="hbase_default")
        with pytest.raises(ValueError, match="Command must not be empty"):
            hook.execute_command(command)
