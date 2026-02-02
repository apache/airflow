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

"""Tests for HBase backup operators."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.hbase.operators.hbase import (
    BackupSetAction,
    BackupType,
    HBaseBackupHistoryOperator,
    HBaseBackupSetOperator,
    HBaseCreateBackupOperator,
    HBaseRestoreOperator,
)


class TestHBaseBackupSetOperator:
    """Test HBaseBackupSetOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_backup_set_add(self, mock_hook_class):
        """Test backup set add operation."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.create_backup_set.return_value = "Backup set created"

        operator = HBaseBackupSetOperator(
            task_id="test_task",
            action=BackupSetAction.ADD,
            backup_set_name="test_set",
            tables=["table1", "table2"],
        )

        result = operator.execute({})

        mock_hook.create_backup_set.assert_called_once_with("test_set", ["table1", "table2"])
        assert result == "Backup set created"

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_backup_set_list(self, mock_hook_class):
        """Test backup set list operation."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.list_backup_sets.return_value = "test_set\nother_set"

        operator = HBaseBackupSetOperator(
            task_id="test_task",
            action=BackupSetAction.LIST,
        )

        result = operator.execute({})

        mock_hook.list_backup_sets.assert_called_once()
        assert result == "test_set\nother_set"

    def test_backup_set_invalid_action(self):
        """Test backup set with invalid action."""
        operator = HBaseBackupSetOperator(
            task_id="test_task",
            action="invalid",
        )

        with pytest.raises(ValueError, match="Unsupported action: invalid"):
            operator.execute({})


class TestHBaseCreateBackupOperator:
    """Test HBaseCreateBackupOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_create_full_backup_with_set(self, mock_hook_class):
        """Test creating full backup with backup set."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.create_full_backup.return_value = "Backup created: backup_123"

        operator = HBaseCreateBackupOperator(
            task_id="test_task",
            backup_type=BackupType.FULL,
            backup_path="/tmp/backup",
            backup_set_name="test_set",
            workers=2,
        )

        result = operator.execute({})

        mock_hook.create_full_backup.assert_called_once_with(
            backup_root="/tmp/backup",
            backup_set_name="test_set",
            tables=None,
            workers=2
        )
        assert result == "Backup created: backup_123"

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_create_incremental_backup_with_tables(self, mock_hook_class):
        """Test creating incremental backup with table list."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.create_incremental_backup.return_value = "Incremental backup created"

        operator = HBaseCreateBackupOperator(
            task_id="test_task",
            backup_type=BackupType.INCREMENTAL,
            backup_path="/tmp/backup",
            tables=["table1", "table2"],
        )

        result = operator.execute({})

        mock_hook.create_incremental_backup.assert_called_once_with(
            backup_root="/tmp/backup",
            backup_set_name=None,
            tables=["table1", "table2"],
            workers=3
        )
        assert result == "Incremental backup created"

    def test_create_backup_invalid_type(self):
        """Test creating backup with invalid type."""
        operator = HBaseCreateBackupOperator(
            task_id="test_task",
            backup_type="invalid",
            backup_path="/tmp/backup",
            backup_set_name="test_set",
        )

        with pytest.raises(ValueError, match="backup_type must be 'full' or 'incremental'"):
            operator.execute({})

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_create_backup_no_tables_or_set(self, mock_hook_class):
        """Test creating backup without tables or backup set."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        
        operator = HBaseCreateBackupOperator(
            task_id="test_task",
            backup_type=BackupType.FULL,
            backup_path="/tmp/backup",
        )

        with pytest.raises(ValueError, match="Either backup_set_name or tables must be specified"):
            operator.execute({})


class TestHBaseRestoreOperator:
    """Test HBaseRestoreOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_restore_with_backup_set(self, mock_hook_class):
        """Test restore with backup set."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.restore_backup.return_value = "Restore completed"

        operator = HBaseRestoreOperator(
            task_id="test_task",
            backup_path="/tmp/backup",
            backup_id="backup_123",
            backup_set_name="test_set",
            overwrite=True,
        )

        result = operator.execute({})

        mock_hook.restore_backup.assert_called_once_with(
            backup_root="/tmp/backup",
            backup_id="backup_123",
            tables=None,
            overwrite=True
        )
        assert result == "Restore completed"

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_restore_with_tables(self, mock_hook_class):
        """Test restore with table list."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.restore_backup.return_value = "Restore completed"

        operator = HBaseRestoreOperator(
            task_id="test_task",
            backup_path="/tmp/backup",
            backup_id="backup_123",
            tables=["table1", "table2"],
        )

        result = operator.execute({})

        mock_hook.restore_backup.assert_called_once_with(
            backup_root="/tmp/backup",
            backup_id="backup_123",
            tables=["table1", "table2"],
            overwrite=False
        )
        assert result == "Restore completed"


class TestHBaseBackupHistoryOperator:
    """Test HBaseBackupHistoryOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_backup_history_with_set(self, mock_hook_class):
        """Test backup history with backup set."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_backup_history.return_value = "backup_123 COMPLETE"

        operator = HBaseBackupHistoryOperator(
            task_id="test_task",
            backup_set_name="test_set",
        )

        result = operator.execute({})

        mock_hook.get_backup_history.assert_called_once_with(backup_set_name="test_set")
        assert result == "backup_123 COMPLETE"

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_backup_history_with_path(self, mock_hook_class):
        """Test backup history with backup path."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_backup_history.return_value = "backup_456 COMPLETE"

        operator = HBaseBackupHistoryOperator(
            task_id="test_task",
            backup_path="/tmp/backup",
        )

        result = operator.execute({})

        mock_hook.get_backup_history.assert_called_once_with(backup_set_name=None)
        assert result == "backup_456 COMPLETE"

    @patch("airflow.providers.hbase.operators.hbase.HBaseAdministrationHook")
    def test_backup_history_no_params(self, mock_hook_class):
        """Test backup history without parameters."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_backup_history.return_value = "All backups"

        operator = HBaseBackupHistoryOperator(
            task_id="test_task",
        )

        result = operator.execute({})

        mock_hook.get_backup_history.assert_called_once_with(backup_set_name=None)
        assert result == "All backups"