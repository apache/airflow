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
"""Tests for HBase backup/restore operators."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.hbase.operators.hbase import (
    HBaseCreateBackupSetOperator,
    HBaseFullBackupOperator,
    HBaseIncrementalBackupOperator,
    HBaseRestoreOperator,
)


class TestHBaseCreateBackupSetOperator:
    """Test HBaseCreateBackupSetOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.create_backup_set.return_value = "backup_set_created"

        operator = HBaseCreateBackupSetOperator(
            task_id="test_task",
            backup_set_name="test_backup_set",
            tables=["table1", "table2"],
        )

        result = operator.execute({})

        mock_hook.create_backup_set.assert_called_once_with("test_backup_set", ["table1", "table2"])
        assert result == "backup_set_created"

    def test_template_fields(self):
        """Test template fields."""
        operator = HBaseCreateBackupSetOperator(
            task_id="test_task",
            backup_set_name="test_backup_set",
            tables=["table1"],
        )
        assert operator.template_fields == ("backup_set_name", "tables")


class TestHBaseFullBackupOperator:
    """Test HBaseFullBackupOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.create_full_backup.return_value = "backup_id_123"

        operator = HBaseFullBackupOperator(
            task_id="test_task",
            backup_path="hdfs://test/backup",
            backup_set_name="test_backup_set",
            workers=5,
        )

        result = operator.execute({})

        mock_hook.create_full_backup.assert_called_once_with("hdfs://test/backup", "test_backup_set", 5)
        assert result == "backup_id_123"

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute_default_workers(self, mock_hook_class):
        """Test execute method with default workers."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.create_full_backup.return_value = "backup_id_123"

        operator = HBaseFullBackupOperator(
            task_id="test_task",
            backup_path="hdfs://test/backup",
            backup_set_name="test_backup_set",
        )

        result = operator.execute({})

        mock_hook.create_full_backup.assert_called_once_with("hdfs://test/backup", "test_backup_set", 3)
        assert result == "backup_id_123"

    def test_template_fields(self):
        """Test template fields."""
        operator = HBaseFullBackupOperator(
            task_id="test_task",
            backup_path="hdfs://test/backup",
            backup_set_name="test_backup_set",
        )
        assert operator.template_fields == ("backup_path", "backup_set_name")


class TestHBaseIncrementalBackupOperator:
    """Test HBaseIncrementalBackupOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.create_incremental_backup.return_value = "backup_id_456"

        operator = HBaseIncrementalBackupOperator(
            task_id="test_task",
            backup_path="hdfs://test/backup",
            backup_set_name="test_backup_set",
            workers=2,
        )

        result = operator.execute({})

        mock_hook.create_incremental_backup.assert_called_once_with("hdfs://test/backup", "test_backup_set", 2)
        assert result == "backup_id_456"

    def test_template_fields(self):
        """Test template fields."""
        operator = HBaseIncrementalBackupOperator(
            task_id="test_task",
            backup_path="hdfs://test/backup",
            backup_set_name="test_backup_set",
        )
        assert operator.template_fields == ("backup_path", "backup_set_name")


class TestHBaseRestoreOperator:
    """Test HBaseRestoreOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.restore_backup.return_value = "restore_completed"

        operator = HBaseRestoreOperator(
            task_id="test_task",
            backup_path="hdfs://test/backup",
            backup_id="backup_123",
            backup_set_name="test_backup_set",
        )

        result = operator.execute({})

        mock_hook.restore_backup.assert_called_once_with("hdfs://test/backup", "backup_123", "test_backup_set")
        assert result == "restore_completed"

    def test_template_fields(self):
        """Test template fields."""
        operator = HBaseRestoreOperator(
            task_id="test_task",
            backup_path="hdfs://test/backup",
            backup_id="backup_123",
            backup_set_name="test_backup_set",
        )
        assert operator.template_fields == ("backup_path", "backup_id", "backup_set_name")