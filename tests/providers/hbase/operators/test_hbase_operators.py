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

from airflow.providers.hbase.operators.hbase import (
    HBaseBatchGetOperator,
    HBaseBatchPutOperator,
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBasePutOperator,
    HBaseScanOperator,
)


class TestHBasePutOperator:
    """Test HBasePutOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        operator = HBasePutOperator(
            task_id="test_put",
            table_name="test_table",
            row_key="row1",
            data={"cf1:col1": "value1"}
        )
        
        operator.execute({})
        
        mock_hook.put_row.assert_called_once_with("test_table", "row1", {"cf1:col1": "value1"})


class TestHBaseCreateTableOperator:
    """Test HBaseCreateTableOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute_create_new_table(self, mock_hook_class):
        """Test execute method for creating new table."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = False
        mock_hook_class.return_value = mock_hook

        operator = HBaseCreateTableOperator(
            task_id="test_create",
            table_name="test_table",
            families={"cf1": {}, "cf2": {}}
        )
        
        operator.execute({})
        
        mock_hook.table_exists.assert_called_once_with("test_table")
        mock_hook.create_table.assert_called_once_with("test_table", {"cf1": {}, "cf2": {}})

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute_table_exists(self, mock_hook_class):
        """Test execute method when table already exists."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = True
        mock_hook_class.return_value = mock_hook

        operator = HBaseCreateTableOperator(
            task_id="test_create",
            table_name="test_table",
            families={"cf1": {}, "cf2": {}}
        )
        
        operator.execute({})
        
        mock_hook.table_exists.assert_called_once_with("test_table")
        mock_hook.create_table.assert_not_called()


class TestHBaseDeleteTableOperator:
    """Test HBaseDeleteTableOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute_delete_existing_table(self, mock_hook_class):
        """Test execute method for deleting existing table."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = True
        mock_hook_class.return_value = mock_hook

        operator = HBaseDeleteTableOperator(
            task_id="test_delete",
            table_name="test_table"
        )
        
        operator.execute({})
        
        mock_hook.table_exists.assert_called_once_with("test_table")
        mock_hook.delete_table.assert_called_once_with("test_table", True)

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute_table_not_exists(self, mock_hook_class):
        """Test execute method when table doesn't exist."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = False
        mock_hook_class.return_value = mock_hook

        operator = HBaseDeleteTableOperator(
            task_id="test_delete",
            table_name="test_table"
        )
        
        operator.execute({})
        
        mock_hook.table_exists.assert_called_once_with("test_table")
        mock_hook.delete_table.assert_not_called()


class TestHBaseScanOperator:
    """Test HBaseScanOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook.scan_table.return_value = [
            ("row1", {"cf1:col1": "value1"}),
            ("row2", {"cf1:col1": "value2"})
        ]
        mock_hook_class.return_value = mock_hook

        operator = HBaseScanOperator(
            task_id="test_scan",
            table_name="test_table",
            limit=10
        )
        
        result = operator.execute({})
        
        assert len(result) == 2
        mock_hook.scan_table.assert_called_once_with(
            table_name="test_table",
            row_start=None,
            row_stop=None,
            columns=None,
            limit=10
        )


class TestHBaseBatchPutOperator:
    """Test HBaseBatchPutOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        rows = [
            {"row_key": "row1", "cf1:col1": "value1"},
            {"row_key": "row2", "cf1:col1": "value2"}
        ]

        operator = HBaseBatchPutOperator(
            task_id="test_batch_put",
            table_name="test_table",
            rows=rows
        )
        
        operator.execute({})
        
        mock_hook.batch_put_rows.assert_called_once_with("test_table", rows)


class TestHBaseBatchGetOperator:
    """Test HBaseBatchGetOperator."""

    @patch("airflow.providers.hbase.operators.hbase.HBaseHook")
    def test_execute(self, mock_hook_class):
        """Test execute method."""
        mock_hook = MagicMock()
        mock_hook.batch_get_rows.return_value = [
            {"cf1:col1": "value1"},
            {"cf1:col1": "value2"}
        ]
        mock_hook_class.return_value = mock_hook

        operator = HBaseBatchGetOperator(
            task_id="test_batch_get",
            table_name="test_table",
            row_keys=["row1", "row2"],
            columns=["cf1:col1"]
        )
        
        result = operator.execute({})
        
        assert len(result) == 2
        mock_hook.batch_get_rows.assert_called_once_with(
            "test_table", 
            ["row1", "row2"], 
            ["cf1:col1"]
        )