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

from airflow.providers.hbase.sensors.hbase import (
    HBaseColumnValueSensor,
    HBaseRowCountSensor,
    HBaseRowSensor,
    HBaseTableSensor,
)


class TestHBaseTableSensor:
    """Test HBaseTableSensor."""

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_table_exists(self, mock_hook_class):
        """Test poke method when table exists."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = True
        mock_hook_class.return_value = mock_hook

        sensor = HBaseTableSensor(
            task_id="test_table_sensor",
            table_name="test_table"
        )
        
        result = sensor.poke({})
        
        assert result is True
        mock_hook.table_exists.assert_called_once_with("test_table")

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_table_not_exists(self, mock_hook_class):
        """Test poke method when table doesn't exist."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = False
        mock_hook_class.return_value = mock_hook

        sensor = HBaseTableSensor(
            task_id="test_table_sensor",
            table_name="test_table"
        )
        
        result = sensor.poke({})
        
        assert result is False


class TestHBaseRowSensor:
    """Test HBaseRowSensor."""

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_row_exists(self, mock_hook_class):
        """Test poke method when row exists."""
        mock_hook = MagicMock()
        mock_hook.get_row.return_value = {"cf1:col1": "value1"}
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowSensor(
            task_id="test_row_sensor",
            table_name="test_table",
            row_key="row1"
        )
        
        result = sensor.poke({})
        
        assert result is True
        mock_hook.get_row.assert_called_once_with("test_table", "row1")

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_row_not_exists(self, mock_hook_class):
        """Test poke method when row doesn't exist."""
        mock_hook = MagicMock()
        mock_hook.get_row.return_value = {}
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowSensor(
            task_id="test_row_sensor",
            table_name="test_table",
            row_key="row1"
        )
        
        result = sensor.poke({})
        
        assert result is False

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_exception(self, mock_hook_class):
        """Test poke method when exception occurs."""
        mock_hook = MagicMock()
        mock_hook.get_row.side_effect = Exception("Connection error")
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowSensor(
            task_id="test_row_sensor",
            table_name="test_table",
            row_key="row1"
        )
        
        result = sensor.poke({})
        
        assert result is False


class TestHBaseRowCountSensor:
    """Test HBaseRowCountSensor."""

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_sufficient_rows(self, mock_hook_class):
        """Test poke method with sufficient rows."""
        mock_hook = MagicMock()
        mock_hook.scan_table.return_value = [
            ("row1", {}), ("row2", {}), ("row3", {})
        ]
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowCountSensor(
            task_id="test_row_count",
            table_name="test_table",
            min_row_count=2
        )
        
        result = sensor.poke({})
        
        assert result is True
        mock_hook.scan_table.assert_called_once_with("test_table", limit=3)

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_insufficient_rows(self, mock_hook_class):
        """Test poke method with insufficient rows."""
        mock_hook = MagicMock()
        mock_hook.scan_table.return_value = [("row1", {})]
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowCountSensor(
            task_id="test_row_count",
            table_name="test_table",
            min_row_count=3
        )
        
        result = sensor.poke({})
        
        assert result is False


class TestHBaseColumnValueSensor:
    """Test HBaseColumnValueSensor."""

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_matching_value(self, mock_hook_class):
        """Test poke method with matching value."""
        mock_hook = MagicMock()
        mock_hook.get_row.return_value = {b"cf1:status": b"active"}
        mock_hook_class.return_value = mock_hook

        sensor = HBaseColumnValueSensor(
            task_id="test_column_value",
            table_name="test_table",
            row_key="user1",
            column="cf1:status",
            expected_value="active"
        )
        
        result = sensor.poke({})
        
        assert result is True
        mock_hook.get_row.assert_called_once_with(
            "test_table", 
            "user1", 
            columns=["cf1:status"]
        )

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_non_matching_value(self, mock_hook_class):
        """Test poke method with non-matching value."""
        mock_hook = MagicMock()
        mock_hook.get_row.return_value = {b"cf1:status": b"inactive"}
        mock_hook_class.return_value = mock_hook

        sensor = HBaseColumnValueSensor(
            task_id="test_column_value",
            table_name="test_table",
            row_key="user1",
            column="cf1:status",
            expected_value="active"
        )
        
        result = sensor.poke({})
        
        assert result is False

    @patch("airflow.providers.hbase.sensors.hbase.HBaseHook")
    def test_poke_row_not_found(self, mock_hook_class):
        """Test poke method when row is not found."""
        mock_hook = MagicMock()
        mock_hook.get_row.return_value = {}
        mock_hook_class.return_value = mock_hook

        sensor = HBaseColumnValueSensor(
            task_id="test_column_value",
            table_name="test_table",
            row_key="user1",
            column="cf1:status",
            expected_value="active"
        )
        
        result = sensor.poke({})
        
        assert result is False