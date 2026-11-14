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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.apache.hbase.sensors.hbase import (
    HBaseRowSensor,
    HBaseTableSensor,
)


class TestHBaseTableSensor:
    """Test HBaseTableSensor."""

    @patch("airflow.providers.apache.hbase.sensors.hbase.HBaseThriftHook")
    def test_poke_table_exists(self, mock_hook_class):
        """Test poke method when table exists."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = True
        mock_hook_class.return_value = mock_hook

        sensor = HBaseTableSensor(task_id="test_table_sensor", table_name="test_table")

        result = sensor.poke({})

        assert result is True
        mock_hook.table_exists.assert_called_once_with("test_table")

    @patch("airflow.providers.apache.hbase.sensors.hbase.HBaseThriftHook")
    def test_poke_table_not_exists(self, mock_hook_class):
        """Test poke method when table doesn't exist."""
        mock_hook = MagicMock()
        mock_hook.table_exists.return_value = False
        mock_hook_class.return_value = mock_hook

        sensor = HBaseTableSensor(task_id="test_table_sensor", table_name="test_table")

        result = sensor.poke({})

        assert result is False


class TestHBaseRowSensor:
    """Test HBaseRowSensor."""

    @patch("airflow.providers.apache.hbase.sensors.hbase.HBaseThriftHook")
    def test_poke_row_exists(self, mock_hook_class):
        """Test poke method when row exists."""
        mock_hook = MagicMock()
        mock_hook.get_row.return_value = {"cf1:col1": "value1"}
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowSensor(task_id="test_row_sensor", table_name="test_table", row_key="row1")

        result = sensor.poke({})

        assert result is True
        mock_hook.get_row.assert_called_once_with("test_table", "row1")

    @patch("airflow.providers.apache.hbase.sensors.hbase.HBaseThriftHook")
    def test_poke_row_not_exists(self, mock_hook_class):
        """Test poke method when row doesn't exist."""
        mock_hook = MagicMock()
        mock_hook.get_row.return_value = {}
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowSensor(task_id="test_row_sensor", table_name="test_table", row_key="row1")

        result = sensor.poke({})

        assert result is False

    @patch("airflow.providers.apache.hbase.sensors.hbase.HBaseThriftHook")
    def test_poke_exception(self, mock_hook_class):
        """Test poke method when exception occurs."""
        mock_hook = MagicMock()
        mock_hook.get_row.side_effect = Exception("Connection error")
        mock_hook_class.return_value = mock_hook

        sensor = HBaseRowSensor(task_id="test_row_sensor", table_name="test_table", row_key="row1")

        with pytest.raises(Exception, match="Connection error"):
            sensor.poke({})
