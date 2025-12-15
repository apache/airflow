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
        expected = {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {},
        }
        assert result == expected