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

import pytest
from unittest.mock import Mock, patch

from airflow.providers.mariadb.hooks.mariadb import MariaDBHook


class TestMariaDBHook:
    """Test cases for MariaDBHook."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.connection_id = "mariadb_default"
        self.hook = MariaDBHook(connection_id=self.connection_id)

    @patch("airflow.providers.mariadb.hooks.mariadb.mariadb.connect")
    def test_get_conn(self, mock_connect):
        """Test getting MariaDB connection."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        conn = self.hook.get_conn()

        assert conn == mock_conn
        mock_connect.assert_called_once()

    @patch("airflow.providers.mariadb.hooks.mariadb.mariadb.connect")
    def test_get_conn_with_extra_params(self, mock_connect):
        """Test getting MariaDB connection with extra parameters."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        # Test with extra parameters
        extra_params = {"charset": "utf8mb4", "autocommit": True}
        hook = MariaDBHook(connection_id=self.connection_id, **extra_params)
        conn = hook.get_conn()

        assert conn == mock_conn
        mock_connect.assert_called_once()

    def test_get_uri(self):
        """Test getting MariaDB URI."""
        with patch.object(self.hook, "get_connection") as mock_get_conn:
            mock_conn = Mock()
            mock_conn.host = "localhost"
            mock_conn.port = 3306
            mock_conn.schema = "test_db"
            mock_conn.login = "test_user"
            mock_conn.password = "test_pass"
            mock_get_conn.return_value = mock_conn

            uri = self.hook.get_uri()

            expected_uri = "mariadb://test_user:test_pass@localhost:3306/test_db"
            assert uri == expected_uri

    @patch("airflow.providers.mariadb.hooks.mariadb.mariadb.connect")
    def test_run_sql(self, mock_connect):
        """Test running SQL queries."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("test",)]
        mock_connect.return_value = mock_conn

        result = self.hook.run("SELECT 'test'")

        assert result == [("test",)]
        mock_cursor.execute.assert_called_once_with("SELECT 'test'")
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
