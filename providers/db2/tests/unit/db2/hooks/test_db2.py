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
"""Unit tests for DB2Hook."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.db2.hooks.db2 import Db2Hook


class TestDb2Hook:
    """Test Db2Hook."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock Db2 connection."""
        conn = Connection(
            conn_id="db2_default",
            conn_type="db2",
            host="localhost",
            login="db2user",
            password="db2pass",
            schema="testdb",
            port=50000,
        )
        return conn

    @pytest.fixture
    def mock_connection_with_extras(self):
        """Create a mock Db2 connection with extra parameters."""
        conn = Connection(
            conn_id="db2_default",
            conn_type="db2",
            host="localhost",
            login="db2user",
            password="db2pass",
            schema="testdb",
            port=50000,
            extra='{"ssl": true, "sslcert": "/path/to/cert.pem"}',
        )
        return conn

    @patch("airflow.providers.db2.hooks.db2.Db2Hook.get_connection")
    @patch("ibm_db_dbi.connect")
    def test_get_conn(self, mock_connect, mock_get_connection, mock_connection):
        """Test get_conn method."""
        mock_get_connection.return_value = mock_connection
        mock_db_conn = MagicMock()
        mock_connect.return_value = mock_db_conn

        hook = Db2Hook(db2_conn_id="db2_default")
        conn = hook.get_conn()

        # Verify connection was created
        assert conn == mock_db_conn
        mock_connect.assert_called_once()

        # Verify connection string contains expected parameters
        call_args = mock_connect.call_args[0][0]
        assert "DATABASE=testdb" in call_args
        assert "HOSTNAME=localhost" in call_args
        assert "PORT=50000" in call_args
        assert "PROTOCOL=TCPIP" in call_args
        assert "UID=db2user" in call_args
        assert "PWD=db2pass" in call_args

    @patch("airflow.providers.db2.hooks.db2.Db2Hook.get_connection")
    @patch("ibm_db_dbi.connect")
    def test_get_conn_with_ssl(self, mock_connect, mock_get_connection, mock_connection_with_extras):
        """Test get_conn method with SSL configuration."""
        mock_get_connection.return_value = mock_connection_with_extras
        mock_db_conn = MagicMock()
        mock_connect.return_value = mock_db_conn

        hook = Db2Hook(db2_conn_id="db2_default")
        conn = hook.get_conn()

        # Verify connection was created
        assert conn == mock_db_conn
        mock_connect.assert_called_once()

        # Verify connection string contains SSL parameters
        call_args = mock_connect.call_args[0][0]
        assert "SECURITY=SSL" in call_args
        assert "SSLClientKeystoredb=/path/to/cert.pem" in call_args

    @patch("airflow.providers.db2.hooks.db2.Db2Hook.get_connection")
    def test_get_uri(self, mock_get_connection, mock_connection):
        """Test get_uri method."""
        mock_get_connection.return_value = mock_connection

        hook = Db2Hook(db2_conn_id="db2_default")
        uri = hook.get_uri()

        # Verify URI format
        assert uri == "db2+ibm_db://db2user:db2pass@localhost:50000/testdb"

    @patch("airflow.providers.db2.hooks.db2.Db2Hook.get_connection")
    def test_get_uri_with_special_chars_in_password(self, mock_get_connection):
        """Test get_uri method with special characters in password."""
        conn = Connection(
            conn_id="db2_default",
            conn_type="db2",
            host="localhost",
            login="db2user",
            password="p@ss:word!",
            schema="testdb",
            port=50000,
        )
        mock_get_connection.return_value = conn

        hook = Db2Hook(db2_conn_id="db2_default")
        uri = hook.get_uri()

        # Verify password is URL encoded
        assert "p%40ss%3Aword%21" in uri

    @patch("airflow.providers.db2.hooks.db2.Db2Hook.get_connection")
    def test_get_uri_with_defaults(self, mock_get_connection):
        """Test get_uri method with default values."""
        conn = Connection(
            conn_id="db2_default",
            conn_type="db2",
            host="",
            login="",
            password="",
            schema="",
            port=None,
        )
        mock_get_connection.return_value = conn

        hook = Db2Hook(db2_conn_id="db2_default")
        uri = hook.get_uri()

        # Verify URI uses defaults
        assert uri == "db2+ibm_db://:@localhost:50000/"

    def test_hook_attributes(self):
        """Test hook class attributes."""
        assert Db2Hook.conn_name_attr == "db2_conn_id"
        assert Db2Hook.default_conn_name == "db2_default"
        assert Db2Hook.conn_type == "db2"
        assert Db2Hook.hook_name == "IBM Db2"
        assert Db2Hook.supports_autocommit is True
        assert Db2Hook.supports_executemany is True
