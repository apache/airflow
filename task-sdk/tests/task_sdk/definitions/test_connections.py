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

from unittest import mock
from urllib.parse import urlparse

import pytest

from airflow.configuration import initialize_secrets_backends
from airflow.exceptions import AirflowException
from airflow.sdk import Connection
from airflow.sdk.execution_time.comms import ConnectionResult
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

from tests_common.test_utils.config import conf_vars


class TestConnections:
    @pytest.fixture
    def mock_providers_manager(self):
        """Mock the ProvidersManager to return predefined hooks."""
        with mock.patch("airflow.providers_manager.ProvidersManager") as mock_manager:
            yield mock_manager

    @mock.patch("airflow.utils.module_loading.import_string")
    def test_get_hook(self, mock_import_string, mock_providers_manager):
        """Test that get_hook returns the correct hook instance."""

        mock_hook_class = mock.MagicMock()
        mock_hook_class.return_value = "mock_hook_instance"
        mock_import_string.return_value = mock_hook_class

        mock_hook = mock.MagicMock()
        mock_hook.hook_class_name = "airflow.providers.mysql.hooks.mysql.MySqlHook"
        mock_hook.connection_id_attribute_name = "conn_id"

        mock_providers_manager.return_value.hooks = {"mysql": mock_hook}

        conn = Connection(
            conn_id="test_conn",
            conn_type="mysql",
        )

        hook_instance = conn.get_hook(hook_params={"param1": "value1"})

        mock_import_string.assert_called_once_with("airflow.providers.mysql.hooks.mysql.MySqlHook")

        mock_hook_class.assert_called_once_with(conn_id="test_conn", param1="value1")

        assert hook_instance == "mock_hook_instance"

    def test_get_hook_invalid_type(self, mock_providers_manager):
        """Test that get_hook raises AirflowException for unknown hook type."""
        mock_providers_manager.return_value.hooks = {}

        conn = Connection(
            conn_id="test_conn",
            conn_type="unknown_type",
        )

        with pytest.raises(AirflowException, match='Unknown hook type "unknown_type"'):
            conn.get_hook()

    def test_get_uri(self):
        """Test that get_uri generates the correct URI based on connection attributes."""

        conn = Connection(
            conn_id="test_conn",
            conn_type="mysql",
            host="localhost",
            login="user",
            password="password",
            schema="test_schema",
            port=3306,
            extra=None,
        )

        uri = conn.get_uri()
        parsed_uri = urlparse(uri)

        assert uri == "mysql://user:password@localhost:3306/test_schema"
        assert parsed_uri.scheme == "mysql"
        assert parsed_uri.hostname == "localhost"
        assert parsed_uri.username == "user"
        assert parsed_uri.password == "password"
        assert parsed_uri.port == 3306
        assert parsed_uri.path.lstrip("/") == "test_schema"

    def test_conn_get(self, mock_supervisor_comms):
        conn_result = ConnectionResult(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)
        mock_supervisor_comms.get_message.return_value = conn_result

        conn = Connection.get(conn_id="mysql_conn")
        assert conn is not None
        assert isinstance(conn, Connection)
        assert conn == Connection(
            conn_id="mysql_conn",
            conn_type="mysql",
            description=None,
            host="mysql",
            schema=None,
            login=None,
            password=None,
            port=3306,
            extra=None,
        )


class TestConnectionsFromSecrets:
    def test_get_connection_secrets_backend(self, mock_supervisor_comms, tmp_path):
        """Tests getting a connection from secrets backend."""
        path = tmp_path / "conn.env"
        path.write_text("CONN_A=mysql://host_a")

        with conf_vars(
            {
                (
                    "workers",
                    "secrets_backend",
                ): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
                ("workers", "secrets_backend_kwargs"): f'{{"connections_file_path": "{path}"}}',
            }
        ):
            retrieved_conn = Connection.get(conn_id="CONN_A")
            assert retrieved_conn is not None
            assert retrieved_conn.conn_id == "CONN_A"

    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connection")
    def test_get_connection_env_var(self, mock_env_get, mock_supervisor_comms):
        """Tests getting a connection from environment variable."""
        mock_env_get.return_value = Connection(conn_id="something", conn_type="some-type")  # return None
        Connection.get("something")
        mock_env_get.assert_called_once_with(conn_id="something")

    @conf_vars(
        {
            ("workers", "secrets_backend"): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
            ("workers", "secrets_backend_kwargs"): '{"connections_file_path": "/files/conn.json"}',
        }
    )
    @mock.patch("airflow.secrets.local_filesystem.LocalFilesystemBackend.get_connection")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connection")
    def test_backend_fallback_to_env_var(self, mock_get_connection, mock_env_get, mock_supervisor_comms):
        """Tests if connection retrieval falls back to environment variable backend if not found in secrets backend."""
        mock_get_connection.return_value = None
        mock_env_get.return_value = Connection(conn_id="something", conn_type="some-type")

        backends = initialize_secrets_backends(DEFAULT_SECRETS_SEARCH_PATH_WORKERS)
        assert len(backends) == 2
        backend_classes = [backend.__class__.__name__ for backend in backends]
        assert "LocalFilesystemBackend" in backend_classes

        conn = Connection.get(conn_id="something")
        # mock_env is only called when LocalFilesystemBackend doesn't have it
        mock_env_get.assert_called()
        assert conn == Connection(conn_id="something", conn_type="some-type")
