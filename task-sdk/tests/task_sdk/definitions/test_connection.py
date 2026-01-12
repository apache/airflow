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

import json
from unittest import mock
from urllib.parse import urlparse

import pytest

from airflow.sdk import Connection
from airflow.sdk.configuration import initialize_secrets_backends
from airflow.sdk.exceptions import AirflowException, AirflowNotFoundException, ErrorType
from airflow.sdk.execution_time.comms import ConnectionResult, ErrorResponse
from airflow.sdk.execution_time.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

from tests_common.test_utils.config import conf_vars


class TestConnections:
    @pytest.fixture
    def mock_providers_manager(self):
        """Mock the ProvidersManager to return predefined hooks."""
        with mock.patch("airflow.providers_manager.ProvidersManager") as mock_manager:
            yield mock_manager

    @mock.patch("airflow.sdk._shared.module_loading.import_string")
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
        mock_supervisor_comms.send.return_value = conn_result

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

    def test_conn_get_not_found(self, mock_supervisor_comms):
        error_response = ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND)
        mock_supervisor_comms.send.return_value = error_response

        with pytest.raises(AirflowNotFoundException, match="The conn_id `mysql_conn` isn't defined"):
            _ = Connection.get(conn_id="mysql_conn")

    def test_to_dict(self):
        """Test that to_dict returns correct dictionary representation."""
        connection = Connection(
            conn_id="test_conn",
            conn_type="http",
            login="user",
            password="pass",
            host="https://api.example.com/",
            port=443,
            schema="https",
            extra='{"timeout": 30, "verify": false}',
        )

        result = connection.to_dict()

        assert result == {
            "conn_id": "test_conn",
            "conn_type": "http",
            "description": None,
            "host": "https://api.example.com/",
            "login": "user",
            "password": "pass",
            "schema": "https",
            "port": 443,
            "extra": {"timeout": 30, "verify": False},
        }

    def test_as_json(self):
        """Test that as_json returns valid JSON string without conn_id."""
        connection = Connection(
            conn_id="test_conn",
            conn_type="postgres",
            host="localhost",
            port=5432,
        )

        json_str = connection.as_json()
        result = json.loads(json_str)

        # Should not include conn_id
        assert "conn_id" not in result
        assert result["conn_type"] == "postgres"
        assert result["host"] == "localhost"
        assert result["port"] == 5432

    def test_from_json(self):
        """Test that from_json creates Connection with type normalization."""
        json_data = {
            "conn_type": "postgresql",
            "host": "localhost",
            "port": "5432",
        }
        expected_id = "postgres"
        connection = Connection.from_json(json.dumps(json_data), conn_id="test_conn")

        assert connection.conn_id == "test_conn"
        assert connection.conn_type == expected_id
        assert connection.host == "localhost"
        assert connection.port == 5432

    def test_extra_dejson_property(self):
        """Test that extra_dejson property correctly deserializes JSON extra field."""
        connection = Connection(
            conn_id="test_conn",
            conn_type="http",
            extra='{"timeout": 30, "verify": false, "retries": 3}',
        )

        result = connection.extra_dejson
        assert result == {"timeout": 30, "verify": False, "retries": 3}

        connection.extra = None
        assert connection.extra_dejson == {}

        connection.extra = '{"auth": {"type": "oauth"}, "headers": {"User-Agent": "Airflow"}}'
        assert connection.extra_dejson == {"auth": {"type": "oauth"}, "headers": {"User-Agent": "Airflow"}}


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
        assert len(backends) == 3
        backend_classes = [backend.__class__.__name__ for backend in backends]
        assert "LocalFilesystemBackend" in backend_classes

        conn = Connection.get(conn_id="something")
        # mock_env is only called when LocalFilesystemBackend doesn't have it
        mock_env_get.assert_called()
        assert conn == Connection(conn_id="something", conn_type="some-type")


class TestConnectionFromUri:
    """Test the Connection.from_uri() classmethod."""

    def test_from_uri_basic(self):
        """Test basic URI parsing."""
        uri = "postgres://user:pass@host:5432/db"
        conn = Connection.from_uri(uri, conn_id="test_conn")

        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "postgres"
        assert conn.host == "host"
        assert conn.login == "user"
        assert conn.password == "pass"
        assert conn.port == 5432
        assert conn.schema == "db"
        assert conn.extra is None

    def test_from_uri_with_query_params(self):
        """Test URI parsing with query parameters."""
        uri = "mysql://user:pass@host:3306/db?charset=utf8&timeout=30"
        conn = Connection.from_uri(uri, conn_id="test_conn")

        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "mysql"
        assert conn.host == "host"
        assert conn.login == "user"
        assert conn.password == "pass"
        assert conn.port == 3306
        assert conn.schema == "db"
        # Extra should be JSON string with query params
        extra_dict = json.loads(conn.extra)
        assert extra_dict == {"charset": "utf8", "timeout": "30"}

    def test_from_uri_with_extra_key(self):
        """Test URI parsing with __extra__ query parameter."""
        extra_value = json.dumps({"ssl_mode": "require", "connect_timeout": 10})
        uri = f"postgres://user:pass@host:5432/db?__extra__={extra_value}"
        conn = Connection.from_uri(uri, conn_id="test_conn")

        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "postgres"
        assert conn.extra == extra_value

    def test_from_uri_with_protocol_in_host(self):
        """Test URI parsing with protocol in host (double ://)."""
        uri = "http://https://example.com:8080/path"
        conn = Connection.from_uri(uri, conn_id="test_conn")

        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "http"
        assert conn.host == "https://example.com"
        assert conn.port == 8080
        assert conn.schema == "path"

    def test_from_uri_encoded_credentials(self):
        """Test URI parsing with URL-encoded credentials."""
        uri = "postgres://user%40domain:pass%21word@host:5432/db"
        conn = Connection.from_uri(uri, conn_id="test_conn")

        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "postgres"
        assert conn.login == "user@domain"
        assert conn.password == "pass!word"

    def test_from_uri_minimal(self):
        """Test URI parsing with minimal information."""
        uri = "redis://"
        conn = Connection.from_uri(uri, conn_id="test_conn")

        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "redis"
        assert conn.host == ""  # urlsplit returns empty string, not None for minimal URI
        assert conn.login is None
        assert conn.password is None
        assert conn.port is None
        assert conn.schema == ""

    def test_from_uri_conn_type_normalization(self):
        """Test that connection types are normalized."""
        # postgresql -> postgres
        uri = "postgresql://user:pass@host:5432/db"
        conn = Connection.from_uri(uri, conn_id="test_conn")
        assert conn.conn_type == "postgres"

        # hyphen to underscore
        uri = "google-cloud-platform://user:pass@host/db"
        conn = Connection.from_uri(uri, conn_id="test_conn")
        assert conn.conn_type == "google_cloud_platform"

    def test_from_uri_too_many_schemes_error(self):
        """Test that too many schemes in URI raises an error."""
        uri = "http://https://ftp://example.com"
        with pytest.raises(AirflowException, match="Invalid connection string"):
            Connection.from_uri(uri, conn_id="test_conn")

    def test_from_uri_invalid_protocol_host_error(self):
        """Test that invalid protocol host raises an error."""
        uri = "http://user@host://example.com"
        with pytest.raises(AirflowException, match="Invalid connection string"):
            Connection.from_uri(uri, conn_id="test_conn")

    def test_from_uri_roundtrip(self):
        """Test that from_uri and get_uri are inverse operations."""
        original_uri = "postgres://user:pass@host:5432/db?param1=value1&param2=value2"
        conn = Connection.from_uri(original_uri, conn_id="test_conn")
        roundtrip_uri = conn.get_uri()

        # Parse both URIs to compare (order of query params might differ)
        conn_from_original = Connection.from_uri(original_uri, conn_id="test")
        conn_from_roundtrip = Connection.from_uri(roundtrip_uri, conn_id="test")

        assert conn_from_original.conn_type == conn_from_roundtrip.conn_type
        assert conn_from_original.host == conn_from_roundtrip.host
        assert conn_from_original.login == conn_from_roundtrip.login
        assert conn_from_original.password == conn_from_roundtrip.password
        assert conn_from_original.port == conn_from_roundtrip.port
        assert conn_from_original.schema == conn_from_roundtrip.schema
        # Check extra content is equivalent (JSON order might differ)
        if conn_from_original.extra:
            original_extra = json.loads(conn_from_original.extra)
            roundtrip_extra = json.loads(conn_from_roundtrip.extra)
            assert original_extra == roundtrip_extra
