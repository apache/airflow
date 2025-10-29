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

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from airflow.sdk.definitions.connection import Connection
from airflow.sdk.execution_time.cache import SecretCache
from airflow.sdk.execution_time.comms import ConnectionResult, VariableResult
from airflow.sdk.execution_time.context import (
    _delete_variable,
    _get_connection,
    _get_variable,
    _set_variable,
)
from airflow.sdk.execution_time.secrets import ExecutionAPISecretsBackend

from tests_common.test_utils.config import conf_vars


class TestConnectionCacheIntegration:
    """Test the integration of SecretCache with connection access."""

    @staticmethod
    @conf_vars({("secrets", "use_cache"): "true"})
    def setup_method():
        SecretCache.reset()
        SecretCache.init()

    @staticmethod
    def teardown_method():
        SecretCache.reset()

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_connection_uses_cache_when_available(self, mock_ensure_backends):
        """Test that _get_connection uses cache when connection is cached."""
        conn_id = "test_conn"
        uri = "postgres://user:pass@host:5432/db"

        SecretCache.save_connection_uri(conn_id, uri)

        result = _get_connection(conn_id)
        assert result.conn_id == conn_id
        assert result.conn_type == "postgres"
        assert result.host == "host"
        assert result.login == "user"
        assert result.password == "pass"
        assert result.port == 5432
        assert result.schema == "db"

        mock_ensure_backends.assert_not_called()

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_connection_from_backend_saves_to_cache(self, mock_ensure_backends):
        """Test that connection from secrets backend is retrieved correctly and cached."""
        conn_id = "test_conn"
        conn = Connection(conn_id=conn_id, conn_type="mysql", host="host", port=3306)

        mock_backend = MagicMock(spec=["get_connection"])
        mock_backend.get_connection.return_value = conn
        mock_ensure_backends.return_value = [mock_backend]

        result = _get_connection(conn_id)
        assert result.conn_id == conn_id
        assert result.conn_type == "mysql"
        mock_backend.get_connection.assert_called_once_with(conn_id=conn_id)

        cached_uri = SecretCache.get_connection_uri(conn_id)
        cached_conn = Connection.from_uri(cached_uri, conn_id=conn_id)
        assert cached_conn.conn_type == "mysql"
        assert cached_conn.host == "host"

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_connection_from_api(self, mock_ensure_backends, mock_supervisor_comms):
        """Test that connection from API server works correctly."""
        conn_id = "test_conn"
        conn_result = ConnectionResult(
            conn_id=conn_id,
            conn_type="mysql",
            host="host",
            port=3306,
            login="user",
            password="pass",
        )

        mock_ensure_backends.return_value = [ExecutionAPISecretsBackend()]

        mock_supervisor_comms.send.return_value = conn_result

        result = _get_connection(conn_id)

        assert result.conn_id == conn_id
        assert result.conn_type == "mysql"
        # Called for GetConnection (and possibly MaskSecret)
        assert mock_supervisor_comms.send.call_count >= 1

        cached_uri = SecretCache.get_connection_uri(conn_id)
        cached_conn = Connection.from_uri(cached_uri, conn_id=conn_id)
        assert cached_conn.conn_type == "mysql"
        assert cached_conn.host == "host"

    @patch("airflow.sdk.execution_time.context.mask_secret")
    def test_get_connection_masks_secrets(self, mock_mask_secret):
        """Test that connection secrets are masked from logs."""
        conn_id = "test_conn"
        conn = Connection(
            conn_id=conn_id, conn_type="mysql", login="user", password="password", extra='{"key": "value"}'
        )

        mock_backend = MagicMock(spec=["get_connection"])
        mock_backend.get_connection.return_value = conn

        with patch(
            "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded", return_value=[mock_backend]
        ):
            result = _get_connection(conn_id)

            assert result.conn_id == conn_id
            # Check that password and extra were masked
            mock_mask_secret.assert_has_calls(
                [
                    call("password"),
                    call('{"key": "value"}'),
                ],
                any_order=True,
            )


class TestVariableCacheIntegration:
    """Test the integration of SecretCache with variable access."""

    @staticmethod
    @conf_vars({("secrets", "use_cache"): "true"})
    def setup_method():
        SecretCache.reset()
        SecretCache.init()

    @staticmethod
    def teardown_method():
        SecretCache.reset()

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_variable_uses_cache_when_available(self, mock_ensure_backends):
        """Test that _get_variable uses cache when variable is cached."""
        key = "test_key"
        value = "test_value"
        SecretCache.save_variable(key, value)

        result = _get_variable(key, deserialize_json=False)
        assert result == value
        mock_ensure_backends.assert_not_called()

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_variable_from_backend_saves_to_cache(self, mock_ensure_backends):
        """Test that variable from secrets backend is saved to cache."""
        key = "test_key"
        value = "test_value"

        mock_backend = MagicMock(spec=["get_variable"])
        mock_backend.get_variable.return_value = value
        mock_ensure_backends.return_value = [mock_backend]

        result = _get_variable(key, deserialize_json=False)
        assert result == value
        mock_backend.get_variable.assert_called_once_with(key=key)
        cached_value = SecretCache.get_variable(key)
        assert cached_value == value

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_variable_from_api_saves_to_cache(self, mock_ensure_backends, mock_supervisor_comms):
        """Test that variable from API server is saved to cache."""
        key = "test_key"
        value = "test_value"
        var_result = VariableResult(key=key, value=value)

        mock_ensure_backends.return_value = [ExecutionAPISecretsBackend()]
        mock_supervisor_comms.send.return_value = var_result

        result = _get_variable(key, deserialize_json=False)

        assert result == value
        cached_value = SecretCache.get_variable(key)
        assert cached_value == value

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_variable_with_json_deserialization(self, mock_ensure_backends):
        """Test that _get_variable handles JSON deserialization correctly with cache."""
        key = "test_key"
        json_value = '{"key": "value", "number": 42}'
        SecretCache.save_variable(key, json_value)

        result = _get_variable(key, deserialize_json=True)
        assert result == {"key": "value", "number": 42}
        cached_value = SecretCache.get_variable(key)
        assert cached_value == json_value

    def test_set_variable_invalidates_cache(self, mock_supervisor_comms):
        """Test that _set_variable invalidates the cache."""
        key = "test_key"
        old_value = "old_value"
        new_value = "new_value"
        SecretCache.save_variable(key, old_value)

        _set_variable(key, new_value)
        mock_supervisor_comms.send.assert_called_once()
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable(key)

    def test_delete_variable_invalidates_cache(self, mock_supervisor_comms):
        """Test that _delete_variable invalidates the cache."""
        key = "test_key"
        value = "test_value"
        SecretCache.save_variable(key, value)

        from airflow.sdk.execution_time.comms import OKResponse

        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        _delete_variable(key)
        mock_supervisor_comms.send.assert_called_once()
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable(key)


class TestAsyncConnectionCache:
    """Test the integration of SecretCache with async connection access."""

    @staticmethod
    @conf_vars({("secrets", "use_cache"): "true"})
    def setup_method():
        SecretCache.reset()
        SecretCache.init()

    @staticmethod
    def teardown_method():
        SecretCache.reset()

    @pytest.mark.asyncio
    async def test_async_get_connection_uses_cache(self):
        """Test that _async_get_connection uses cache when connection is cached."""
        from airflow.sdk.execution_time.context import _async_get_connection

        conn_id = "test_conn"
        uri = "postgres://user:pass@host:5432/db"

        SecretCache.save_connection_uri(conn_id, uri)

        result = await _async_get_connection(conn_id)
        assert result.conn_id == conn_id
        assert result.conn_type == "postgres"
        assert result.host == "host"
        assert result.login == "user"
        assert result.password == "pass"
        assert result.port == 5432
        assert result.schema == "db"

    @pytest.mark.asyncio
    async def test_async_get_connection_from_api(self, mock_supervisor_comms):
        """Test that async connection from API server works correctly."""
        from airflow.sdk.execution_time.context import _async_get_connection

        conn_id = "test_conn"
        conn_result = ConnectionResult(
            conn_id=conn_id,
            conn_type="mysql",
            host="host",
            port=3306,
        )

        # Configure asend to return the conn_result when awaited
        mock_supervisor_comms.asend = AsyncMock(return_value=conn_result)

        result = await _async_get_connection(conn_id)

        assert result.conn_id == conn_id
        assert result.conn_type == "mysql"
        mock_supervisor_comms.asend.assert_called_once()

        cached_uri = SecretCache.get_connection_uri(conn_id)
        cached_conn = Connection.from_uri(cached_uri, conn_id=conn_id)
        assert cached_conn.conn_type == "mysql"
        assert cached_conn.host == "host"


class TestCacheDisabled:
    """Test behavior when cache is disabled."""

    @staticmethod
    @conf_vars({("secrets", "use_cache"): "false"})
    def setup_method():
        SecretCache.reset()
        SecretCache.init()

    @staticmethod
    def teardown_method():
        SecretCache.reset()

    @patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded")
    def test_get_connection_no_cache_when_disabled(self, mock_ensure_backends, mock_supervisor_comms):
        """Test that cache is not used when disabled."""
        conn_id = "test_conn"
        conn_result = ConnectionResult(conn_id=conn_id, conn_type="mysql", host="host")

        mock_ensure_backends.return_value = [ExecutionAPISecretsBackend()]

        mock_supervisor_comms.send.return_value = conn_result

        result = _get_connection(conn_id)

        assert result.conn_id == conn_id
        # Called for GetConnection (and possibly MaskSecret)
        assert mock_supervisor_comms.send.call_count >= 1

        _get_connection(conn_id)

        # Called twice since cache is disabled
        assert mock_supervisor_comms.send.call_count >= 2
