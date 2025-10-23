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

import pytest

from airflow.sdk.definitions.connection import Connection
from airflow.sdk.execution_time.secrets.execution_api import ExecutionAPISecretsBackend


class TestExecutionAPISecretsBackend:
    """Test ExecutionAPISecretsBackend."""

    def test_get_connection_via_supervisor_comms(self, mock_supervisor_comms):
        """Test that connection is retrieved via SUPERVISOR_COMMS."""
        from airflow.sdk.api.datamodels._generated import ConnectionResponse
        from airflow.sdk.execution_time.comms import ConnectionResult

        # Mock connection response
        conn_response = ConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
            host="example.com",
            port=443,
            schema="https",
        )
        conn_result = ConnectionResult.from_conn_response(conn_response)
        mock_supervisor_comms.send.return_value = conn_result

        backend = ExecutionAPISecretsBackend()
        conn = backend.get_connection("test_conn")

        assert conn is not None
        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "http"
        assert conn.host == "example.com"
        mock_supervisor_comms.send.assert_called_once()

    def test_get_connection_not_found(self, mock_supervisor_comms):
        """Test that None is returned when connection not found."""
        from airflow.sdk.exceptions import ErrorType
        from airflow.sdk.execution_time.comms import ErrorResponse

        # Mock error response
        error_response = ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND, detail={"message": "Not found"})
        mock_supervisor_comms.send.return_value = error_response

        backend = ExecutionAPISecretsBackend()
        conn = backend.get_connection("nonexistent")

        assert conn is None
        mock_supervisor_comms.send.assert_called_once()

    def test_get_variable_via_supervisor_comms(self, mock_supervisor_comms):
        """Test that variable is retrieved via SUPERVISOR_COMMS."""
        from airflow.sdk.execution_time.comms import VariableResult

        # Mock variable response
        var_result = VariableResult(key="test_var", value="test_value")
        mock_supervisor_comms.send.return_value = var_result

        backend = ExecutionAPISecretsBackend()
        value = backend.get_variable("test_var")

        assert value == "test_value"
        mock_supervisor_comms.send.assert_called_once()

    def test_get_variable_not_found(self, mock_supervisor_comms):
        """Test that None is returned when variable not found."""
        from airflow.sdk.exceptions import ErrorType
        from airflow.sdk.execution_time.comms import ErrorResponse

        # Mock error response
        error_response = ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"message": "Not found"})
        mock_supervisor_comms.send.return_value = error_response

        backend = ExecutionAPISecretsBackend()
        value = backend.get_variable("nonexistent")

        assert value is None
        mock_supervisor_comms.send.assert_called_once()

    def test_get_connection_handles_exception(self, mock_supervisor_comms):
        """Test that exceptions are handled gracefully."""
        mock_supervisor_comms.send.side_effect = RuntimeError("Connection failed")

        backend = ExecutionAPISecretsBackend()
        conn = backend.get_connection("test_conn")

        # Should return None on exception to allow fallback to other backends
        assert conn is None

    def test_get_variable_handles_exception(self, mock_supervisor_comms):
        """Test that exceptions are handled gracefully for variables."""
        mock_supervisor_comms.send.side_effect = RuntimeError("Communication failed")

        backend = ExecutionAPISecretsBackend()
        value = backend.get_variable("test_var")

        # Should return None on exception to allow fallback to other backends
        assert value is None

    def test_get_conn_value_not_implemented(self):
        """Test that get_conn_value raises NotImplementedError."""
        backend = ExecutionAPISecretsBackend()
        with pytest.raises(NotImplementedError, match="Use get_connection instead"):
            backend.get_conn_value("test_conn")

    def test_runtime_error_triggers_greenback_fallback(self, mocker, mock_supervisor_comms):
        """
        Test that RuntimeError from async_to_sync triggers greenback fallback.

        This test verifies the fix for issue #57145: when SUPERVISOR_COMMS.send()
        raises the specific RuntimeError about async_to_sync in an event loop,
        the backend catches it and uses greenback to call aget_connection().
        """

        # Expected connection to be returned
        expected_conn = Connection(
            conn_id="databricks_default",
            conn_type="databricks",
            host="example.databricks.com",
        )

        # Simulate the RuntimeError that triggers greenback fallback
        mock_supervisor_comms.send.side_effect = RuntimeError(
            "You cannot use AsyncToSync in the same thread as an async event loop"
        )

        # Mock the greenback and asyncio modules that are imported inside the exception handler
        mocker.patch("greenback.has_portal", return_value=True)
        mock_greenback_await = mocker.patch("greenback.await_", return_value=expected_conn)
        mocker.patch("asyncio.current_task")

        backend = ExecutionAPISecretsBackend()
        conn = backend.get_connection("databricks_default")

        # Verify we got the expected connection
        assert conn is not None
        assert conn.conn_id == "databricks_default"
        # Verify the greenback fallback was called
        mock_greenback_await.assert_called_once()
        # Verify send was attempted first (and raised RuntimeError)
        mock_supervisor_comms.send.assert_called_once()


class TestContextDetection:
    """Test context detection in ensure_secrets_backend_loaded."""

    def test_client_context_with_supervisor_comms(self, mock_supervisor_comms):
        """Client context: SUPERVISOR_COMMS set → uses worker chain."""
        from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

        backends = ensure_secrets_backend_loaded()
        backend_classes = [type(b).__name__ for b in backends]
        assert "ExecutionAPISecretsBackend" in backend_classes
        assert "MetastoreBackend" not in backend_classes

    def test_server_context_with_env_var(self, monkeypatch):
        """Server context: env var set → uses server chain."""
        import sys

        from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

        monkeypatch.setenv("_AIRFLOW_PROCESS_CONTEXT", "server")
        # Ensure SUPERVISOR_COMMS is not available
        if "airflow.sdk.execution_time.task_runner" in sys.modules:
            monkeypatch.delitem(sys.modules, "airflow.sdk.execution_time.task_runner")

        backends = ensure_secrets_backend_loaded()
        backend_classes = [type(b).__name__ for b in backends]
        assert "MetastoreBackend" in backend_classes
        assert "ExecutionAPISecretsBackend" not in backend_classes

    def test_fallback_context_no_markers(self, monkeypatch):
        """Fallback context: no SUPERVISOR_COMMS, no env var → only env vars + external."""
        import sys

        from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

        # Ensure no SUPERVISOR_COMMS
        if "airflow.sdk.execution_time.task_runner" in sys.modules:
            monkeypatch.delitem(sys.modules, "airflow.sdk.execution_time.task_runner")

        # Ensure no env var
        monkeypatch.delenv("_AIRFLOW_PROCESS_CONTEXT", raising=False)

        backends = ensure_secrets_backend_loaded()
        backend_classes = [type(b).__name__ for b in backends]
        assert "EnvironmentVariablesBackend" in backend_classes
        assert "MetastoreBackend" not in backend_classes
        assert "ExecutionAPISecretsBackend" not in backend_classes
