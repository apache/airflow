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


class TestContextDetection:
    """Test context detection in ensure_secrets_backend_loaded."""

    def test_client_context_with_supervisor_comms(self, mock_supervisor_comms):
        """Test that client chain is used when SUPERVISOR_COMMS is set."""
        from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

        backends = ensure_secrets_backend_loaded()

        # Should use worker chain (ExecutionAPISecretsBackend)
        backend_classes = [type(b).__name__ for b in backends]
        assert "ExecutionAPISecretsBackend" in backend_classes

    def test_server_context_without_supervisor_comms(self, monkeypatch):
        """Test that server chain is used when SUPERVISOR_COMMS is not available."""
        # Mock task_runner to not have SUPERVISOR_COMMS
        import sys
        from unittest.mock import Mock

        from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

        mock_task_runner = Mock()
        mock_task_runner.SUPERVISOR_COMMS = None
        monkeypatch.setitem(sys.modules, "airflow.sdk.execution_time.task_runner", mock_task_runner)

        backends = ensure_secrets_backend_loaded()

        # Should use server chain (MetastoreBackend)
        backend_classes = [type(b).__name__ for b in backends]
        assert "MetastoreBackend" in backend_classes
        assert "ExecutionAPISecretsBackend" not in backend_classes

    def test_server_context_when_import_fails(self, monkeypatch):
        """Test that server chain is used when task_runner import fails."""
        # Mock sys.modules to make task_runner import fail
        import sys

        from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

        # Remove task_runner from sys.modules to simulate import failure
        if "airflow.sdk.execution_time.task_runner" in sys.modules:
            monkeypatch.delitem(sys.modules, "airflow.sdk.execution_time.task_runner")

        # Make any attempt to import raise an error
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "task_runner" in name:
                raise ImportError("Mocked import failure")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)

        backends = ensure_secrets_backend_loaded()

        # Should default to server chain
        backend_classes = [type(b).__name__ for b in backends]
        assert "MetastoreBackend" in backend_classes
