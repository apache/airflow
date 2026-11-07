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

import subprocess
import sys
import textwrap
from unittest import mock

import httpx2
import pytest

from airflow.sdk import Connection, SecretCache
from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.api.datamodels._generated import ConnectionResponse
from airflow.sdk.bases.secrets_backend import BaseSecretsBackend
from airflow.sdk.configuration import clear_secrets_backends_cache, ensure_secrets_loaded
from airflow.sdk.exceptions import (
    AirflowNotFoundException,
    AirflowRuntimeError,
    AirflowSecretsBackendAccessDenied,
    ErrorType,
)
from airflow.sdk.execution_time.comms import ConnectionResult, ErrorResponse, VariableResult
from airflow.sdk.execution_time.context import (
    _async_get_connection,
    _async_get_variable,
    _get_connection,
    _get_variable,
)
from airflow.sdk.execution_time.secrets import (
    _SERVER_DEFAULT_SECRETS_SEARCH_PATH,
    DEFAULT_SECRETS_SEARCH_PATH_WORKERS,
)
from airflow.sdk.execution_time.secrets.execution_api import ExecutionAPISecretsBackend


class TestExecutionAPISecretsBackend:
    """Test ExecutionAPISecretsBackend."""

    def test_get_connection_via_supervisor_comms(self, mock_supervisor_comms):
        """Test that connection is retrieved via SUPERVISOR_COMMS."""
        conn_response = ConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
            host="example.com",
            port=443,
            schema="https",
            login=None,
            password=None,
            extra=None,
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
        error_response = ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND, detail={"message": "Not found"})
        mock_supervisor_comms.send.return_value = error_response

        backend = ExecutionAPISecretsBackend()
        conn = backend.get_connection("nonexistent")

        assert conn is None
        mock_supervisor_comms.send.assert_called_once()

    def test_get_variable_via_supervisor_comms(self, mock_supervisor_comms):
        """Test that variable is retrieved via SUPERVISOR_COMMS."""
        var_result = VariableResult(key="test_var", value="test_value")
        mock_supervisor_comms.send.return_value = var_result

        backend = ExecutionAPISecretsBackend()
        value = backend.get_variable("test_var")

        assert value == "test_value"
        mock_supervisor_comms.send.assert_called_once()

    def test_get_variable_not_found(self, mock_supervisor_comms):
        """Test that None is returned when variable not found."""
        error_response = ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"message": "Not found"})
        mock_supervisor_comms.send.return_value = error_response

        backend = ExecutionAPISecretsBackend()
        value = backend.get_variable("nonexistent")

        assert value is None
        mock_supervisor_comms.send.assert_called_once()

    def test_get_connection_preserves_exception(self, mock_supervisor_comms: mock.Mock) -> None:
        mock_supervisor_comms.send.side_effect = RuntimeError("Connection failed")

        backend = ExecutionAPISecretsBackend()
        with pytest.raises(RuntimeError, match="Connection failed"):
            backend.get_connection(conn_id="test_conn")

    def test_get_variable_preserves_exception(self, mock_supervisor_comms: mock.Mock) -> None:
        mock_supervisor_comms.send.side_effect = RuntimeError("Communication failed")

        backend = ExecutionAPISecretsBackend()
        with pytest.raises(RuntimeError, match="Communication failed"):
            backend.get_variable(key="test_var")

    def test_get_conn_value_not_implemented(self):
        """Test that get_conn_value raises NotImplementedError."""
        backend = ExecutionAPISecretsBackend()
        with pytest.raises(NotImplementedError, match="Use get_connection instead"):
            backend.get_conn_value("test_conn")

    def test_get_connection_raises_on_permission_denied(self, mock_supervisor_comms):
        """An explicit deny from the Execution API must raise, not fall through.

        Returning None on a 401/403 would let the secrets-backend dispatcher
        fall through to a less-restrictive backend (e.g. EnvironmentVariablesBackend).
        """
        mock_supervisor_comms.send.return_value = ErrorResponse(
            error=ErrorType.PERMISSION_DENIED,
            detail={"conn_id": "denied_conn", "status_code": 403},
        )
        backend = ExecutionAPISecretsBackend()
        with pytest.raises(AirflowSecretsBackendAccessDenied, match="connection 'denied_conn'"):
            backend.get_connection("denied_conn")

    def test_get_variable_raises_on_permission_denied(self, mock_supervisor_comms):
        """An explicit deny from the Execution API must raise for variables too."""
        mock_supervisor_comms.send.return_value = ErrorResponse(
            error=ErrorType.PERMISSION_DENIED,
            detail={"key": "denied_var", "status_code": 403},
        )
        backend = ExecutionAPISecretsBackend()
        with pytest.raises(AirflowSecretsBackendAccessDenied, match="variable 'denied_var'"):
            backend.get_variable("denied_var")

    @pytest.mark.asyncio
    async def test_aget_connection_raises_on_permission_denied(self, mock_supervisor_comms):
        """Async variant must also raise on PERMISSION_DENIED."""

        async def asend(*_args, **_kwargs):
            return ErrorResponse(
                error=ErrorType.PERMISSION_DENIED,
                detail={"conn_id": "denied_conn", "status_code": 403},
            )

        mock_supervisor_comms.asend = asend
        backend = ExecutionAPISecretsBackend()
        with pytest.raises(AirflowSecretsBackendAccessDenied, match="connection 'denied_conn'"):
            await backend.aget_connection("denied_conn")

    @pytest.mark.asyncio
    async def test_aget_variable_raises_on_permission_denied(self, mock_supervisor_comms):
        """Async variant for variables must also raise on PERMISSION_DENIED."""

        async def asend(*_args, **_kwargs):
            return ErrorResponse(
                error=ErrorType.PERMISSION_DENIED,
                detail={"key": "denied_var", "status_code": 403},
            )

        mock_supervisor_comms.asend = asend
        backend = ExecutionAPISecretsBackend()
        with pytest.raises(AirflowSecretsBackendAccessDenied, match="variable 'denied_var'"):
            await backend.aget_variable("denied_var")


class _AnsweringBackend(BaseSecretsBackend):
    """Answer every lookup with ``result``, raising it if it is an exception."""

    def __init__(self, result: object) -> None:
        super().__init__()
        self.result = result
        self.calls = 0

    def _answer(self):
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    def get_connection(self, conn_id: str, team_name: str | None = None):
        return self._answer()

    def get_variable(self, key: str, team_name: str | None = None):
        return self._answer()


class TestDispatcherBackendErrors:
    """An Execution API failure is not an answer about whether the secret exists."""

    @pytest.fixture(autouse=True)
    def reset_cache(self) -> None:
        SecretCache.reset()

    @staticmethod
    async def lookup(kind: str, is_async: bool) -> object:
        if kind == "connection":
            return await _async_get_connection("target") if is_async else _get_connection("target")
        if is_async:
            return await _async_get_variable("target", deserialize_json=False)
        return _get_variable("target", deserialize_json=False)

    @staticmethod
    def use_backends(monkeypatch: pytest.MonkeyPatch, *backends: BaseSecretsBackend) -> None:
        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded", lambda: list(backends)
        )

    @pytest.mark.parametrize("kind", ["connection", "variable"])
    @pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
    @pytest.mark.parametrize("found_later", [False, True], ids=["missing-later", "found-later"])
    @pytest.mark.parametrize(
        ("error_type", "delivery"),
        [
            # How the supervisor reports a failed API call: an error frame that comms raises.
            pytest.param(ErrorType.API_SERVER_ERROR, "frame", id="api-server-error-frame"),
            pytest.param(ErrorType.GENERIC_ERROR, "frame", id="generic-error-frame"),
            pytest.param(ErrorType.API_SERVER_ERROR, "response", id="api-server-error-response"),
            pytest.param(ErrorType.GENERIC_ERROR, "response", id="generic-error-response"),
            # The in-process supervisor behind dag.test() lets the client's error through unchanged.
            pytest.param(ErrorType.API_SERVER_ERROR, "in-process", id="in-process-server-error"),
        ],
    )
    @pytest.mark.asyncio
    async def test_api_failure_is_not_reported_as_missing(
        self,
        mock_supervisor_comms: mock.Mock,
        monkeypatch: pytest.MonkeyPatch,
        kind: str,
        is_async: bool,
        found_later: bool,
        error_type: ErrorType,
        delivery: str,
    ) -> None:
        response = ErrorResponse(error=error_type, detail={"status_code": 500})
        for method in (mock_supervisor_comms.send, mock_supervisor_comms.asend):
            if delivery == "frame":
                method.side_effect = AirflowRuntimeError(response)
            elif delivery == "in-process":
                method.side_effect = ServerResponseError.from_response(
                    httpx2.Response(
                        500,
                        json={"detail": {"reason": "Database error"}},
                        request=httpx2.Request("GET", "http://in-process.invalid./variables/target"),
                    )
                )
            else:
                method.return_value = response
        value = (
            Connection(conn_id="target", conn_type="http", host="fallback") if kind == "connection" else "v"
        )
        later = _AnsweringBackend(value if found_later else None)
        self.use_backends(monkeypatch, ExecutionAPISecretsBackend(), later)

        if found_later:
            assert await self.lookup(kind, is_async) == value
        else:
            with pytest.raises(AirflowRuntimeError) as error:
                await self.lookup(kind, is_async)
            assert error.value.error.error == response.error
            detail = error.value.error.detail
            assert detail is not None
            assert detail["status_code"] == 500
        # A failing Execution API must not stop a later backend from answering.
        assert later.calls == 1

    @pytest.mark.parametrize("kind", ["connection", "variable"])
    @pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
    @pytest.mark.asyncio
    async def test_other_backend_errors_still_report_missing(
        self, monkeypatch: pytest.MonkeyPatch, kind: str, is_async: bool
    ) -> None:
        later = _AnsweringBackend(None)
        self.use_backends(monkeypatch, _AnsweringBackend(RuntimeError("vault unavailable")), later)

        if kind == "connection":
            with pytest.raises(AirflowNotFoundException):
                await self.lookup(kind, is_async)
        else:
            with pytest.raises(AirflowRuntimeError) as error:
                await self.lookup(kind, is_async)
            assert error.value.error.error == ErrorType.VARIABLE_NOT_FOUND
        assert later.calls == 1


class TestDispatcherRefusesFallbackOnDeny:
    """End-to-end: the secrets-backend dispatcher must NOT fall through on an authoritative deny.

    A backend-level raise is not enough on its own — the outer ``except Exception:`` in
    ``context._get_connection`` / ``_get_variable`` / ``_async_get_connection`` previously
    swallowed ``PermissionError`` and silently called the next (less-restrictive) backend.
    These tests pin the dispatcher behaviour by inserting a spy backend AFTER
    ``ExecutionAPISecretsBackend`` and asserting it is never called once the first backend
    raises ``AirflowSecretsBackendAccessDenied``.
    """

    def test_get_connection_does_not_fall_through_after_deny(self, mock_supervisor_comms, monkeypatch):
        from unittest.mock import MagicMock

        from airflow.sdk.execution_time import context as ctx_module

        mock_supervisor_comms.send.return_value = ErrorResponse(
            error=ErrorType.PERMISSION_DENIED,
            detail={"conn_id": "denied_conn", "status_code": 403},
        )

        later_backend = MagicMock(name="LaterBackend")
        later_backend.get_connection.return_value = MagicMock(name="leaked_conn")

        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded",
            lambda: [ExecutionAPISecretsBackend(), later_backend],
        )

        with pytest.raises(AirflowSecretsBackendAccessDenied, match="connection 'denied_conn'"):
            ctx_module._get_connection("denied_conn")

        later_backend.get_connection.assert_not_called()

    def test_get_variable_does_not_fall_through_after_deny(self, mock_supervisor_comms, monkeypatch):
        from unittest.mock import MagicMock

        from airflow.sdk.execution_time import context as ctx_module

        mock_supervisor_comms.send.return_value = ErrorResponse(
            error=ErrorType.PERMISSION_DENIED,
            detail={"key": "denied_var", "status_code": 403},
        )

        later_backend = MagicMock(name="LaterBackend")
        later_backend.get_variable.return_value = "leaked-value"

        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded",
            lambda: [ExecutionAPISecretsBackend(), later_backend],
        )

        with pytest.raises(AirflowSecretsBackendAccessDenied, match="variable 'denied_var'"):
            ctx_module._get_variable("denied_var", deserialize_json=False)

        later_backend.get_variable.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_get_connection_does_not_fall_through_after_deny(
        self, mock_supervisor_comms, monkeypatch
    ):
        from unittest.mock import MagicMock

        from airflow.sdk.execution_time import context as ctx_module

        async def asend(*_args, **_kwargs):
            return ErrorResponse(
                error=ErrorType.PERMISSION_DENIED,
                detail={"conn_id": "denied_conn", "status_code": 403},
            )

        mock_supervisor_comms.asend = asend

        later_backend = MagicMock(name="LaterBackend")
        # The dispatcher prefers aget_connection if present; mock both for safety.
        later_backend.aget_connection = MagicMock(return_value=MagicMock(name="leaked_conn"))
        later_backend.get_connection = MagicMock(return_value=MagicMock(name="leaked_conn"))

        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded",
            lambda: [ExecutionAPISecretsBackend(), later_backend],
        )

        with pytest.raises(AirflowSecretsBackendAccessDenied, match="connection 'denied_conn'"):
            await ctx_module._async_get_connection("denied_conn")

        later_backend.aget_connection.assert_not_called()
        later_backend.get_connection.assert_not_called()


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

    def test_does_not_import_task_runner_outside_task_execution(self):
        code = textwrap.dedent(
            """
            import sys
            from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

            ensure_secrets_backend_loaded()
            print("airflow.sdk.execution_time.task_runner" in sys.modules)
            """
        )

        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == "False"


class TestSecretsBackendMemoisation:
    BACKEND = "airflow.secrets.environment_variables.EnvironmentVariablesBackend"

    def test_nothing_is_memoised_until_a_custom_backend_is_configured(self):
        first = ensure_secrets_loaded(default_backends=_SERVER_DEFAULT_SECRETS_SEARCH_PATH)

        assert ensure_secrets_loaded(default_backends=_SERVER_DEFAULT_SECRETS_SEARCH_PATH) is not first

    def test_backends_are_built_once_with_a_custom_backend(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__SECRETS__BACKEND", self.BACKEND)

        first = ensure_secrets_loaded(default_backends=_SERVER_DEFAULT_SECRETS_SEARCH_PATH)

        assert ensure_secrets_loaded(default_backends=_SERVER_DEFAULT_SECRETS_SEARCH_PATH) is first

    def test_worker_chain_is_memoised_separately_from_the_server_chain(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__SECRETS__BACKEND", self.BACKEND)

        server = ensure_secrets_loaded(default_backends=_SERVER_DEFAULT_SECRETS_SEARCH_PATH)
        worker = ensure_secrets_loaded(default_backends=DEFAULT_SECRETS_SEARCH_PATH_WORKERS)

        assert server is not worker
        assert ensure_secrets_loaded(default_backends=DEFAULT_SECRETS_SEARCH_PATH_WORKERS) is worker

    def test_clearing_the_cache_rebuilds_backends(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__SECRETS__BACKEND", self.BACKEND)

        first = ensure_secrets_loaded(default_backends=_SERVER_DEFAULT_SECRETS_SEARCH_PATH)
        clear_secrets_backends_cache()

        assert ensure_secrets_loaded(default_backends=_SERVER_DEFAULT_SECRETS_SEARCH_PATH) is not first
