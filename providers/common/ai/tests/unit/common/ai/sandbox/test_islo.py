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

pytest.importorskip("islo")

from islo.errors import NotFoundError
from islo.types import LifecyclePolicy

from airflow.providers.common.ai.sandbox.islo import IsloSandboxBackend

_BASE_HOOK_PATH = "airflow.providers.common.ai.sandbox.islo.BaseHook"
_ISLO_PATH = "airflow.providers.common.ai.sandbox.islo.Islo"


def _make_mock_connection(password="secret-key", host=None, extra=None):
    conn = MagicMock()
    conn.password = password
    conn.host = host
    conn.extra_dejson = extra or {}
    return conn


def _backend_with_mock_client(**kwargs) -> tuple[IsloSandboxBackend, MagicMock]:
    backend = IsloSandboxBackend(**kwargs)
    mock_client = MagicMock()
    mock_client.sandboxes.exec_in_sandbox.return_value.exec_id = "exec-1"
    backend._client = mock_client
    return backend, mock_client


def _make_exec_result(
    *,
    status="completed",
    stdout="out",
    stderr="err",
    exit_code=0,
    truncated=False,
):
    result = MagicMock()
    result.status = status
    result.stdout = stdout
    result.stderr = stderr
    result.exit_code = exit_code
    result.truncated = truncated
    return result


class TestIsloSandboxBackendClient:
    @patch(_ISLO_PATH, autospec=True)
    @patch(_BASE_HOOK_PATH, autospec=True)
    def test_init_resolves_nothing(self, mock_base_hook, mock_islo_cls):
        IsloSandboxBackend()
        mock_base_hook.get_connection.assert_not_called()
        mock_islo_cls.assert_not_called()

    @patch(_ISLO_PATH, autospec=True)
    @patch(_BASE_HOOK_PATH, autospec=True)
    def test_connection_maps_to_client_kwargs(self, mock_base_hook, mock_islo_cls):
        mock_base_hook.get_connection.return_value = _make_mock_connection(
            password="  secret-key  ",
            host="https://compute.islo.dev",
            extra={"base_url": "https://api.islo.dev", "timeout": 30},
        )

        backend = IsloSandboxBackend("islo_conn")
        client = backend._get_client()

        mock_base_hook.get_connection.assert_called_once_with("islo_conn")
        mock_islo_cls.assert_called_once_with(
            api_key="secret-key",
            compute_url="https://compute.islo.dev",
            base_url="https://api.islo.dev",
            timeout=30.0,
        )
        assert client is mock_islo_cls.return_value

    @patch(_ISLO_PATH, autospec=True)
    @patch(_BASE_HOOK_PATH, autospec=True)
    def test_connection_omits_unset_optional_fields(self, mock_base_hook, mock_islo_cls):
        mock_base_hook.get_connection.return_value = _make_mock_connection()

        IsloSandboxBackend()._get_client()

        mock_islo_cls.assert_called_once_with(api_key="secret-key")

    @patch(_ISLO_PATH, autospec=True)
    @patch(_BASE_HOOK_PATH, autospec=True)
    def test_none_conn_id_uses_sdk_environment_resolution(self, mock_base_hook, mock_islo_cls):
        IsloSandboxBackend(islo_conn_id=None)._get_client()

        mock_base_hook.get_connection.assert_not_called()
        mock_islo_cls.assert_called_once_with()

    @pytest.mark.parametrize("password", [None, "   "])
    @patch(_BASE_HOOK_PATH, autospec=True)
    def test_missing_password_raises_clear_error(self, mock_base_hook, password):
        mock_base_hook.get_connection.return_value = _make_mock_connection(password=password)

        with pytest.raises(ValueError, match="no password.*islo API key"):
            IsloSandboxBackend("islo_conn")._get_client()

    @patch(_ISLO_PATH, autospec=True)
    @patch(_BASE_HOOK_PATH, autospec=True)
    def test_caches_client(self, mock_base_hook, mock_islo_cls):
        mock_base_hook.get_connection.return_value = _make_mock_connection()

        backend = IsloSandboxBackend()
        assert backend._get_client() is backend._get_client()

        mock_base_hook.get_connection.assert_called_once()
        mock_islo_cls.assert_called_once()

    @patch(_BASE_HOOK_PATH, autospec=True)
    def test_rejects_invalid_connection_timeout(self, mock_base_hook):
        mock_base_hook.get_connection.return_value = _make_mock_connection(extra={"timeout": 0})

        with pytest.raises(ValueError, match="connection extra timeout"):
            IsloSandboxBackend()._get_client()


class TestIsloSandboxBackendCreate:
    def test_create_defaults_disable_internet(self):
        backend, mock_client = _backend_with_mock_client()

        backend.create()

        kwargs = mock_client.sandboxes.create_sandbox.call_args.kwargs
        assert set(kwargs) == {"name", "lifecycle", "internet_enabled"}
        assert kwargs["name"].startswith("airflow-sandbox-")
        assert kwargs["internet_enabled"] is False
        assert kwargs["lifecycle"] == LifecyclePolicy(delete_after=3600)

    def test_create_defers_internet_to_server_when_none(self):
        backend, mock_client = _backend_with_mock_client(internet_enabled=None)

        backend.create()

        kwargs = mock_client.sandboxes.create_sandbox.call_args.kwargs
        assert set(kwargs) == {"name", "lifecycle"}

    def test_create_passes_set_kwargs(self):
        backend, mock_client = _backend_with_mock_client(
            image="python:3.12", vcpus=2, memory_mb=1024, internet_enabled=False, delete_after=60
        )

        backend.create()

        kwargs = mock_client.sandboxes.create_sandbox.call_args.kwargs
        assert kwargs["image"] == "python:3.12"
        assert kwargs["vcpus"] == 2
        assert kwargs["memory_mb"] == 1024
        assert kwargs["internet_enabled"] is False
        assert kwargs["lifecycle"] == LifecyclePolicy(delete_after=60)

    def test_create_returns_server_normalized_name(self):
        backend, mock_client = _backend_with_mock_client()
        mock_client.sandboxes.create_sandbox.return_value.name = "airflow-sbx-normalized"

        assert backend.create() == "airflow-sbx-normalized"

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"image": ""}, "image"),
            ({"vcpus": 0}, "vcpus"),
            ({"memory_mb": -1}, "memory_mb"),
            ({"delete_after": float("nan")}, "delete_after"),
        ],
    )
    def test_init_rejects_invalid_resources(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            IsloSandboxBackend(**kwargs)


class TestIsloSandboxBackendRun:
    def test_run_maps_exec_result_and_preserves_truncation(self):
        backend, mock_client = _backend_with_mock_client()
        mock_client.sandboxes.get_exec_result.return_value = _make_exec_result(exit_code=3, truncated=True)

        result = backend.run("sbx", ["python3", "-c", "x"], timeout=10.0)

        mock_client.sandboxes.exec_in_sandbox.assert_called_once_with(
            "sbx",
            command=["python3", "-c", "x"],
            timeout_secs=10,
        )
        mock_client.sandboxes.get_exec_result.assert_called_once_with("sbx", "exec-1")
        assert result.exit_code == 3
        assert result.stdout == "out"
        assert result.stderr == "err"
        assert result.timed_out is False
        assert result.truncated is True

    def test_run_maps_server_enforced_timeout(self):
        backend, mock_client = _backend_with_mock_client()
        mock_client.sandboxes.get_exec_result.return_value = _make_exec_result(
            status="timeout", stdout="", stderr="", exit_code=None
        )

        result = backend.run("sbx", ["sleep", "60"], timeout=0.5)

        assert result.timed_out is True
        assert result.exit_code == -1
        mock_client.sandboxes.exec_in_sandbox.assert_called_once_with(
            "sbx",
            command=["sleep", "60"],
            timeout_secs=1,
        )

    @patch("airflow.providers.common.ai.sandbox.islo.time.monotonic", side_effect=[0.0, 10.0])
    def test_run_deletes_sandbox_when_server_does_not_stop_command(self, mock_monotonic):
        backend, mock_client = _backend_with_mock_client()

        result = backend.run("sbx", ["sleep", "60"], timeout=1.0)

        assert result.timed_out is True
        assert result.sandbox_terminated is True
        mock_client.sandboxes.get_exec_result.assert_not_called()
        mock_client.sandboxes.delete_sandbox.assert_called_once_with(sandbox_name="sbx")

    @patch("airflow.providers.common.ai.sandbox.islo.time.monotonic", side_effect=[0.0, 10.0])
    def test_run_termination_cleanup_failure_does_not_fail_task(self, mock_monotonic):
        # A transient delete failure during the timeout-termination path must not
        # propagate — the server-side TTL is the backstop.
        backend, mock_client = _backend_with_mock_client()
        mock_client.sandboxes.delete_sandbox.side_effect = RuntimeError("transient 503")

        result = backend.run("sbx", ["sleep", "60"], timeout=1.0)

        assert result.timed_out is True
        assert result.sandbox_terminated is True

    def test_run_caps_oversized_output(self):
        from airflow.providers.common.ai.sandbox.base import _MAX_OUTPUT_CHARS

        backend, mock_client = _backend_with_mock_client()
        mock_client.sandboxes.get_exec_result.return_value = _make_exec_result(
            stdout="x" * (_MAX_OUTPUT_CHARS + 50), stderr="y", truncated=False
        )

        result = backend.run("sbx", ["python3", "-c", "x"], timeout=10.0)

        assert len(result.stdout) == _MAX_OUTPUT_CHARS
        assert result.truncated is True

    @pytest.mark.parametrize("timeout", [0, -1, float("nan")])
    def test_run_rejects_invalid_timeout(self, timeout):
        backend, _ = _backend_with_mock_client()

        with pytest.raises(ValueError, match="timeout"):
            backend.run("sbx", ["true"], timeout=timeout)


class TestIsloSandboxBackendDestroy:
    def test_destroy_deletes_sandbox(self):
        backend, mock_client = _backend_with_mock_client()

        backend.destroy("airflow-sbx-abc")

        mock_client.sandboxes.delete_sandbox.assert_called_once_with(sandbox_name="airflow-sbx-abc")

    def test_destroy_swallows_missing_sandbox(self):
        backend, mock_client = _backend_with_mock_client()
        mock_client.sandboxes.delete_sandbox.side_effect = NotFoundError(body=None)

        backend.destroy("airflow-sbx-abc")  # Does not raise.
