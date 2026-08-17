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

import base64
import builtins
from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("ascii_box_sdk")

from ascii_box_sdk.exceptions import ApiException

from airflow.providers.common.ai.sandbox.ascii_box import AsciiBoxSandboxBackend
from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)

_BASE_HOOK_PATH = "airflow.providers.common.ai.sandbox.ascii_box.BaseHook"


def _api_error(status: int) -> ApiException:
    return ApiException(status=status)


def _connection(*, password: str | None = "box_secret", host: str | None = None, extra: dict | None = None):
    return SimpleNamespace(password=password, host=host, extra_dejson=extra or {})


def _command_result(
    *,
    exit_code=0,
    stdout="",
    stderr="",
    timed_out=False,
    stdout_truncated=False,
    stderr_truncated=False,
):
    return SimpleNamespace(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        timed_out=timed_out,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
    )


def _backend_with_api(**kwargs) -> tuple[AsciiBoxSandboxBackend, mock.MagicMock]:
    backend = AsciiBoxSandboxBackend(**kwargs)
    api = mock.MagicMock(spec=["create", "update", "get", "command", "read_file", "write_file", "api_client"])
    api.api_client = mock.MagicMock(spec=["param_serialize", "call_api"])
    backend._box_api = api
    backend._request_timeout = 30.0
    backend._resolved_no_env = True
    return backend, api


def test_missing_sdk_error_is_actionable():
    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name.startswith("ascii_box_sdk"):
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    backend = AsciiBoxSandboxBackend(box_conn_id=None)
    with mock.patch.dict("os.environ", {"BOX_API_KEY": "box_key"}, clear=False):
        with mock.patch("builtins.__import__", side_effect=blocked_import):
            with pytest.raises(SandboxTerminalError, match="sandbox-ascii-box"):
                backend._get_api()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"machine_type": "xlarge"}, "machine_type"),
        ({"ttl_seconds": 0}, "ttl_seconds"),
        ({"ready_timeout": 0}, "ready_timeout"),
    ],
)
def test_constructor_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        AsciiBoxSandboxBackend(**kwargs)


class TestConnection:
    @mock.patch("ascii_box_sdk.api.box_api.BoxApi", autospec=True)
    @mock.patch("ascii_box_sdk.ApiClient", autospec=True)
    @mock.patch("ascii_box_sdk.Configuration", autospec=True)
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_airflow_connection_fields_and_allowlisted_extras_are_forwarded(
        self, hook, configuration, _client, _box_api
    ):
        hook.get_connection.return_value = _connection(
            password=" key ",
            host="https://box.example/api/box/v1",
            extra={"timeout": "12.5", "no_env": "false", "ignored": "value"},
        )
        backend = AsciiBoxSandboxBackend(box_conn_id="my_box")

        backend._get_api()

        hook.get_connection.assert_called_once_with("my_box")
        configuration.assert_called_once_with(host="https://box.example/api/box/v1", access_token="key")
        assert backend._resolved_no_env is False
        assert backend._request_timeout == 12.5

    @mock.patch("ascii_box_sdk.api.box_api.BoxApi", autospec=True)
    @mock.patch("ascii_box_sdk.ApiClient", autospec=True)
    @mock.patch("ascii_box_sdk.Configuration", autospec=True)
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_connection_is_resolved_once_and_cached(self, hook, _configuration, _client, _box_api):
        hook.get_connection.return_value = _connection()
        backend = AsciiBoxSandboxBackend()

        backend._get_api()
        backend._get_api()

        hook.get_connection.assert_called_once_with("ascii_box_default")

    @mock.patch("ascii_box_sdk.api.box_api.BoxApi", autospec=True)
    @mock.patch("ascii_box_sdk.ApiClient", autospec=True)
    @mock.patch("ascii_box_sdk.Configuration", autospec=True)
    def test_none_connection_id_reads_environment(self, configuration, _client, _box_api):
        with mock.patch.dict(
            "os.environ",
            {"BOX_API_KEY": "env-key", "BOX_BASE_URL": "https://custom.example/api/box/v1"},
            clear=False,
        ):
            AsciiBoxSandboxBackend(box_conn_id=None)._get_api()

        configuration.assert_called_once_with(
            host="https://custom.example/api/box/v1", access_token="env-key"
        )

    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_missing_api_key_is_terminal(self, hook):
        hook.get_connection.return_value = _connection(password="")

        with pytest.raises(SandboxTerminalError, match="has no password"):
            AsciiBoxSandboxBackend()._get_api()

    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_invalid_connection_extra_is_terminal(self, hook):
        hook.get_connection.return_value = _connection(extra={"timeout": "never"})

        with pytest.raises(SandboxTerminalError, match="timeout must be a positive finite number"):
            AsciiBoxSandboxBackend()._get_api()


class TestCreate:
    def test_refuses_a_per_domain_egress_allowlist(self):
        backend, _ = _backend_with_api()

        with pytest.raises(SandboxTerminalError, match="per-domain egress allowlist"):
            backend.create(spec=SandboxSpec(allow_egress_to=["example.com"]))

    def test_refuses_block_network(self):
        backend, _ = _backend_with_api()

        with pytest.raises(SandboxTerminalError, match="cannot deny outbound network access"):
            backend.create(spec=SandboxSpec(block_network=True))

    @mock.patch("ascii_box_sdk.wait_until_ready", autospec=True)
    def test_spec_and_sizing_are_passed_at_creation(self, wait_ready):
        backend, api = _backend_with_api(machine_type="small", ttl_seconds=120, ready_timeout=45, no_env=True)
        api.create.return_value = SimpleNamespace(box=SimpleNamespace(id="bx_created1"))

        box_id = backend.create(spec=SandboxSpec(block_network=False, env={"TOKEN": "value"}))

        assert box_id == "bx_created1"
        request = api.create.call_args.args[0]
        assert request.type == "small"
        assert request.ttl_seconds == 120
        assert request.no_env is True
        assert request.env == {"TOKEN": "value"}
        wait_ready.assert_called_once()
        assert api.update.called

    @mock.patch("ascii_box_sdk.wait_until_ready", autospec=True)
    def test_none_spec_is_allowed(self, _wait_ready):
        backend, api = _backend_with_api()
        api.create.return_value = SimpleNamespace(box=SimpleNamespace(id="bx_created1"))

        backend.create()

        request = api.create.call_args.args[0]
        assert request.env is None

    def test_api_failure_is_terminal(self):
        backend, api = _backend_with_api()
        api.create.side_effect = _api_error(503)

        with pytest.raises(SandboxTerminalError, match="HTTP 503"):
            backend.create(spec=SandboxSpec(block_network=False))


class TestRunCommand:
    def test_forwards_command_and_bounds_output(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_result(
            stdout="0" * 20, stderr="err", stdout_truncated=True, stderr_truncated=False
        )

        result = backend.run_command("bx_1", "echo hi", timeout=5, max_output_bytes=8)

        request = api.command.call_args.args[1]
        assert request.command == "echo hi"
        assert request.timeout_seconds == 5
        assert result.stdout == "0" * 8
        assert result.stdout_truncated
        assert result.stderr == "err"
        assert result.exit_code == 0

    def test_rejects_timeout_above_api_cap(self):
        backend, _ = _backend_with_api()

        with pytest.raises(SandboxTerminalError, match="capped at 600"):
            backend.run_command("bx_1", "sleep 1", timeout=601, max_output_bytes=1024)

    def test_timeout_destroys_sandbox(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_result(timed_out=True, stdout="partial")
        response = mock.MagicMock()
        response.status = 202
        response.read = mock.MagicMock()
        api.api_client.param_serialize.return_value = ("DELETE", "https://example/boxes/bx_1", {}, None, None)
        api.api_client.call_api.return_value = response

        result = backend.run_command("bx_1", "sleep 99", timeout=1, max_output_bytes=1024)

        assert result.timed_out
        assert result.sandbox_terminated
        assert api.api_client.call_api.called


class TestFiles:
    def test_read_file_decodes_base64_and_enforces_budget(self):
        backend, api = _backend_with_api()
        payload = b"hello-world"
        api.read_file.return_value = SimpleNamespace(
            content=base64.b64encode(payload).decode(), size=len(payload)
        )

        assert backend.read_file("bx_1", "/tmp/a.txt", max_bytes=64) == payload

        api.read_file.return_value = SimpleNamespace(
            content=base64.b64encode(payload).decode(), size=len(payload)
        )
        with pytest.raises(SandboxFileTooLargeError):
            backend.read_file("bx_1", "/tmp/a.txt", max_bytes=4)

    def test_missing_file_is_recoverable(self):
        backend, api = _backend_with_api()
        api.read_file.side_effect = _api_error(404)
        api.get.return_value = SimpleNamespace(box=SimpleNamespace(state="ready"))

        with pytest.raises(SandboxError, match="does not exist"):
            backend.read_file("bx_1", "/tmp/missing", max_bytes=10)

    def test_write_file_uses_base64_and_creates_parents(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_result()

        backend.write_file("bx_1", "/tmp/dir/file.bin", b"\x00\x01")

        mkdir_request = api.command.call_args.args[1]
        assert "mkdir -p" in mkdir_request.command
        write_request = api.write_file.call_args.args[1]
        assert write_request.path == "/tmp/dir/file.bin"
        assert write_request.encoding == "base64"
        assert base64.b64decode(write_request.content) == b"\x00\x01"


class TestDestroy:
    def test_delete_is_idempotent_for_missing_boxes(self):
        backend, api = _backend_with_api()
        api.api_client.param_serialize.return_value = ("DELETE", "https://example/boxes/bx_1", {}, None, None)
        api.api_client.call_api.side_effect = _api_error(404)

        backend.destroy("bx_1")

        assert api.api_client.call_api.called

    def test_delete_sends_confirm_header(self):
        backend, api = _backend_with_api()
        response = mock.MagicMock()
        response.status = 202
        response.read = mock.MagicMock()
        api.api_client.param_serialize.return_value = ("DELETE", "https://example/boxes/bx_1", {}, None, None)
        api.api_client.call_api.return_value = response

        backend.destroy("bx_gone01")

        kwargs = api.api_client.param_serialize.call_args.kwargs
        assert kwargs["method"] == "DELETE"
        assert kwargs["path_params"] == {"boxId": "bx_gone01"}
        assert kwargs["header_params"]["X-Ascii-Confirm-Delete"] == "bx_gone01"
