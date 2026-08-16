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

import builtins
from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("opensandbox")

from opensandbox.exceptions import SandboxApiException

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.opensandbox import OpenSandboxBackend

_BASE_HOOK_PATH = "airflow.providers.common.ai.sandbox.opensandbox.BaseHook"


def _api_error(status_code: int) -> SandboxApiException:
    return SandboxApiException(status_code=status_code)


def _connection(
    *,
    password: str | None = "secret",
    host: str | None = "sandbox.example",
    port: int | None = 443,
    schema: str | None = "https",
    extra: dict | None = None,
):
    return SimpleNamespace(
        password=password,
        host=host,
        port=port,
        schema=schema,
        extra_dejson=extra or {},
    )


def _execution(*, exit_code=0, error=None):
    return SimpleNamespace(exit_code=exit_code, error=error)


def _backend_with_sandbox(**kwargs) -> tuple[OpenSandboxBackend, mock.MagicMock]:
    backend = OpenSandboxBackend(**kwargs)
    backend._connection_config = mock.sentinel.connection_config
    sandbox = mock.MagicMock(spec=["id", "commands", "files", "get_info", "destroy"])
    sandbox.id = "box-1"
    sandbox.commands = mock.MagicMock(spec=["run"])
    sandbox.files = mock.MagicMock(
        spec=["read_bytes_stream", "create_directories", "write_file", "list_directory"]
    )
    backend._sandboxes[sandbox.id] = sandbox
    return backend, sandbox


def test_missing_sdk_error_is_actionable():
    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name.startswith("opensandbox"):
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    backend = OpenSandboxBackend(opensandbox_conn_id=None)
    with mock.patch("builtins.__import__", side_effect=blocked_import):
        with pytest.raises(SandboxTerminalError, match="sandbox-opensandbox"):
            backend.create()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"image": ""}, "image"),
        ({"cpu": ""}, "cpu"),
        ({"memory": ""}, "memory"),
        ({"sandbox_timeout": 0}, "sandbox_timeout"),
        ({"ready_timeout": 0}, "ready_timeout"),
    ],
)
def test_constructor_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        OpenSandboxBackend(**kwargs)


class TestConnection:
    @mock.patch("opensandbox.config.ConnectionConfigSync", autospec=True)
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_airflow_connection_fields_and_allowlisted_extras_are_forwarded(self, hook, config):
        hook.get_connection.return_value = _connection(
            extra={"request_timeout": "12.5", "use_server_proxy": "false", "ignored": "value"}
        )
        backend = OpenSandboxBackend(opensandbox_conn_id="my_opensandbox")

        backend._get_connection_config()

        hook.get_connection.assert_called_once_with("my_opensandbox")
        kwargs = config.call_args.kwargs
        assert kwargs["api_key"] == "secret"
        assert kwargs["domain"] == "sandbox.example:443"
        assert kwargs["protocol"] == "https"
        assert kwargs["request_timeout"].total_seconds() == 12.5
        assert kwargs["use_server_proxy"] is False
        assert "ignored" not in kwargs

    @mock.patch("opensandbox.config.ConnectionConfigSync", autospec=True)
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_connection_is_resolved_once_and_cached(self, hook, _config):
        hook.get_connection.return_value = _connection()
        backend = OpenSandboxBackend()

        backend._get_connection_config()
        backend._get_connection_config()

        hook.get_connection.assert_called_once_with("opensandbox_default")

    @mock.patch("opensandbox.config.ConnectionConfigSync", autospec=True)
    def test_none_connection_id_defers_to_sdk_environment(self, config):
        OpenSandboxBackend(opensandbox_conn_id=None)._get_connection_config()

        config.assert_called_once_with(use_server_proxy=True)

    @pytest.mark.parametrize(
        ("extra", "message"),
        [
            ({"request_timeout": "never"}, "request_timeout"),
            ({"use_server_proxy": "sometimes"}, "use_server_proxy"),
        ],
    )
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_invalid_connection_extra_is_terminal(self, hook, extra, message):
        hook.get_connection.return_value = _connection(extra=extra)

        with pytest.raises(SandboxTerminalError, match=message):
            OpenSandboxBackend()._get_connection_config()


class TestCreate:
    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_spec_resources_and_timeouts_are_forwarded(self, create):
        create.return_value = SimpleNamespace(id="created")
        backend = OpenSandboxBackend(
            image="python:3.13-slim",
            cpu="2",
            memory="4Gi",
            sandbox_timeout=300,
            ready_timeout=45,
        )
        backend._connection_config = mock.sentinel.connection_config

        sandbox_id = backend.create(spec=SandboxSpec(env={"TOKEN": "value"}, allow_egress_to=["pypi.org"]))

        assert sandbox_id == "created"
        kwargs = create.call_args.kwargs
        assert create.call_args.args == ("python:3.13-slim",)
        assert kwargs["env"] == {"TOKEN": "value"}
        assert kwargs["resource"] == {"cpu": "2", "memory": "4Gi"}
        assert kwargs["timeout"].total_seconds() == 300
        assert kwargs["ready_timeout"].total_seconds() == 45
        assert kwargs["connection_config"] is mock.sentinel.connection_config
        assert kwargs["network_policy"].default_action == "deny"
        assert [(rule.action, rule.target) for rule in kwargs["network_policy"].egress] == [
            ("allow", "pypi.org")
        ]

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            (SandboxSpec(), "deny"),
            (SandboxSpec(block_network=False), "allow"),
        ],
    )
    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_network_default_is_mapped(self, create, spec, expected):
        create.return_value = SimpleNamespace(id="created")
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.create(spec=spec)

        assert create.call_args.kwargs["network_policy"].default_action == expected

    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_none_spec_states_no_network_requirement(self, create):
        create.return_value = SimpleNamespace(id="created")
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.create()

        assert create.call_args.kwargs["network_policy"] is None

    def test_open_network_with_allowlist_is_refused(self):
        backend = OpenSandboxBackend()

        with pytest.raises(SandboxTerminalError, match="block_network=True"):
            backend.create(spec=SandboxSpec(block_network=False, allow_egress_to=["example.com"]))

    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_api_failure_is_terminal(self, create):
        create.side_effect = _api_error(503)
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        with pytest.raises(SandboxTerminalError, match="HTTP 503"):
            backend.create(spec=SandboxSpec())


class TestRunCommand:
    def test_streams_output_into_byte_bounded_tails(self):
        backend, sandbox = _backend_with_sandbox()

        def run(_command, *, opts, handlers):
            assert opts.timeout.total_seconds() == 9
            assert handlers.skip_accumulation
            handlers.on_stdout(SimpleNamespace(text="prefix-"))
            handlers.on_stdout(SimpleNamespace(text="ééé"))
            handlers.on_stderr(SimpleNamespace(text="stderr-tail"))
            return _execution(exit_code=3)

        sandbox.commands.run.side_effect = run

        result = backend.run_command("box-1", "echo hi", timeout=9, max_output_bytes=6)

        assert result.exit_code == 3
        assert result.stdout == "ééé"
        assert result.stderr == "r-tail"
        assert result.stdout_truncated
        assert result.stderr_truncated

    def test_execution_error_is_returned_on_stderr(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution(
            exit_code=1,
            error=SimpleNamespace(traceback=["line one", "line two"], value="failed"),
        )

        result = backend.run_command("box-1", "bad", timeout=9, max_output_bytes=100)

        assert result.stderr == "line one\nline two"

    @mock.patch("airflow.providers.common.ai.sandbox.opensandbox.time.monotonic", side_effect=[0.0, 5.0])
    def test_server_kill_at_deadline_is_a_timeout(self, _monotonic):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution(exit_code=-9)

        result = backend.run_command("box-1", "sleep 60", timeout=5, max_output_bytes=100)

        assert result.timed_out

    def test_missing_terminal_status_is_terminal(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution(exit_code=None)

        with pytest.raises(SandboxTerminalError, match="no terminal status"):
            backend.run_command("box-1", "echo hi", timeout=5, max_output_bytes=100)

    def test_api_failure_is_terminal(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.side_effect = _api_error(401)

        with pytest.raises(SandboxTerminalError, match="HTTP 401"):
            backend.run_command("box-1", "echo hi", timeout=5, max_output_bytes=100)

    @pytest.mark.parametrize(
        ("timeout", "max_bytes", "message"),
        [(0, 1, "timeout"), (1, 0, "max_output_bytes")],
    )
    def test_rejects_invalid_budgets(self, timeout, max_bytes, message):
        backend, _ = _backend_with_sandbox()

        with pytest.raises(ValueError, match=message):
            backend.run_command("box-1", "echo hi", timeout=timeout, max_output_bytes=max_bytes)


class TestFileOperations:
    def test_read_uses_range_and_native_streaming(self):
        backend, sandbox = _backend_with_sandbox()
        stream = mock.MagicMock(spec=["__iter__", "close"])
        stream.__iter__.return_value = iter([b"he", b"llo"])
        sandbox.files.read_bytes_stream.return_value = stream

        assert backend.read_file("box-1", "/w/a", max_bytes=10) == b"hello"
        sandbox.files.read_bytes_stream.assert_called_once_with(
            "/w/a", chunk_size=11, range_header="bytes=0-10"
        )
        stream.close.assert_called_once()

    def test_oversized_read_stops_at_the_sentinel_byte(self):
        backend, sandbox = _backend_with_sandbox()
        stream = mock.MagicMock(spec=["__iter__", "close"])
        stream.__iter__.return_value = iter([b"x" * 11, b"must-not-be-read"])
        sandbox.files.read_bytes_stream.return_value = stream

        with pytest.raises(SandboxFileTooLargeError) as error:
            backend.read_file("box-1", "/w/a", max_bytes=10)

        assert error.value.size_bytes == 11
        stream.close.assert_called_once()

    def test_missing_file_is_recoverable_when_sandbox_exists(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.files.read_bytes_stream.side_effect = _api_error(404)

        with pytest.raises(SandboxError, match="does not exist") as error:
            backend.read_file("box-1", "/w/missing", max_bytes=10)

        assert not isinstance(error.value, SandboxTerminalError)
        sandbox.get_info.assert_called_once()

    def test_missing_sandbox_is_terminal(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.files.read_bytes_stream.side_effect = _api_error(404)
        sandbox.get_info.side_effect = _api_error(404)

        with pytest.raises(SandboxTerminalError, match="confirm that a sandbox still exists"):
            backend.read_file("box-1", "/w/a", max_bytes=10)

    def test_bad_read_request_is_recoverable(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.files.read_bytes_stream.side_effect = _api_error(400)

        with pytest.raises(SandboxError) as error:
            backend.read_file("box-1", "bad", max_bytes=10)

        assert not isinstance(error.value, SandboxTerminalError)

    def test_write_creates_parent_and_uses_native_file_api(self):
        backend, sandbox = _backend_with_sandbox()

        backend.write_file("box-1", "/w/sub/a", b"data")

        entry = sandbox.files.create_directories.call_args.args[0][0]
        assert (entry.path, entry.mode) == ("/w/sub", 755)
        sandbox.files.write_file.assert_called_once_with("/w/sub/a", b"data", mode=644)

    def test_missing_write_path_is_recoverable_when_sandbox_exists(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.files.write_file.side_effect = _api_error(404)

        with pytest.raises(SandboxError, match="Could not write") as error:
            backend.write_file("box-1", "a", b"data")

        assert not isinstance(error.value, SandboxTerminalError)
        sandbox.get_info.assert_called_once()

    def test_list_returns_direct_children_and_marks_directories(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.files.list_directory.return_value = [
            SimpleNamespace(path="/w/a.txt", entry_type="file"),
            SimpleNamespace(path="/w/sub/", entry_type="directory"),
        ]

        assert backend.list_directory("box-1", "/w") == [("a.txt", False), ("sub", True)]
        entry = sandbox.files.list_directory.call_args.args[0]
        assert (entry.path, entry.depth) == ("/w", 1)

    def test_missing_directory_is_recoverable_when_sandbox_exists(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.files.list_directory.side_effect = _api_error(404)

        with pytest.raises(SandboxError, match="does not exist") as error:
            backend.list_directory("box-1", "/missing")

        assert not isinstance(error.value, SandboxTerminalError)

    def test_read_rejects_invalid_budget(self):
        backend, _ = _backend_with_sandbox()

        with pytest.raises(ValueError, match="max_bytes"):
            backend.read_file("box-1", "/w/a", max_bytes=0)


class TestDestroy:
    def test_cached_sandbox_is_destroyed_and_evicted(self):
        backend, sandbox = _backend_with_sandbox()

        backend.destroy("box-1")

        sandbox.destroy.assert_called_once()
        assert "box-1" not in backend._sandboxes

    @mock.patch("opensandbox.SandboxSync.connect", autospec=True)
    def test_uncached_sandbox_is_reconnected_before_destroy(self, connect):
        remote = mock.MagicMock(spec=["destroy"])
        connect.return_value = remote
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.destroy("remote")

        remote.destroy.assert_called_once()

    @mock.patch("opensandbox.SandboxSync.connect", autospec=True)
    def test_already_gone_sandbox_is_idempotent(self, connect):
        connect.side_effect = _api_error(404)
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.destroy("gone")

    def test_destroy_failure_is_terminal(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.destroy.side_effect = _api_error(503)

        with pytest.raises(SandboxTerminalError, match="HTTP 503"):
            backend.destroy("box-1")
