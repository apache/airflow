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

from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("islo")

from islo.core.api_error import ApiError
from islo.errors import NotFoundError

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.islo import IsloSandboxBackend

_BASE_HOOK_PATH = "airflow.providers.common.ai.sandbox.islo.BaseHook"
_ISLO_PATH = "islo.Islo"


def _connection(password="secret-key", host=None, extra=None):
    return SimpleNamespace(password=password, host=host, extra_dejson=extra or {})


def _exec_result(status="completed", exit_code=0, stdout="0\n", stderr="0\n", truncated=False):
    return SimpleNamespace(
        status=status, exit_code=exit_code, stdout=stdout, stderr=stderr, truncated=truncated
    )


def _backend_with_client(**kwargs) -> tuple[IsloSandboxBackend, mock.MagicMock]:
    backend = IsloSandboxBackend(**kwargs)
    client = mock.MagicMock(spec=["sandboxes"])
    client.sandboxes = mock.MagicMock(
        spec=[
            "create_sandbox",
            "delete_sandbox",
            "download_file",
            "exec_in_sandbox",
            "get_exec_result",
            "get_sandbox",
            "upload_file",
        ]
    )
    client.sandboxes.exec_in_sandbox.return_value = SimpleNamespace(exec_id="exec-1")
    client.sandboxes.create_sandbox.return_value = SimpleNamespace(name="box-1")
    client.sandboxes.get_exec_result.return_value = _exec_result()
    backend._client = client
    return backend, client


class TestCredentials:
    @mock.patch(_ISLO_PATH, autospec=True)
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_api_key_and_allowlisted_connection_options_are_forwarded(self, hook, islo):
        backend = IsloSandboxBackend(islo_conn_id="my_islo")
        hook.get_connection.return_value = _connection(
            password=" key ",
            host="https://compute",
            extra={"base_url": "https://api", "timeout": 12},
        )

        backend._get_client()

        hook.get_connection.assert_called_once_with("my_islo")
        islo.assert_called_once_with(
            api_key="key", compute_url="https://compute", base_url="https://api", timeout=12.0
        )

    @mock.patch(_ISLO_PATH, autospec=True)
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_client_is_resolved_once_and_cached(self, hook, _islo):
        backend = IsloSandboxBackend()
        hook.get_connection.return_value = _connection()

        backend._get_client()
        backend._get_client()

        hook.get_connection.assert_called_once_with("islo_default")

    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_missing_api_key_is_terminal(self, hook):
        backend = IsloSandboxBackend()
        hook.get_connection.return_value = _connection(password="")

        with pytest.raises(SandboxTerminalError, match="has no password"):
            backend._get_client()

    @mock.patch(_ISLO_PATH, autospec=True)
    def test_none_conn_id_defers_to_the_sdk_environment(self, islo):
        backend = IsloSandboxBackend(islo_conn_id=None)

        backend._get_client()

        islo.assert_called_once_with()

    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_connection_resolution_failure_is_terminal(self, hook):
        backend = IsloSandboxBackend()
        hook.get_connection.side_effect = RuntimeError("secret backend down")

        with pytest.raises(SandboxTerminalError, match="initialize its client"):
            backend._get_client()

    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_invalid_connection_timeout_is_terminal_and_actionable(self, hook):
        backend = IsloSandboxBackend()
        hook.get_connection.return_value = _connection(extra={"timeout": "never"})

        with pytest.raises(SandboxTerminalError, match="timeout must be a positive finite number"):
            backend._get_client()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"image": ""}, "image"),
        ({"vcpus": 0}, "vcpus"),
        ({"memory_mb": 0}, "memory_mb"),
        ({"delete_after": 0}, "delete_after"),
    ],
)
def test_constructor_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        IsloSandboxBackend(**kwargs)


class TestCreate:
    def test_refuses_a_per_domain_egress_allowlist(self):
        backend, _ = _backend_with_client()

        with pytest.raises(SandboxTerminalError, match="per-domain egress allowlist"):
            backend.create(spec=SandboxSpec(allow_egress_to=["example.com"]))

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            (None, False),
            (SandboxSpec(), False),
            (SandboxSpec(block_network=True), False),
            (SandboxSpec(block_network=False), True),
        ],
    )
    def test_block_network_maps_to_internet_enabled(self, spec, expected):
        backend, client = _backend_with_client()

        backend.create(spec=spec)

        assert client.sandboxes.create_sandbox.call_args.kwargs["internet_enabled"] is expected

    def test_spec_and_sizing_are_passed_at_creation(self):
        backend, client = _backend_with_client(image="python", vcpus=2, memory_mb=1024, delete_after=120)

        name = backend.create(spec=SandboxSpec(env={"TOKEN": "value"}))

        assert name == "box-1"
        kwargs = client.sandboxes.create_sandbox.call_args.kwargs
        assert kwargs["image"] == "python"
        assert kwargs["vcpus"] == 2
        assert kwargs["memory_mb"] == 1024
        assert kwargs["env"] == {"TOKEN": "value"}
        assert kwargs["lifecycle"].delete_after == 120
        assert kwargs["request_options"] == {"timeout_in_seconds": 120, "max_retries": 0}

    def test_omitted_sizing_is_left_to_the_server(self):
        backend, client = _backend_with_client()

        backend.create()

        assert not {"image", "vcpus", "memory_mb"} & client.sandboxes.create_sandbox.call_args.kwargs.keys()

    def test_api_failure_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.create_sandbox.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError, match="HTTP 503"):
            backend.create()


class TestRunCommand:
    def test_polls_with_backoff(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.side_effect = [
            _exec_result(status="running"),
            _exec_result(status="running"),
            _exec_result(status="running"),
            _exec_result(stdout="0\ndone"),
        ]

        with mock.patch("time.sleep", autospec=True) as sleep:
            result = backend.run_command("box", "x", timeout=60, max_output_bytes=1024)

        intervals = [call.args[0] for call in sleep.call_args_list]
        assert result.stdout == "done"
        assert intervals == sorted(intervals)
        assert intervals[-1] > intervals[0]

    def test_user_command_is_an_argument_to_the_bounding_wrapper(self):
        backend, client = _backend_with_client()
        user_command = "echo '$HOME'; rm -f /tmp/nope"

        backend.run_command("box", user_command, timeout=5, max_output_bytes=1024)

        command = client.sandboxes.exec_in_sandbox.call_args.kwargs["command"]
        assert command[:2] == ["sh", "-c"]
        assert user_command not in command[2]
        assert command[4:] == [user_command, "1024"]

    def test_keeps_the_bounded_tail_and_truncation_flags(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(
            stdout="1\nstdout-tail", stderr="1\nstderr-tail"
        )

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert result.stdout == "stdout-tail"
        assert result.stderr == "stderr-tail"
        assert result.stdout_truncated
        assert result.stderr_truncated

    def test_applies_a_utf8_byte_cap_as_a_second_defence(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="0\nééé")

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=4)

        assert result.stdout == "éé"
        assert result.stdout_truncated

    def test_a_missing_wrapper_header_is_treated_as_truncated(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="abcdef")

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=3)

        assert result.stdout == "def"
        assert result.stdout_truncated

    def test_server_truncation_is_reported_for_both_streams(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(truncated=True)

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert result.stdout_truncated
        assert result.stderr_truncated

    @mock.patch.object(IsloSandboxBackend, "_await_exec", autospec=True, return_value=None)
    def test_poll_deadline_destroys_the_sandbox(self, _await_exec):
        backend, client = _backend_with_client()

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert result.timed_out
        assert result.sandbox_terminated
        client.sandboxes.delete_sandbox.assert_called_once()

    @mock.patch.object(IsloSandboxBackend, "_await_exec", autospec=True, return_value=None)
    def test_timeout_cleanup_failure_is_terminal(self, _await_exec):
        backend, client = _backend_with_client()
        client.sandboxes.delete_sandbox.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError, match="deletion.*could not be confirmed"):
            backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

    def test_server_timeout_also_destroys_the_sandbox(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(status="timeout", exit_code=None)

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert result.timed_out
        assert result.exit_code == -1
        assert result.sandbox_terminated
        client.sandboxes.delete_sandbox.assert_called_once()

    def test_start_failure_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.exec_in_sandbox.side_effect = ApiError(status_code=401)

        with pytest.raises(SandboxTerminalError, match="HTTP 401"):
            backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

    def test_poll_failure_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.side_effect = RuntimeError("transport down")

        with pytest.raises(SandboxTerminalError, match="poll a sandbox command"):
            backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

    @pytest.mark.parametrize(
        ("timeout", "max_bytes", "message"), [(0, 1, "timeout"), (1, 0, "max_output_bytes")]
    )
    def test_rejects_invalid_budgets(self, timeout, max_bytes, message):
        backend, _ = _backend_with_client()

        with pytest.raises(ValueError, match=message):
            backend.run_command("box", "x", timeout=timeout, max_output_bytes=max_bytes)


class TestFileOperations:
    def test_read_file_uses_the_native_streaming_api(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.return_value = iter([b"he", b"llo"])

        data = backend.read_file("box", "/w/a", max_bytes=100)

        assert data == b"hello"
        client.sandboxes.download_file.assert_called_once_with(
            "box",
            path="/w/a",
            request_options={"timeout_in_seconds": 120, "max_retries": 0, "chunk_size": 101},
        )
        client.sandboxes.exec_in_sandbox.assert_not_called()

    def test_oversized_read_stops_and_closes_the_stream(self):
        backend, client = _backend_with_client()
        closed: list[bool] = []

        def chunks():
            try:
                yield b"x" * 11
                raise AssertionError("the backend must stop after the sentinel byte")
            finally:
                closed.append(True)

        client.sandboxes.download_file.return_value = chunks()

        with pytest.raises(SandboxFileTooLargeError):
            backend.read_file("box", "/w/a", max_bytes=10)

        assert closed == [True]

    def test_missing_file_is_recoverable_when_the_sandbox_exists(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = NotFoundError({})

        with pytest.raises(SandboxError, match="does not exist") as error:
            backend.read_file("box", "/w/missing", max_bytes=100)

        assert not isinstance(error.value, SandboxTerminalError)
        client.sandboxes.get_sandbox.assert_called_once()

    def test_missing_sandbox_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = NotFoundError({})
        client.sandboxes.get_sandbox.side_effect = NotFoundError({})

        with pytest.raises(SandboxTerminalError, match="check a sandbox"):
            backend.read_file("box", "/w/a", max_bytes=100)

    def test_download_failure_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError, match="download a sandbox file"):
            backend.read_file("box", "/w/a", max_bytes=100)

    def test_write_file_creates_parents_then_uses_native_upload(self):
        backend, client = _backend_with_client()

        backend.write_file("box", "/w/sub/a", b"data")

        command = client.sandboxes.exec_in_sandbox.call_args.kwargs["command"]
        assert "mkdir -p" in command[4]
        client.sandboxes.upload_file.assert_called_once_with(
            "box",
            path="/w/sub/a",
            file=("upload", b"data", "application/octet-stream"),
            request_options={"timeout_in_seconds": 120, "max_retries": 0},
        )

    def test_write_stops_when_parent_creation_fails(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(exit_code=1, stderr="0\nread-only")

        with pytest.raises(SandboxError, match="read-only"):
            backend.write_file("box", "/w/a", b"data")

        client.sandboxes.upload_file.assert_not_called()

    def test_upload_failure_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.upload_file.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError, match="upload a sandbox file"):
            backend.write_file("box", "/w/a", b"data")

    def test_list_directory_marks_directories_and_preserves_newlines(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="0\nf a.txt\0d new\nline\0")

        assert backend.list_directory("box", "/w") == [("a.txt", False), ("new\nline", True)]

    def test_list_failure_is_recoverable(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(exit_code=1, stderr="0\nmissing")

        with pytest.raises(SandboxError, match="missing"):
            backend.list_directory("box", "/w")

    def test_helper_timeout_is_terminal(self):
        backend, _ = _backend_with_client()
        backend.run_command = mock.create_autospec(
            backend.run_command,
            return_value=SandboxExecResult(
                exit_code=-1,
                stdout="",
                stderr="",
                timed_out=True,
                sandbox_terminated=True,
            ),
        )

        with pytest.raises(SandboxTerminalError, match="destroyed"):
            backend.list_directory("box", "/w")

    def test_read_rejects_an_invalid_budget(self):
        backend, _ = _backend_with_client()

        with pytest.raises(ValueError, match="max_bytes"):
            backend.read_file("box", "/w/a", max_bytes=0)


class TestDestroy:
    def test_is_idempotent_when_the_sandbox_is_already_gone(self):
        backend, client = _backend_with_client()
        client.sandboxes.delete_sandbox.side_effect = NotFoundError({})

        backend.destroy("box")

    def test_delete_failure_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.delete_sandbox.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError, match="delete a sandbox"):
            backend.destroy("box")
