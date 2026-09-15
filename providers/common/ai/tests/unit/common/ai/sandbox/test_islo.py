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

import os
import signal
import subprocess
import time
from pathlib import Path
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
from airflow.providers.common.ai.sandbox.islo import (
    _COMMAND_WRAPPER,
    IsloSandboxBackend,
    _bound_result_stream,
)

_MODULE = "airflow.providers.common.ai.sandbox.islo"
_BASE_HOOK_PATH = f"{_MODULE}.BaseHook"
_ISLO_PATH = "islo.Islo"


def _connection(password="secret-key", host=None, extra=None):
    return SimpleNamespace(password=password, host=host, extra_dejson=extra or {})


def _exec_result(status="completed", exit_code=0, stdout="", stderr="", truncated=False):
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

    def test_a_failed_create_deletes_the_name_it_had_already_bound(self):
        backend, client = _backend_with_client()
        client.sandboxes.create_sandbox.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError):
            backend.create()

        # The server may have provisioned the microVM before failing to answer,
        # and this name is the only handle that can still reclaim it.
        requested = client.sandboxes.create_sandbox.call_args.kwargs["name"]
        client.sandboxes.delete_sandbox.assert_called_once()
        assert client.sandboxes.delete_sandbox.call_args.kwargs["sandbox_name"] == requested

    def test_a_cleanup_failure_does_not_mask_the_original_create_error(self):
        backend, client = _backend_with_client()
        client.sandboxes.create_sandbox.side_effect = ApiError(status_code=503)
        client.sandboxes.delete_sandbox.side_effect = ApiError(status_code=500)

        with pytest.raises(SandboxTerminalError, match="HTTP 503"):
            backend.create()

    def test_spec_env_is_forwarded_so_it_is_never_silently_dropped(self):
        backend, client = _backend_with_client()

        backend.create(spec=SandboxSpec(env={"TOKEN": "value", "OTHER": "2"}))

        # base.py treats dropping a SandboxSpec field as a contract violation.
        assert client.sandboxes.create_sandbox.call_args.kwargs["env"] == {
            "TOKEN": "value",
            "OTHER": "2",
        }


class TestRunCommand:
    def test_polls_with_backoff(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.side_effect = [
            _exec_result(status="running"),
            _exec_result(status="running"),
            _exec_result(status="running"),
            _exec_result(stdout="done"),
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
        # One byte over the budget, so an over-budget stream proves truncation.
        assert command[4:] == [user_command, "1025"]

    def test_a_stream_within_budget_is_passed_through_untouched(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="a\nb\n", stderr="err\n")

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert result.stdout == "a\nb\n"
        assert result.stderr == "err\n"
        assert not result.stdout_truncated
        assert not result.stderr_truncated

    def test_an_over_budget_stream_keeps_the_tail_and_reports_truncation(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(
            stdout="line1\nline2\nline3\n", stderr="e1\ne2\ne3\n"
        )

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=8)

        assert result.stdout == "line3\n"
        assert result.stderr == "e2\ne3\n"
        assert result.stdout_truncated
        assert result.stderr_truncated

    def test_truncation_never_emits_a_partial_leading_line(self):
        backend, client = _backend_with_client()
        # A byte-aligned cut of the last 4 bytes would land inside "line988".
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="line988\nline989\n")

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=12)

        assert result.stdout == "line989\n"
        assert result.stdout_truncated

    def test_a_single_line_over_budget_is_cut_rather_than_dropped(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="abcdef")

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=3)

        assert result.stdout == "def"
        assert result.stdout_truncated

    def test_applies_the_byte_cap_on_utf8_boundaries(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="ééé")

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=4)

        assert result.stdout == "éé"
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

    @mock.patch(f"{_MODULE}.log", autospec=True)
    @mock.patch.object(IsloSandboxBackend, "_await_exec", autospec=True, return_value=None)
    def test_timeout_cleanup_failure_warns_and_leaves_the_ttl_to_reclaim(self, _await_exec, logger):
        backend, client = _backend_with_client()
        client.sandboxes.delete_sandbox.side_effect = ApiError(status_code=503)

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        # A command that merely ran long must not fail the task because one
        # cleanup call was refused; delete_after reclaims the microVM anyway.
        assert result.timed_out
        assert result.sandbox_terminated
        assert "could not confirm its deletion" in logger.warning.call_args.args[0]

    @pytest.mark.parametrize("status", ["cancelled", "dead", "something-new"])
    def test_an_unrecognised_status_is_terminal_rather_than_still_running(self, status):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(status=status, exit_code=None)

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        # The vendor types status as a plain string, so the vocabulary can grow.
        # Reading an unknown value as still-running would poll to the deadline
        # and then destroy the sandbox, costing the agent its files.
        assert result.exit_code == -1
        assert not result.timed_out
        assert not result.sandbox_terminated
        client.sandboxes.delete_sandbox.assert_not_called()

    @pytest.mark.parametrize("status", ["pending", "queued", "starting", "running"])
    def test_in_flight_statuses_keep_polling(self, status):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.side_effect = [
            _exec_result(status=status),
            _exec_result(stdout="eventually\n"),
        ]

        with mock.patch("time.sleep", autospec=True):
            result = backend.run_command("box", "x", timeout=60, max_output_bytes=1024)

        assert result.stdout == "eventually\n"

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

    def test_delete_keeps_the_sdk_retries(self):
        backend, client = _backend_with_client()

        backend.destroy("box")

        # Deletion is idempotent and is the only call whose failure strands a
        # microVM, so it must not be the one call that never retries.
        options = client.sandboxes.delete_sandbox.call_args.kwargs["request_options"]
        assert options["max_retries"] > 0


def _run_wrapper(command: str, max_output_bytes: int, *, timeout: float = 30.0):
    """Run the real wrapper through a local ``sh``, exactly as the backend invokes it."""
    return subprocess.run(
        [
            "sh",
            "-c",
            _COMMAND_WRAPPER,
            "airflow-sandbox",
            command,
            str(max_output_bytes + 1),
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


class TestCommandWrapper:
    """
    Execute ``_COMMAND_WRAPPER`` for real, rather than asserting on its text.

    Every other test in this module mocks ``exec_in_sandbox``, so without these
    the wrapper only ever runs in the system test, which needs a live API key and
    so does not run in ordinary CI. A local ``sh`` needs no Islo access at all.
    """

    def test_reports_stdout_stderr_and_the_exit_status_separately(self):
        result = _run_wrapper("echo out; echo err >&2; exit 3", 1024)

        assert result.stdout == "out\n"
        assert result.stderr == "err\n"
        assert result.returncode == 3

    def test_a_backgrounded_process_does_not_hold_the_command_open(self):
        # The command's foreground part finishes at once. Waiting for the capture
        # to reach end-of-input would block until the backgrounded child exits,
        # and past the deadline the backend destroys the sandbox -- so the agent
        # would lose its files over a command that already finished.
        start = time.monotonic()
        result = _run_wrapper("sleep 20 & echo started", 1024)
        elapsed = time.monotonic() - start

        assert result.stdout == "started\n"
        assert result.returncode == 0
        assert elapsed < 5.0

    def test_a_long_lived_daemon_does_not_hold_the_command_open(self):
        start = time.monotonic()
        result = _run_wrapper("nohup sleep 300 & echo server-started", 1024)
        elapsed = time.monotonic() - start

        assert result.stdout == "server-started\n"
        assert elapsed < 5.0

    def test_does_not_change_the_permissions_of_what_the_agent_creates(self, tmp_path):
        result = _run_wrapper(f"cd {tmp_path} && touch a_file && mkdir a_dir && ls -ld a_dir a_file", 4096)

        # A umask left in force for the agent's command would make these 700/600.
        assert result.returncode == 0
        modes = [line.split()[0] for line in result.stdout.strip().splitlines()]
        assert all(mode.startswith(("drwxr-xr-x", "-rw-r--r--")) for mode in modes), result.stdout

    def test_the_scratch_directory_is_private_while_in_use_and_gone_after(self, tmp_path):
        scratch_root = Path(os.environ.get("TMPDIR", "/tmp"))
        pattern = "airflow-sandbox-[0-9]*"
        assert not list(scratch_root.glob(pattern)), "a previous run leaked a scratch directory"

        process = subprocess.Popen(
            ["sh", "-c", _COMMAND_WRAPPER, "airflow-sandbox", "sleep 2", "1024"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 5.0
            scratch: list[Path] = []
            while not scratch and time.monotonic() < deadline:
                scratch = list(scratch_root.glob(pattern))
                time.sleep(0.05)
            assert scratch, "the wrapper never created its scratch directory"
            # The capture files sit here, so the agent's command must not be able
            # to hand them to another user in a shared image.
            assert scratch[0].stat().st_mode & 0o777 == 0o700
        finally:
            process.communicate(timeout=30)

        assert not list(scratch_root.glob(pattern))

    def test_keeps_the_tail_and_never_a_partial_leading_line(self):
        cap = 100
        result = _run_wrapper("i=1; while [ $i -le 1000 ]; do echo line$i; i=$((i+1)); done", cap)

        # The wrapper is asked for cap+1 bytes, so the backend can tell the
        # stream was over budget; the first record must still be whole.
        assert len(result.stdout.encode()) == cap + 1
        payload, truncated = _bound_result_stream(result.stdout, cap, server_truncated=False)
        assert truncated
        assert payload.endswith("line1000\n")
        assert all(line.startswith("line") for line in payload.splitlines())

    def test_a_terminated_command_exits_nonzero_without_emitting_garbage(self):
        process = subprocess.Popen(
            ["sh", "-c", _COMMAND_WRAPPER, "airflow-sandbox", "sleep 30", "1024"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        time.sleep(1.0)
        # What stopping the microVM looks like from inside it.
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        stdout, _ = process.communicate(timeout=30)

        # A trap that cleans up and then falls through would carry on with its
        # scratch files already deleted and report a clean exit 0.
        assert process.returncode != 0
        assert stdout == ""
