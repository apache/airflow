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
import os
import signal
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("islo")

import httpx
from islo.core.api_error import ApiError
from islo.errors import NotFoundError
from islo.sandboxes.client import SandboxesClient

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.islo import (
    _COMMAND_WRAPPER,
    _SERVER_STREAM_CAP,
    IsloSandboxBackend,
    _bound_result_stream,
)

_MODULE = "airflow.providers.common.ai.sandbox.islo"
_HOOK_PATH = f"{_MODULE}.IsloHook"
_ISLO_PATH = "islo.Islo"


def _exec_result(status="completed", exit_code=0, stdout="", stderr="", truncated=False):
    return SimpleNamespace(
        status=status, exit_code=exit_code, stdout=stdout, stderr=stderr, truncated=truncated
    )


def _sandbox_info(name="box-1", status="running", deleted_at=None):
    return SimpleNamespace(name=name, status=status, deleted_at=deleted_at)


def _backend_with_client(**kwargs) -> tuple[IsloSandboxBackend, mock.MagicMock]:
    backend = IsloSandboxBackend(**kwargs)
    client = mock.MagicMock(spec=["sandboxes"])
    client.sandboxes = mock.create_autospec(SandboxesClient, instance=True)
    client.sandboxes.exec_in_sandbox.return_value = SimpleNamespace(exec_id="exec-1")
    client.sandboxes.create_sandbox.return_value = _sandbox_info()
    client.sandboxes.get_sandbox.return_value = _sandbox_info()
    client.sandboxes.get_exec_result.return_value = _exec_result()
    backend._client = client
    return backend, client


class TestCredentials:
    @mock.patch(_HOOK_PATH, autospec=True)
    def test_client_comes_from_the_hook(self, hook):
        backend = IsloSandboxBackend(islo_conn_id="my_islo")

        client = backend._get_client()

        hook.assert_called_once_with(islo_conn_id="my_islo")
        assert client is hook.return_value.get_conn.return_value

    @mock.patch(_HOOK_PATH, autospec=True)
    def test_client_is_resolved_once_and_cached(self, hook):
        backend = IsloSandboxBackend()

        backend._get_client()
        backend._get_client()

        hook.assert_called_once_with(islo_conn_id="islo_default")
        hook.return_value.get_conn.assert_called_once_with()

    @mock.patch(_HOOK_PATH, autospec=True)
    def test_a_connection_the_hook_rejects_is_terminal_and_actionable(self, hook):
        hook.return_value.get_conn.side_effect = ValueError(
            "Connection 'islo_default' has no password; set it to the Islo API key."
        )

        with pytest.raises(SandboxTerminalError, match="has no password"):
            IsloSandboxBackend()._get_client()

    @mock.patch(_ISLO_PATH, autospec=True)
    def test_none_conn_id_defers_to_the_sdk_environment(self, islo):
        backend = IsloSandboxBackend(islo_conn_id=None)

        backend._get_client()

        islo.assert_called_once_with()

    @mock.patch(_HOOK_PATH, autospec=True)
    def test_connection_resolution_failure_is_terminal(self, hook):
        hook.return_value.get_conn.side_effect = RuntimeError("secret backend down")

        with pytest.raises(SandboxTerminalError, match="initialize its client"):
            IsloSandboxBackend()._get_client()

    def test_missing_sdk_error_is_actionable(self):
        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name.startswith("islo"):
                raise ImportError("blocked for test")
            return real_import(name, *args, **kwargs)

        backend = IsloSandboxBackend(islo_conn_id=None)
        with mock.patch("builtins.__import__", side_effect=blocked_import):
            with pytest.raises(SandboxTerminalError, match=r"\[islo\]"):
                backend.create()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"image": ""}, "image"),
        ({"vcpus": 0}, "vcpus"),
        ({"memory_mb": 0}, "memory_mb"),
        ({"delete_after": 0}, "delete_after"),
        ({"pause_after_idle": 0}, "pause_after_idle"),
        ({"auto_resume": "sometimes"}, "auto_resume"),
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

    def test_refuses_an_address_egress_allowlist(self):
        backend, client = _backend_with_client()

        # internet_enabled is all-or-nothing, so honouring block_network=True alone
        # would silently drop the ranges the Dag author asked to reach.
        with pytest.raises(SandboxTerminalError, match="allow_egress_to_cidrs"):
            backend.create(spec=SandboxSpec(allow_egress_to_cidrs=["203.0.113.0/24"]))

        client.sandboxes.create_sandbox.assert_not_called()

    def test_refuses_a_path_the_runner_would_drop(self):
        backend, client = _backend_with_client()

        with pytest.raises(SandboxTerminalError, match="PATH"):
            backend.create(spec=SandboxSpec(env={"PATH": "/opt/tool/bin"}))

        client.sandboxes.create_sandbox.assert_not_called()

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
        backend, client = _backend_with_client(
            image="python",
            vcpus=2,
            memory_mb=1024,
            pause_after_idle=300,
            auto_resume="never",
            delete_after=120,
        )

        name = backend.create(spec=SandboxSpec(env={"TOKEN": "value"}))

        assert name == "box-1"
        kwargs = client.sandboxes.create_sandbox.call_args.kwargs
        assert kwargs["image"] == "python"
        assert kwargs["vcpus"] == 2
        assert kwargs["memory_mb"] == 1024
        assert kwargs["env"] == {"TOKEN": "value"}
        assert kwargs["lifecycle"].pause_after_idle == 300
        assert kwargs["lifecycle"].auto_resume == "never"
        assert kwargs["lifecycle"].delete_after == 120
        assert kwargs["request_options"] == {"timeout_in_seconds": 120}

    def test_default_lifecycle_pauses_idle_sandboxes_and_deletes_after_a_day(self):
        backend, client = _backend_with_client()

        backend.create()

        lifecycle = client.sandboxes.create_sandbox.call_args.kwargs["lifecycle"]
        assert lifecycle.pause_after_idle == 600
        assert lifecycle.auto_resume == "on_activity"
        assert lifecycle.delete_after == 86400

    def test_disabled_lifecycle_timers_are_sent_as_unset(self):
        backend, client = _backend_with_client(pause_after_idle=None, delete_after=None)

        backend.create()

        lifecycle = client.sandboxes.create_sandbox.call_args.kwargs["lifecycle"]
        assert lifecycle.pause_after_idle is None
        assert lifecycle.delete_after is None

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

    def test_a_sandbox_that_cannot_serve_after_creation_is_destroyed_and_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.create_sandbox.return_value = _sandbox_info(status="stopped")

        with pytest.raises(SandboxTerminalError, match="cannot serve requests"):
            backend.create()

        client.sandboxes.delete_sandbox.assert_called_once()

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

    def test_budget_above_the_server_cap_is_clamped_to_it(self):
        backend, client = _backend_with_client()

        backend.run_command("box", "x", timeout=5, max_output_bytes=5 * _SERVER_STREAM_CAP)

        command = client.sandboxes.exec_in_sandbox.call_args.kwargs["command"]
        assert command[-1] == str(_SERVER_STREAM_CAP)

    def test_requests_leave_the_sdk_retries_in_place(self):
        backend, client = _backend_with_client()

        backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        for call in (client.sandboxes.exec_in_sandbox, client.sandboxes.get_exec_result):
            assert "max_retries" not in call.call_args.kwargs["request_options"]

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

    def test_the_server_flag_marks_only_a_stream_at_the_server_cap(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(
            stdout="x" * _SERVER_STREAM_CAP, stderr="short\n", truncated=True
        )

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=_SERVER_STREAM_CAP)

        assert result.stdout_truncated
        assert not result.stderr_truncated

    def test_the_server_flag_alone_does_not_mark_streams_that_fit(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(
            stdout="a\n", stderr="b\n", truncated=True
        )

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert not result.stdout_truncated
        assert not result.stderr_truncated

    @mock.patch.object(IsloSandboxBackend, "_await_exec", autospec=True, return_value=None)
    def test_poll_deadline_destroys_the_sandbox(self, _await_exec):
        backend, client = _backend_with_client()

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert result.timed_out
        assert result.sandbox_terminated
        client.sandboxes.delete_sandbox.assert_called_once()

    @pytest.mark.parametrize(
        ("delete_after", "reclaim"),
        [
            (3600, "its delete_after policy removes it 3600s after creation"),
            (None, "delete_after is disabled, so it persists until deleted by hand"),
        ],
    )
    @mock.patch(f"{_MODULE}.log", autospec=True)
    @mock.patch.object(IsloSandboxBackend, "_await_exec", autospec=True, return_value=None)
    def test_timeout_cleanup_failure_warns_and_says_whether_anything_reclaims_it(
        self, _await_exec, logger, delete_after, reclaim
    ):
        backend, client = _backend_with_client(delete_after=delete_after)
        client.sandboxes.delete_sandbox.side_effect = ApiError(status_code=503)

        result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        # A command that merely ran long must not fail the task because one
        # cleanup call was refused, but the operator must learn whether the
        # microVM will be reclaimed at all.
        assert result.timed_out
        assert result.sandbox_terminated
        assert "could not confirm its deletion" in logger.warning.call_args.args[0]
        assert logger.warning.call_args.args[2] == reclaim

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

    def test_transient_poll_errors_are_ridden_out(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.side_effect = [
            ApiError(status_code=502),
            httpx.ReadError("connection reset"),
            ApiError(status_code=None),
            _exec_result(stdout="done\n"),
        ]

        with mock.patch("time.sleep", autospec=True):
            result = backend.run_command("box", "x", timeout=60, max_output_bytes=1024)

        assert result.stdout == "done\n"
        assert not result.timed_out
        client.sandboxes.delete_sandbox.assert_not_called()

    @pytest.mark.parametrize("error", [ApiError(status_code=404), RuntimeError("bad client state")])
    def test_a_non_transient_poll_error_is_terminal(self, error):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.side_effect = error

        with pytest.raises(SandboxTerminalError, match="poll a sandbox command"):
            backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

    def test_transient_errors_lasting_past_the_deadline_are_terminal_not_a_timeout(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError, match="poll a sandbox command"):
            backend.run_command("box", "x", timeout=0.05, max_output_bytes=1024)

        # Nothing was heard from the command, so claiming it timed out would be a guess.
        client.sandboxes.delete_sandbox.assert_not_called()

    def test_the_last_poll_keeps_a_minimum_http_budget(self):
        backend, client = _backend_with_client()

        backend.run_command("box", "x", timeout=1, max_output_bytes=1024)

        options = client.sandboxes.get_exec_result.call_args.kwargs["request_options"]
        assert options["timeout_in_seconds"] == 5

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
            request_options={"timeout_in_seconds": 120, "chunk_size": 101},
        )
        client.sandboxes.exec_in_sandbox.assert_not_called()

    def test_oversized_read_stops_closes_the_stream_and_reports_the_real_size(self):
        backend, client = _backend_with_client()
        closed: list[bool] = []

        def chunks():
            try:
                yield b"x" * 11
                raise AssertionError("the backend must stop after the sentinel byte")
            finally:
                closed.append(True)

        client.sandboxes.download_file.return_value = chunks()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="123456\n")

        with pytest.raises(SandboxFileTooLargeError) as error:
            backend.read_file("box", "/w/a", max_bytes=10)

        assert closed == [True]
        assert error.value.size_bytes == 123456
        assert client.sandboxes.exec_in_sandbox.call_args.kwargs["command"][4].startswith("stat -Lc %s -- ")

    def test_oversized_read_without_a_size_says_so_rather_than_inventing_one(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.return_value = iter([b"x" * 11])
        client.sandboxes.get_exec_result.return_value = _exec_result(exit_code=1, stderr="stat: no such file")

        with pytest.raises(SandboxError, match="larger than the 10 byte read limit") as error:
            backend.read_file("box", "/w/a", max_bytes=10)

        assert not isinstance(error.value, (SandboxTerminalError, SandboxFileTooLargeError))

    def test_missing_file_is_recoverable_when_the_sandbox_exists(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = NotFoundError({})

        with pytest.raises(SandboxError, match="does not exist") as error:
            backend.read_file("box", "/w/missing", max_bytes=100)

        assert not isinstance(error.value, SandboxTerminalError)
        client.sandboxes.get_sandbox.assert_called_once()

    @pytest.mark.parametrize(
        "info",
        [_sandbox_info(status="stopped"), _sandbox_info(deleted_at="2026-09-19T00:00:00Z")],
        ids=["stopped", "deleted"],
    )
    def test_missing_file_on_a_sandbox_that_cannot_serve_is_terminal(self, info):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = NotFoundError({})
        client.sandboxes.get_sandbox.return_value = info

        with pytest.raises(SandboxTerminalError, match="cannot serve requests"):
            backend.read_file("box", "/w/a", max_bytes=100)

    @pytest.mark.parametrize(("auto_resume", "terminal"), [("on_activity", False), ("never", True)])
    def test_a_paused_sandbox_is_usable_only_when_it_resumes_on_activity(self, auto_resume, terminal):
        backend, client = _backend_with_client(auto_resume=auto_resume)
        client.sandboxes.download_file.side_effect = NotFoundError({})
        client.sandboxes.get_sandbox.return_value = _sandbox_info(status="paused")

        with pytest.raises(SandboxError) as error:
            backend.read_file("box", "/w/a", max_bytes=100)

        assert isinstance(error.value, SandboxTerminalError) is terminal

    def test_missing_sandbox_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = NotFoundError({})
        client.sandboxes.get_sandbox.side_effect = NotFoundError({})

        with pytest.raises(SandboxTerminalError, match="check a sandbox"):
            backend.read_file("box", "/w/a", max_bytes=100)

    @pytest.mark.parametrize(
        "error",
        [
            ApiError(status_code=503),
            ApiError(status_code=401),
            ApiError(status_code=429),
            httpx.ConnectError("reset"),
        ],
        ids=["503", "401", "429", "no-response"],
    )
    def test_a_failure_that_says_nothing_about_the_path_is_terminal(self, error):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = error

        with pytest.raises(SandboxTerminalError, match="read a sandbox file"):
            backend.read_file("box", "/w/a", max_bytes=100)

        client.sandboxes.get_sandbox.assert_not_called()

    def test_a_rejected_path_is_recoverable_and_carries_the_api_message(self):
        # Measured: a relative path is a 400 whose message names the problem.
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = ApiError(
            status_code=400,
            body={"code": "INVALID_REQUEST", "message": "path must be absolute: rel.txt"},
        )

        with pytest.raises(SandboxError) as error:
            backend.read_file("box", "rel.txt", max_bytes=100)

        assert not isinstance(error.value, SandboxTerminalError)
        assert str(error.value) == "Could not read 'rel.txt' (HTTP 400): path must be absolute: rel.txt."

    def test_a_long_api_message_is_trimmed(self):
        backend, client = _backend_with_client()
        client.sandboxes.download_file.side_effect = ApiError(status_code=400, body={"message": "x" * 5000})

        with pytest.raises(SandboxError) as error:
            backend.read_file("box", "/w/a", max_bytes=100)

        assert len(str(error.value)) < 300

    def test_write_file_creates_parents_then_uses_native_upload(self):
        backend, client = _backend_with_client()

        backend.write_file("box", "/w/sub/a", b"data")

        command = client.sandboxes.exec_in_sandbox.call_args.kwargs["command"]
        assert "mkdir -p" in command[4]
        client.sandboxes.upload_file.assert_called_once_with(
            "box",
            path="/w/sub/a",
            file=("upload", b"data", "application/octet-stream"),
            request_options={"timeout_in_seconds": 120},
        )

    def test_write_stops_when_parent_creation_fails(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(exit_code=1, stderr="read-only")

        with pytest.raises(SandboxError, match="read-only"):
            backend.write_file("box", "/w/a", b"data")

        client.sandboxes.upload_file.assert_not_called()

    def test_upload_outage_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.upload_file.side_effect = ApiError(status_code=503)

        with pytest.raises(SandboxTerminalError, match="write a sandbox file"):
            backend.write_file("box", "/w/a", b"data")

    def test_an_unwritable_target_is_recoverable_with_a_hint(self):
        # Measured: uploading onto a directory, or into /proc or /sys, is a bare
        # 500 whose body says only "An internal error occurred".
        backend, client = _backend_with_client()
        client.sandboxes.upload_file.side_effect = ApiError(
            status_code=500, body={"code": "INTERNAL_ERROR", "message": "An internal error occurred"}
        )

        with pytest.raises(SandboxError) as error:
            backend.write_file("box", "/tmp", b"data")

        assert not isinstance(error.value, SandboxTerminalError)
        assert "(HTTP 500)." in str(error.value)
        assert "directory or on a read-only filesystem" in str(error.value)
        assert "internal error" not in str(error.value)
        client.sandboxes.get_sandbox.assert_called_once()

    def test_an_upload_error_on_a_sandbox_that_cannot_serve_is_terminal(self):
        backend, client = _backend_with_client()
        client.sandboxes.upload_file.side_effect = ApiError(status_code=500)
        client.sandboxes.get_sandbox.return_value = _sandbox_info(status="stopped")

        with pytest.raises(SandboxTerminalError, match="cannot serve requests"):
            backend.write_file("box", "/w/a", b"data")

    def test_list_directory_marks_directories_and_preserves_newlines(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="f a.txt\0d new\nline\0")

        assert backend.list_directory("box", "/w") == [("a.txt", False), ("new\nline", True)]

    @mock.patch(f"{_MODULE}._HELPER_OUTPUT_CAP", 16)
    def test_a_listing_cut_at_the_head_drops_only_the_leading_record(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(stdout="f aaaa\0d bbbb\0d cccc\0")

        assert backend.list_directory("box", "/w") == [("bbbb", True), ("cccc", True)]

    def test_the_server_flag_alone_leaves_a_short_listing_intact(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(
            stdout="f a.txt\0d sub\0", truncated=True
        )

        assert backend.list_directory("box", "/w") == [("a.txt", False), ("sub", True)]

    def test_list_failure_is_recoverable(self):
        backend, client = _backend_with_client()
        client.sandboxes.get_exec_result.return_value = _exec_result(exit_code=1, stderr="missing")

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


def _run_wrapper(command: str, max_output_bytes: int, *, timeout: float = 30.0, env: dict | None = None):
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
        env=env,
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

    def test_the_process_environment_reaches_the_command_unchanged(self):
        # A login shell would run /etc/profile after the spec's variables were set,
        # and Debian's and macOS's both rewrite PATH.
        env = {**os.environ, "PATH": "/spec/wins:" + os.environ["PATH"]}
        result = _run_wrapper('echo "${PATH%%:*}"', 1024, env=env)

        assert result.stdout == "/spec/wins\n"

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
        result = _run_wrapper("nohup sleep 300 >/dev/null 2>&1 & echo $! >&2; echo server-started", 1024)
        elapsed = time.monotonic() - start
        try:
            assert result.stdout == "server-started\n"
            assert elapsed < 5.0
        finally:
            os.kill(int(result.stderr.strip()), signal.SIGTERM)

    def test_does_not_change_the_permissions_of_what_the_agent_creates(self, tmp_path):
        result = _run_wrapper(f"cd {tmp_path} && touch a_file && mkdir a_dir && ls -ld a_dir a_file", 4096)

        # A umask left in force for the agent's command would make these 700/600.
        assert result.returncode == 0
        modes = [line.split()[0] for line in result.stdout.strip().splitlines()]
        assert all(mode.startswith(("drwxr-xr-x", "-rw-r--r--")) for mode in modes), result.stdout

    def test_the_scratch_directory_is_private_while_in_use_and_gone_after(self, tmp_path):
        scratch_root = tmp_path
        pattern = "airflow-sandbox-[0-9]*"

        process = subprocess.Popen(
            ["sh", "-c", _COMMAND_WRAPPER, "airflow-sandbox", "sleep 2", "1024"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "TMPDIR": str(tmp_path)},
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

    def test_a_terminated_command_exits_143_and_emits_nothing(self):
        process = subprocess.Popen(
            ["sh", "-c", _COMMAND_WRAPPER, "airflow-sandbox", "sleep 30", "1024"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        pgid = os.getpgid(process.pid)
        # The traps are in place once the agent's command itself is running,
        # which on a loaded host can take longer than any fixed pause.
        deadline = time.monotonic() + 10.0
        while (
            subprocess.run(
                ["pgrep", "-g", str(pgid), "-x", "sleep"], capture_output=True, check=False
            ).returncode
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        # What stopping the microVM looks like from inside it.
        os.killpg(pgid, signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=30)

        # Without the trap the wrapper dies of the signal itself (a negative
        # return code); a trap that cleans up and then falls through reaches
        # ``tail`` with its scratch files already deleted and reports that on
        # stderr. bash may still announce the child's death ("Terminated: 15"),
        # which is the shell's notice, not wrapper output.
        assert process.returncode == 143
        assert stdout == ""
        assert "No such file" not in stderr
