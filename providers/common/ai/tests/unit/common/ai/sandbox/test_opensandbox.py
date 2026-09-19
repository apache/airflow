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
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("opensandbox")

from opensandbox.exceptions import SandboxApiException
from opensandbox.models.sandboxes import NetworkPolicy, NetworkRule

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.opensandbox import OpenSandboxBackend

_BASE_HOOK_PATH = "airflow.providers.common.ai.sandbox.opensandbox.BaseHook"
_MONOTONIC_PATH = "airflow.providers.common.ai.sandbox.opensandbox.time.monotonic"


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


def _error(value: str, *, traceback: list[str] | None = None):
    return SimpleNamespace(name="CommandError", value=value, traceback=traceback or [])


def _execution(*, error=None, complete=None):
    """Shape a foreground result as the SDK does: an int parsed from error.value, 0 on complete, else None."""
    if error is not None:
        try:
            exit_code = int(error.value)
        except ValueError:
            exit_code = None
    else:
        exit_code = 0 if complete is not None else None
    return SimpleNamespace(exit_code=exit_code, error=error, complete=complete)


def _completed():
    return _execution(complete=SimpleNamespace())


def _deny_policy(*targets: str) -> NetworkPolicy:
    return NetworkPolicy(
        defaultAction="deny",
        egress=[NetworkRule(action="allow", target=target) for target in targets] or None,
    )


def _created(policy: NetworkPolicy | Exception | None = None):
    sandbox = mock.MagicMock(spec=["id", "get_egress_policy", "destroy"])
    sandbox.id = "created"
    if isinstance(policy, Exception):
        sandbox.get_egress_policy.side_effect = policy
    else:
        sandbox.get_egress_policy.return_value = policy
    return sandbox


def _backend_with_sandbox(**kwargs) -> tuple[OpenSandboxBackend, mock.MagicMock]:
    backend = OpenSandboxBackend(**kwargs)
    backend._connection_config = mock.sentinel.connection_config
    sandbox = mock.MagicMock(spec=["id", "commands", "files", "get_info", "destroy"])
    sandbox.id = "box-1"
    sandbox.commands = mock.MagicMock(spec=["run"])
    sandbox.files = mock.MagicMock(
        spec=["read_bytes_stream", "create_directories", "write_file", "list_directory", "get_file_info"]
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

    @pytest.mark.parametrize("host", [None, ""])
    @mock.patch("opensandbox.config.ConnectionConfigSync", autospec=True)
    @mock.patch(_BASE_HOOK_PATH, autospec=True)
    def test_connection_without_host_is_terminal_rather_than_localhost(self, hook, config, host):
        hook.get_connection.return_value = _connection(host=host)

        with pytest.raises(SandboxTerminalError, match="has no host"):
            OpenSandboxBackend()._get_connection_config()

        config.assert_not_called()


class TestCreate:
    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_spec_resources_and_timeouts_are_forwarded(self, create):
        create.return_value = _created(_deny_policy("pypi.org"))
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

    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_sandboxes_are_tagged_as_airflows(self, create):
        create.return_value = _created(_deny_policy())
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.create(spec=SandboxSpec())

        metadata = create.call_args.kwargs["metadata"]
        assert metadata["created-by"] == "airflow"
        assert metadata["name"].startswith("airflow-sandbox-")

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            (SandboxSpec(), "deny"),
            (SandboxSpec(block_network=False), "allow"),
        ],
    )
    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_network_default_is_mapped(self, create, spec, expected):
        create.return_value = _created(_deny_policy())
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.create(spec=spec)

        assert create.call_args.kwargs["network_policy"].default_action == expected

    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_none_spec_states_no_network_requirement(self, create):
        create.return_value = _created()
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.create()

        assert create.call_args.kwargs["network_policy"] is None
        create.return_value.get_egress_policy.assert_not_called()

    def test_open_network_with_allowlist_is_refused(self):
        backend = OpenSandboxBackend()

        with pytest.raises(SandboxTerminalError, match="block_network=True"):
            backend.create(spec=SandboxSpec(block_network=False, allow_egress_to=["example.com"]))

    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_enforced_deny_policy_is_read_back_before_the_sandbox_is_handed_out(self, create):
        create.return_value = _created(_deny_policy("pypi.org"))
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        assert backend.create(spec=SandboxSpec(allow_egress_to=["pypi.org"])) == "created"

        create.return_value.get_egress_policy.assert_called_once_with()
        create.return_value.destroy.assert_not_called()

    @pytest.mark.parametrize(
        "enforced",
        [
            NetworkPolicy(defaultAction="allow", egress=None),
            _deny_policy("pypi.org", "example.com"),
            _deny_policy(),
            _api_error(404),
        ],
        ids=["open-egress", "wider-allowlist", "missing-rule", "no-sidecar-endpoint"],
    )
    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_unenforced_policy_destroys_the_sandbox_and_is_terminal(self, create, enforced):
        create.return_value = _created(enforced)
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        with pytest.raises(SandboxTerminalError, match="did not enforce the requested network policy"):
            backend.create(spec=SandboxSpec(allow_egress_to=["pypi.org"]))

        create.return_value.destroy.assert_called_once_with()
        assert backend._sandboxes == {}

    @mock.patch("opensandbox.SandboxSync.create", autospec=True)
    def test_open_network_needs_no_read_back(self, create):
        create.return_value = _created()
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.create(spec=SandboxSpec(block_network=False))

        create.return_value.get_egress_policy.assert_not_called()

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
            return _execution(error=_error("3"))

        sandbox.commands.run.side_effect = run

        result = backend.run_command("box-1", "echo hi", timeout=9, max_output_bytes=6)

        assert result.exit_code == 3
        assert result.stdout == "éé\n"
        assert result.stderr == "-tail\n"
        assert result.stdout_truncated
        assert result.stderr_truncated
        assert not result.sandbox_terminated

    def test_line_delimiters_are_restored(self):
        """execd strips the delimiter from each streamed line; the tail must put it back."""
        backend, sandbox = _backend_with_sandbox()

        def run(_command, *, opts, handlers):
            for text in ("first", "second", "\n", "third"):
                handlers.on_stdout(SimpleNamespace(text=text))
            handlers.on_stderr(SimpleNamespace(text="err one"))
            handlers.on_stderr(SimpleNamespace(text="err two"))
            return _completed()

        sandbox.commands.run.side_effect = run

        result = backend.run_command("box-1", "printf ...", timeout=9, max_output_bytes=4096)

        assert result.exit_code == 0
        assert result.stdout == "first\nsecond\n\nthird\n"
        assert result.stderr == "err one\nerr two\n"

    def test_execution_error_is_returned_on_stderr(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution(
            error=_error("failed", traceback=["line one", "line two"])
        )

        result = backend.run_command("box-1", "bad", timeout=9, max_output_bytes=100)

        assert result.exit_code == 1
        assert result.stderr == "line one\nline two"

    @pytest.mark.parametrize("value", ["exit status 1", "signal: killed"])
    def test_prose_error_value_is_a_failed_command_not_a_terminal_error(self, value):
        """The SDK cannot parse an exit code out of prose; that is an unknown status, not a dead sandbox."""
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution(error=_error(value))

        result = backend.run_command("box-1", "bad", timeout=9, max_output_bytes=100)

        assert result.exit_code == 1
        assert result.stderr == value
        assert not result.timed_out

    def test_no_terminal_event_is_reported_not_terminal(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution()

        result = backend.run_command("box-1", "echo hi", timeout=5, max_output_bytes=100)

        assert result.exit_code == -1
        assert "without reporting an exit status" in result.stderr

    @pytest.mark.parametrize("value", ["-9", "124", "signal: killed"])
    @mock.patch(_MONOTONIC_PATH, side_effect=[0.0, 5.0])
    def test_nonzero_exit_at_the_deadline_is_a_timeout(self, _monotonic, value):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution(error=_error(value))

        result = backend.run_command("box-1", "sleep 60", timeout=5, max_output_bytes=100)

        assert result.timed_out
        assert not result.sandbox_terminated

    @mock.patch(_MONOTONIC_PATH, side_effect=[0.0, 5.0])
    def test_clean_exit_at_the_deadline_is_not_a_timeout(self, _monotonic):
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _completed()

        result = backend.run_command("box-1", "sleep 5", timeout=5, max_output_bytes=100)

        assert not result.timed_out

    @mock.patch(_MONOTONIC_PATH, side_effect=[0.0, 0.2])
    def test_negative_exit_well_inside_the_deadline_is_not_a_timeout(self, _monotonic):
        """A signal kill is only a timeout when the call also outlived the deadline."""
        backend, sandbox = _backend_with_sandbox()
        sandbox.commands.run.return_value = _execution(error=_error("-9"))

        result = backend.run_command("box-1", "kill -9 $$", timeout=30, max_output_bytes=100)

        assert not result.timed_out

    @mock.patch("opensandbox.SandboxSync.connect", autospec=True)
    def test_reconnect_time_is_not_charged_to_the_command(self, connect):
        remote = mock.MagicMock(spec=["commands"])
        remote.commands = mock.MagicMock(spec=["run"])
        remote.commands.run.return_value = _execution(error=_error("-9"))

        def slow_connect(*_args, **_kwargs):
            time.sleep(0.3)
            return remote

        connect.side_effect = slow_connect
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        result = backend.run_command("remote", "kill -9 $$", timeout=0.25, max_output_bytes=100)

        assert not result.timed_out

    def test_error_details_do_not_overwrite_streamed_stderr(self):
        backend, sandbox = _backend_with_sandbox()

        def run(_command, *, opts, handlers):
            handlers.on_stderr(SimpleNamespace(text="real stderr"))
            return _execution(error=_error("1", traceback=["exit status 1"]))

        sandbox.commands.run.side_effect = run

        result = backend.run_command("box-1", "bad", timeout=9, max_output_bytes=100)

        assert result.stderr == "real stderr\n"

    @mock.patch("airflow.providers.common.ai.sandbox.opensandbox._EXEC_GRACE", 0.0)
    def test_stalled_stream_destroys_the_sandbox_and_reports_a_timeout(self):
        backend, sandbox = _backend_with_sandbox()
        release = threading.Event()

        def hang(_command, *, opts, handlers):
            handlers.on_stdout(SimpleNamespace(text="partial"))
            release.wait(5)
            return _completed()

        sandbox.commands.run.side_effect = hang
        try:
            result = backend.run_command("box-1", "yes", timeout=0.1, max_output_bytes=100)
        finally:
            release.set()

        assert result.timed_out
        assert result.sandbox_terminated
        assert result.exit_code == -1
        assert result.stdout == "partial\n"
        sandbox.destroy.assert_called_once_with()
        assert "box-1" not in backend._sandboxes

    @mock.patch("airflow.providers.common.ai.sandbox.opensandbox._EXEC_GRACE", 0.0)
    def test_stalled_stream_whose_sandbox_cannot_be_destroyed_still_reports_a_timeout(self):
        backend, sandbox = _backend_with_sandbox()
        release = threading.Event()
        sandbox.commands.run.side_effect = lambda *_a, **_k: release.wait(5)
        sandbox.destroy.side_effect = _api_error(503)
        try:
            result = backend.run_command("box-1", "yes", timeout=0.1, max_output_bytes=100)
        finally:
            release.set()

        assert result.timed_out
        assert result.sandbox_terminated

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

    def test_oversized_read_stops_at_the_sentinel_byte_and_reports_the_real_size(self):
        backend, sandbox = _backend_with_sandbox()
        stream = mock.MagicMock(spec=["__iter__", "close"])
        stream.__iter__.return_value = iter([b"x" * 11, b"must-not-be-read"])
        sandbox.files.read_bytes_stream.return_value = stream
        sandbox.files.get_file_info.return_value = {"/w/a": SimpleNamespace(size=1_000_000)}

        with pytest.raises(SandboxFileTooLargeError) as error:
            backend.read_file("box-1", "/w/a", max_bytes=10)

        assert error.value.size_bytes == 1_000_000
        sandbox.files.get_file_info.assert_called_once_with(["/w/a"])
        stream.close.assert_called_once()

    @pytest.mark.parametrize(
        "lookup",
        [{"/w/a": SimpleNamespace(size=0)}, _api_error(500)],
        ids=["streaming-source", "lookup-failed"],
    )
    def test_oversized_read_never_reports_less_than_was_read(self, lookup):
        backend, sandbox = _backend_with_sandbox()
        stream = mock.MagicMock(spec=["__iter__", "close"])
        stream.__iter__.return_value = iter([b"x" * 11])
        sandbox.files.read_bytes_stream.return_value = stream
        if isinstance(lookup, Exception):
            sandbox.files.get_file_info.side_effect = lookup
        else:
            sandbox.files.get_file_info.return_value = lookup

        with pytest.raises(SandboxFileTooLargeError) as error:
            backend.read_file("box-1", "/w/a", max_bytes=10)

        assert error.value.size_bytes == 11

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

    @mock.patch("airflow.providers.common.ai.sandbox.opensandbox._LIST_DIRECTORY_MAX_ENTRIES", 2)
    def test_oversized_listing_is_refused_with_guidance(self):
        backend, sandbox = _backend_with_sandbox()
        sandbox.files.list_directory.return_value = [
            SimpleNamespace(path=f"/w/{index}", entry_type="file") for index in range(3)
        ]

        with pytest.raises(SandboxError, match="more than 2 entries") as error:
            backend.list_directory("box-1", "/w")

        assert not isinstance(error.value, SandboxTerminalError)

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


class TestGetSandbox:
    @mock.patch("opensandbox.SandboxSync.connect", autospec=True)
    def test_uncached_sandbox_is_reconnected_and_then_cached(self, connect):
        remote = mock.MagicMock(spec=["commands"])
        remote.commands = mock.MagicMock(spec=["run"])
        remote.commands.run.return_value = _completed()
        connect.return_value = remote
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.run_command("remote", "echo hi", timeout=5, max_output_bytes=100)
        backend.run_command("remote", "echo hi again", timeout=5, max_output_bytes=100)

        connect.assert_called_once()
        assert connect.call_args.kwargs.get("skip_health_check", False) is False
        assert backend._sandboxes["remote"] is remote

    @mock.patch("opensandbox.SandboxSync.connect", autospec=True)
    def test_reconnect_failure_is_terminal(self, connect):
        connect.side_effect = _api_error(503)
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        with pytest.raises(SandboxTerminalError, match="HTTP 503"):
            backend.run_command("remote", "echo hi", timeout=5, max_output_bytes=100)


class TestDestroy:
    def test_cached_sandbox_is_destroyed_and_evicted(self):
        backend, sandbox = _backend_with_sandbox()

        backend.destroy("box-1")

        sandbox.destroy.assert_called_once()
        assert "box-1" not in backend._sandboxes

    @mock.patch("opensandbox.SandboxSync.connect", autospec=True)
    def test_uncached_sandbox_is_reconnected_without_a_readiness_poll(self, connect):
        remote = mock.MagicMock(spec=["destroy"])
        connect.return_value = remote
        backend = OpenSandboxBackend()
        backend._connection_config = mock.sentinel.connection_config

        backend.destroy("remote")

        remote.destroy.assert_called_once()
        assert connect.call_args.kwargs["skip_health_check"] is True

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
