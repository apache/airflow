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
import json
import os
import subprocess
from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("boat_sdk")

from boat_sdk.exceptions import ApiException
from boat_sdk.models.command_response import CommandResponse

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.boat import BoatSandboxBackend

_MONOTONIC = "airflow.providers.common.ai.sandbox.boat.time.monotonic"
_SLEEP = "airflow.providers.common.ai.sandbox.boat.time.sleep"


def _api_error(status: int, body: str | None = None) -> ApiException:
    return ApiException(status=status, body=body)


def _command_response(
    *,
    exit_code=0,
    stdout="",
    stderr="",
    timed_out=False,
    stdout_truncated=False,
    stderr_truncated=False,
    signal=None,
    oom_killed=None,
):
    """The oneOf wrapper the SDK returns, carrying a real ``CommandResponse``."""
    result = CommandResponse(
        ok=True,
        type="command.finished",
        success=exit_code == 0,
        exitCode=exit_code,
        stdout=stdout,
        stderr=stderr,
        stdoutTruncated=stdout_truncated,
        stderrTruncated=stderr_truncated,
        timedOut=timed_out,
        signal=signal,
        oomKilled=oom_killed,
    )
    return SimpleNamespace(actual_instance=result)


def _created(sandbox_id: str):
    return SimpleNamespace(sandbox=SimpleNamespace(id=sandbox_id))


def _sandbox_info(state: str):
    return SimpleNamespace(sandbox=SimpleNamespace(id="bx_1", state=state))


def _run_like_boat(wrapped: str, *, home, timeout: float) -> subprocess.CompletedProcess[str]:
    """Run a wrapped command in a login shell, as Boat does, with ``home`` keeping the host's profile out."""
    return subprocess.run(
        ["bash", "-lc", wrapped],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env={"HOME": str(home), "PATH": os.environ["PATH"]},
    )


def _backend_with_api(**kwargs) -> tuple[BoatSandboxBackend, mock.MagicMock]:
    backend = BoatSandboxBackend(**kwargs)
    api = mock.MagicMock(
        spec=["create", "update", "get", "command", "read_file", "write_file", "delete_sandbox"]
    )
    backend._boat_api = api
    return backend, api


@pytest.fixture
def clock():
    """A monotonic clock that moves only when the backend sleeps."""
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    with (
        mock.patch(_MONOTONIC, autospec=True, side_effect=lambda: now[0]),
        mock.patch(_SLEEP, autospec=True, side_effect=sleep),
    ):
        yield now


def test_missing_sdk_error_is_actionable():
    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name.startswith("boat_sdk"):
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    backend = BoatSandboxBackend()
    with mock.patch.dict("os.environ", {"BOAT_API_KEY": "boat_key"}, clear=False):
        with mock.patch("builtins.__import__", side_effect=blocked_import):
            with pytest.raises(SandboxTerminalError, match=r"\[boat\]"):
                backend._get_api()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"machine_type": "xlarge"}, "machine_type"),
        ({"ttl_seconds": 0}, "ttl_seconds"),
        ({"ttl_seconds": 0.5}, "whole number of seconds"),
        ({"ttl_seconds": True}, "ttl_seconds"),
        ({"ttl_seconds": 2_592_001}, "at most 2592000"),
        ({"ready_timeout": 0}, "ready_timeout"),
        ({"ready_timeout": False}, "ready_timeout"),
        ({"request_timeout": 0}, "request_timeout"),
        ({"no_env": "false"}, "no_env"),
    ],
)
def test_constructor_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        BoatSandboxBackend(**kwargs)


def test_request_timeout_bounds_every_api_call():
    backend, api = _backend_with_api(request_timeout=12.5, ready_timeout=45)
    api.create.return_value = _created("bx_1")
    api.get.return_value = _sandbox_info("ready")
    api.command.return_value = _command_response()

    backend.create(spec=SandboxSpec(block_network=False))
    backend.run_command("bx_1", "true", timeout=1, max_output_bytes=8)
    backend.write_file("bx_1", "/tmp/f", b"x")
    backend._confirm_sandbox_exists("bx_1")
    backend.destroy("bx_1")

    def sent(method):
        return [call.kwargs["_request_timeout"] for call in method.call_args_list]

    # A call that waits on an operation gets the operation's time plus request_timeout.
    assert sent(api.create) == [45 + 12.5]
    assert sent(api.command) == [1 + 15 + 12.5, 120 + 15 + 12.5]
    assert sent(api.write_file) == [120 + 12.5]
    # One that answers at once gets request_timeout alone.
    assert sent(api.update) == [12.5]
    assert sent(api.get) == [12.5, 12.5]
    assert sent(api.delete_sandbox) == [12.5]


class TestCredentials:
    @mock.patch("boat_sdk.ApiClient", autospec=True)
    def test_construction_reads_no_credentials_and_opens_no_client(self, api_client):
        with mock.patch.dict("os.environ", {}, clear=True):
            BoatSandboxBackend()

        api_client.assert_not_called()

    @mock.patch("boat_sdk.api.boat_api.BoatApi", autospec=True)
    @mock.patch("boat_sdk.ApiClient", autospec=True)
    @mock.patch("boat_sdk.Configuration", autospec=True)
    def test_environment_key_and_base_url_are_read(self, configuration, _client, boat_api):
        with mock.patch.dict(
            "os.environ",
            {"BOAT_API_KEY": " env-key ", "BOAT_BASE_URL": "https://custom.example/api/v1/"},
            clear=False,
        ):
            BoatSandboxBackend()._get_api()

        configuration.assert_called_once_with(host="https://custom.example/api/v1", access_token="env-key")
        boat_api.assert_called_once()

    @mock.patch("boat_sdk.api.boat_api.BoatApi", autospec=True)
    @mock.patch("boat_sdk.ApiClient", autospec=True)
    @mock.patch("boat_sdk.Configuration", autospec=True)
    def test_client_is_resolved_once_and_cached(self, configuration, _client, _boat_api):
        with mock.patch.dict("os.environ", {"BOAT_API_KEY": "env-key"}, clear=False):
            backend = BoatSandboxBackend()
            backend._get_api()
            backend._get_api()

        configuration.assert_called_once_with(host="https://boat.dev/api/v1", access_token="env-key")

    def test_missing_api_key_is_terminal(self):
        with mock.patch.dict("os.environ", {"BOAT_API_KEY": ""}, clear=False):
            with pytest.raises(SandboxTerminalError, match="BOAT_API_KEY is not set"):
                BoatSandboxBackend()._get_api()


class TestCreate:
    def test_refuses_an_owner_because_nothing_could_attach(self):
        # The ownership rules ride on per-sandbox metadata the attaching side reads back,
        # which this backend does not keep; refuse before anything is provisioned.
        backend, api = _backend_with_api()

        with pytest.raises(SandboxTerminalError, match="owner"):
            backend.create(spec=SandboxSpec(block_network=False, owner="dag/run"))

        api.create.assert_not_called()

    def test_refuses_a_per_domain_egress_allowlist(self):
        backend, _ = _backend_with_api()

        with pytest.raises(SandboxTerminalError, match="per-domain egress allowlist"):
            backend.create(spec=SandboxSpec(allow_egress_to=["example.com"]))

    def test_refuses_block_network(self):
        backend, _ = _backend_with_api()

        with pytest.raises(SandboxTerminalError, match="cannot deny outbound network access"):
            backend.create(spec=SandboxSpec(block_network=True))

    def test_refuses_cidr_egress_allowlist(self):
        backend, _ = _backend_with_api()

        with pytest.raises(SandboxTerminalError, match="CIDR egress allowlist"):
            backend.create(spec=SandboxSpec(block_network=False, allow_egress_to_cidrs=["203.0.113.0/24"]))

    @pytest.mark.parametrize("no_env", [True, False])
    def test_spec_and_sizing_are_passed_at_creation(self, no_env):
        backend, api = _backend_with_api(machine_type="small", ttl_seconds=120, no_env=no_env)
        api.create.return_value = _created("bx_created1")
        api.get.return_value = _sandbox_info("ready")

        sandbox_id = backend.create(spec=SandboxSpec(block_network=False, env={"TOKEN": "value"}))

        assert sandbox_id == "bx_created1"
        request = api.create.call_args.kwargs["create_sandbox_request"]
        assert request.type == "small"
        assert request.ttl_seconds == 120
        assert request.no_env is no_env
        assert request.env == {"TOKEN": "value"}
        api.get.assert_called_once_with("bx_created1", _request_timeout=30.0)
        assert api.update.called

    def test_none_spec_is_allowed(self):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_created1")
        api.get.return_value = _sandbox_info("ready")

        backend.create()

        request = api.create.call_args.kwargs["create_sandbox_request"]
        assert request.env is None

    @pytest.mark.parametrize(
        ("polls", "match"),
        [
            pytest.param(TimeoutError("read timed out"), "ready: TimeoutError", id="poll_failed"),
            pytest.param(
                [_sandbox_info("starting"), _sandbox_info("error")],
                "entered state 'error' before it was ready",
                id="failed_while_starting",
            ),
            pytest.param(
                # Boat reports a cancelled create once, then answers 404 for it.
                [_sandbox_info("starting"), _sandbox_info("cancelled"), _api_error(404)],
                "entered state 'cancelled' before it was ready",
                id="cancelled_while_starting",
            ),
        ],
    )
    def test_a_sandbox_that_never_becomes_ready_is_destroyed(self, clock, polls, match):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_stuck01")
        api.get.side_effect = polls

        with pytest.raises(SandboxTerminalError, match=match):
            backend.create(spec=SandboxSpec(block_network=False))

        # The id never reached the caller, so create is the only place that can
        # still tear this sandbox down.
        api.delete_sandbox.assert_called_once_with("bx_stuck01", "bx_stuck01", _request_timeout=mock.ANY)

    def test_each_readiness_poll_is_bounded_by_what_is_left_of_ready_timeout(self, clock):
        backend, api = _backend_with_api(ready_timeout=5)
        api.create.return_value = _created("bx_stuck01")
        api.get.return_value = _sandbox_info("starting")

        with pytest.raises(SandboxTerminalError, match="not ready within 5 seconds"):
            backend.create(spec=SandboxSpec(block_network=False))

        # Without an HTTP timeout, one stalled response would hold create forever.
        assert [call.kwargs["_request_timeout"] for call in api.get.call_args_list] == [5.0, 3.0, 1.0]
        assert clock[0] == 5.0
        api.delete_sandbox.assert_called_once_with("bx_stuck01", "bx_stuck01", _request_timeout=mock.ANY)

    def test_an_unnameable_sandbox_is_still_created(self):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_created1")
        api.get.return_value = _sandbox_info("ready")
        api.update.side_effect = _api_error(500)

        assert backend.create(spec=SandboxSpec(block_network=False)) == "bx_created1"

    def test_api_failure_is_terminal(self):
        backend, api = _backend_with_api()
        api.create.side_effect = _api_error(503)

        with pytest.raises(SandboxTerminalError, match="HTTP 503"):
            backend.create(spec=SandboxSpec(block_network=False))

    def test_api_error_reason_is_surfaced(self):
        backend, api = _backend_with_api()
        api.create.side_effect = _api_error(
            402,
            json.dumps(
                {
                    "ok": False,
                    "status": 402,
                    "code": "billing_required",
                    "message": "Start the $20/month plan to create sandboxes.",
                    "error": {
                        "code": "billing_required",
                        "message": "Start the $20/month plan to create sandboxes.",
                    },
                }
            ),
        )

        with pytest.raises(
            SandboxTerminalError,
            match=r"HTTP 402\)\. billing_required: Start the \$20/month plan",
        ):
            backend.create(spec=SandboxSpec(block_network=False))

    @pytest.mark.parametrize("body", [None, "", "<html>gateway</html>", "[]", '{"error": "not_a_dict"}'])
    def test_unparseable_error_body_is_ignored(self, body):
        backend, api = _backend_with_api()
        api.create.side_effect = _api_error(500, body)

        with pytest.raises(SandboxTerminalError, match=r"^Boat could not create a sandbox \(HTTP 500\)\.$"):
            backend.create(spec=SandboxSpec(block_network=False))

    @pytest.mark.parametrize("state", ["ready", "idle", "running"])
    def test_confirm_exists_accepts_a_runnable_state(self, state):
        backend, api = _backend_with_api()
        api.get.return_value = _sandbox_info(state)

        backend._confirm_sandbox_exists("bx_1")

    @pytest.mark.parametrize("state", ["archiving", "archived", "error"])
    def test_confirm_exists_rejects_a_sandbox_that_cannot_run(self, state):
        backend, api = _backend_with_api()
        api.get.return_value = _sandbox_info(state)

        with pytest.raises(SandboxTerminalError, match="not runnable"):
            backend._confirm_sandbox_exists("bx_1")


class TestRunCommand:
    @pytest.mark.parametrize(
        ("command", "stdout", "stderr", "exit_code"),
        [
            ("echo hi # trailing comment", "hi\n", "", 0),
            ("cat <<'EOF'\nheredoc-ok\nEOF", "heredoc-ok\n", "", 0),
            ("echo failure >&2; exit 3", "", "failure\n", 3),
            ('printf "%s" "$SPEC_MARKER"', "kept", "", 0),
            ("sleep 20 & echo background-ok", "background-ok\n", "", 0),
        ],
    )
    def test_wrapper_preserves_shell_syntax_and_results(self, tmp_path, command, stdout, stderr, exit_code):
        backend, api = _backend_with_api()
        backend._sandbox_env["bx_1"] = {"SPEC_MARKER": "kept"}
        api.command.return_value = _command_response()

        backend.run_command("bx_1", command, timeout=5, max_output_bytes=1024)
        result = _run_like_boat(api.command.call_args.args[1].command, home=tmp_path, timeout=5)

        assert (result.stdout, result.stderr, result.returncode) == (stdout, stderr, exit_code)

    @pytest.mark.parametrize(
        ("command", "exit_code"),
        [
            pytest.param("echo partial; echo err-partial >&2; sleep 30", 124, id="stopped_by_sigterm"),
            pytest.param(
                "trap '' TERM; echo partial; echo err-partial >&2; sleep 30",
                137,
                id="killed_after_ignoring_it",
            ),
        ],
    )
    @mock.patch("airflow.providers.common.ai.sandbox.boat._KILL_AFTER", 1)
    def test_wrapper_returns_what_a_command_printed_before_its_deadline(self, tmp_path, command, exit_code):
        backend, api = _backend_with_api()
        backend._sandbox_env["bx_1"] = {"SPEC_MARKER": "kept"}
        api.command.return_value = _command_response()

        backend.run_command("bx_1", command, timeout=1, max_output_bytes=1024)
        result = _run_like_boat(api.command.call_args.args[1].command, home=tmp_path, timeout=10)

        assert (result.stdout, result.stderr, result.returncode) == ("partial\n", "err-partial\n", exit_code)

    @pytest.mark.parametrize(
        ("command", "expected"),
        [
            pytest.param('printf "%s" "$PROFILE_ONLY"', "from-profile", id="unexported_variable"),
            pytest.param("profile_function", "from-function", id="function"),
            pytest.param('printf "%s" "$SPEC_MARKER"', "kept", id="spec_env_applied_after_it"),
        ],
    )
    def test_the_command_runs_with_the_guests_login_profile(self, tmp_path, command, expected):
        (tmp_path / ".bash_profile").write_text(
            "PROFILE_ONLY=from-profile\n"
            "profile_function() { printf from-function; }\n"
            "export SPEC_MARKER=from-profile\n"
        )
        backend, api = _backend_with_api()
        backend._sandbox_env["bx_1"] = {"SPEC_MARKER": "kept"}
        api.command.return_value = _command_response()

        backend.run_command("bx_1", command, timeout=5, max_output_bytes=1024)
        result = _run_like_boat(api.command.call_args.args[1].command, home=tmp_path, timeout=10)

        assert (result.stdout, result.stderr, result.returncode) == (expected, "", 0)

    def test_forwards_command_and_bounds_output(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(
            stdout="0" * 20, stderr="err", stdout_truncated=True, stderr_truncated=False
        )

        result = backend.run_command("bx_1", "echo hi", timeout=5, max_output_bytes=8)

        request = api.command.call_args.args[1]
        assert "timeout --kill-after=5 5 bash -lc 'echo hi' " in request.command
        assert "mktemp -d" in request.command
        assert '>"$tmp_dir/stdout" 2>"$tmp_dir/stderr"' in request.command
        assert 'command_pid=$!; wait "$command_pid"' in request.command
        # Boat's own deadline sits past the in-guest SIGTERM and SIGKILL.
        assert request.timeout_seconds == 5 + 5 + 10
        assert result.stdout == "0" * 8
        assert result.stdout_truncated
        assert result.stderr == "err"
        assert result.exit_code == 0

    def test_spec_environment_is_exported_after_shell_profile(self):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_env01")
        api.get.return_value = _sandbox_info("ready")
        api.command.return_value = _command_response()

        backend.create(spec=SandboxSpec(block_network=False, env={"SPEC_MARKER": "kept"}))
        backend.run_command("bx_env01", "printf '%s' \"$SPEC_MARKER\"", timeout=5, max_output_bytes=1024)

        command = api.command.call_args.args[1].command
        assert "export SPEC_MARKER=kept" in command
        assert "mktemp -d" in command

    def test_a_detached_command_response_is_terminal(self):
        backend, api = _backend_with_api()
        api.command.return_value = SimpleNamespace(actual_instance=SimpleNamespace(id="cmd_1"))

        with pytest.raises(SandboxTerminalError, match="no result for the command"):
            backend.run_command("bx_1", "echo hi", timeout=5, max_output_bytes=8)

    def test_a_timeout_above_the_api_cap_is_the_models_to_fix(self):
        backend, api = _backend_with_api()

        with pytest.raises(SandboxError, match="Ask for a shorter timeout") as error:
            backend.run_command("bx_1", "sleep 1", timeout=601, max_output_bytes=1024)

        assert not isinstance(error.value, SandboxTerminalError)
        api.command.assert_not_called()

    @pytest.mark.parametrize(
        ("timeout", "applied", "boat_timeout"),
        [(2.2, 3.0, 18), (600, 585.0, 600)],
        ids=["rounded_up", "shortened_under_the_api_cap"],
    )
    @pytest.mark.parametrize("timed_out", [False, True], ids=["finished", "boat_deadline"])
    def test_the_deadline_the_command_got_is_reported(self, timed_out, timeout, applied, boat_timeout):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=-1 if timed_out else 0, timed_out=timed_out)

        with mock.patch.object(backend, "destroy", autospec=True):
            result = backend.run_command("bx_1", "true", timeout=timeout, max_output_bytes=1024)

        assert api.command.call_args.args[1].timeout_seconds == boat_timeout
        assert result.applied_timeout == applied

    @pytest.mark.parametrize(
        ("exit_code", "elapsed", "timed_out"),
        [
            pytest.param(124, 10.2, True, id="sigterm_at_the_deadline"),
            pytest.param(137, 15.1, True, id="sigkill_after_the_deadline"),
            pytest.param(124, 0.5, False, id="124_from_the_command_itself"),
            pytest.param(137, 0.5, False, id="killed_before_the_deadline"),
            pytest.param(1, 12.0, False, id="other_exit"),
        ],
    )
    def test_the_in_guest_deadline_keeps_the_sandbox_and_output(self, exit_code, elapsed, timed_out):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=exit_code, stdout="partial\n", stderr="err\n")

        with mock.patch(_MONOTONIC, side_effect=[0.0, elapsed]):
            result = backend.run_command("bx_1", "sleep 99", timeout=10, max_output_bytes=1024)

        assert result.timed_out is timed_out
        assert result.exit_code == exit_code
        assert (result.stdout, result.stderr) == ("partial\n", "err\n")
        assert not result.sandbox_terminated
        api.delete_sandbox.assert_not_called()

    def test_boats_own_deadline_destroys_the_sandbox(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=None, timed_out=True)

        result = backend.run_command("bx_1", "sleep 99", timeout=1, max_output_bytes=1024)

        assert result.timed_out
        assert result.sandbox_terminated
        assert result.stderr == ""
        api.delete_sandbox.assert_called_once_with("bx_1", "bx_1", _request_timeout=mock.ANY)

    @pytest.mark.parametrize(
        ("signal", "oom_killed", "stderr", "expected"),
        [
            pytest.param(
                "SIGKILL",
                True,
                "",
                "The command was killed by SIGKILL when the sandbox ran out of memory.",
                id="oom_with_signal",
            ),
            pytest.param(
                None, True, "", "The command was killed when the sandbox ran out of memory.", id="oom"
            ),
            pytest.param("SIGTERM", None, "boom\n", "boom\nThe command was killed by SIGTERM.", id="signal"),
            pytest.param(
                None, None, "", "The command ended without reporting an exit status.", id="no_reason"
            ),
        ],
    )
    def test_a_missing_exit_status_is_explained(self, signal, oom_killed, stderr, expected):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(
            exit_code=None, signal=signal, oom_killed=oom_killed, stderr=stderr
        )

        result = backend.run_command("bx_1", "python big.py", timeout=10, max_output_bytes=1024)

        assert (result.exit_code, result.stderr, result.timed_out) == (-1, expected, False)

    def test_boats_own_deadline_with_a_failed_delete_is_terminal(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=None, timed_out=True)
        api.delete_sandbox.side_effect = _api_error(500)

        with pytest.raises(SandboxTerminalError, match="deletion of its sandbox could not be confirmed"):
            backend.run_command("bx_1", "sleep 99", timeout=1, max_output_bytes=1024)


class TestFiles:
    def test_read_file_caps_the_transfer_inside_the_guest(self):
        backend, api = _backend_with_api()
        payload = b"hello-world"
        api.command.return_value = _command_response(
            stdout=f"{len(payload)}\n{base64.b64encode(payload).decode()}"
        )

        assert backend.read_file("bx_1", "/tmp/a.txt", max_bytes=64) == payload

        # The guest, not the worker, is what bounds the read: the cap reaches it
        # as a head -c argument rather than arriving after the bytes do.
        assert f"head -c {64 + 1} --" in api.command.call_args.args[1].command
        assert api.read_file.called is False

    def test_read_file_rejects_a_file_over_budget(self):
        backend, api = _backend_with_api()
        payload = b"hello-world"
        api.command.return_value = _command_response(
            stdout=f"{len(payload)}\n{base64.b64encode(payload).decode()}"
        )

        with pytest.raises(SandboxFileTooLargeError):
            backend.read_file("bx_1", "/tmp/a.txt", max_bytes=4)

    def test_missing_file_is_recoverable(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=66)

        with pytest.raises(SandboxError, match="does not exist"):
            backend.read_file("bx_1", "/tmp/missing", max_bytes=10)

    def test_a_read_stopped_at_its_deadline_is_recoverable(self):
        # A FIFO passes the stat check and then blocks head -c until the deadline.
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=124)

        with mock.patch(_MONOTONIC, side_effect=[0.0, 120.5]):
            with pytest.raises(SandboxError) as raised:
                backend.read_file("bx_1", "/tmp/fifo", max_bytes=10)

        assert not isinstance(raised.value, SandboxTerminalError)
        request = api.command.call_args.args[1]
        assert "timeout --kill-after=5 120 bash -lc " in request.command
        assert request.timeout_seconds == 120 + 5 + 10
        api.delete_sandbox.assert_not_called()

    @pytest.mark.parametrize(
        "operation",
        [
            pytest.param(lambda backend: backend.read_file("bx_1", "/tmp/x", max_bytes=10), id="read_file"),
            pytest.param(lambda backend: backend.list_directory("bx_1", "/tmp/x"), id="list_directory"),
        ],
    )
    @pytest.mark.parametrize(
        ("response", "expected", "match"),
        [
            pytest.param(
                _command_response(exit_code=None, timed_out=True),
                SandboxTerminalError,
                r"ended while '/tmp/x' was being (read|listed)",
                id="deleted_at_boats_deadline",
            ),
            pytest.param(
                _command_response(exit_code=1, stderr="Permission denied\n"),
                SandboxError,
                "Permission denied",
                id="failed_in_a_live_sandbox",
            ),
        ],
    )
    def test_a_file_operation_is_terminal_once_boats_deadline_deleted_the_sandbox(
        self, operation, response, expected, match
    ):
        # A retry offered against a deleted sandbox can only fail on its next call.
        backend, api = _backend_with_api()
        api.command.return_value = response

        with pytest.raises(SandboxError, match=match) as raised:
            operation(backend)

        assert type(raised.value) is expected

    def test_write_file_uses_base64_and_creates_parents(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response()

        backend.write_file("bx_1", "/tmp/dir/file.bin", b"\x00\x01")

        mkdir_request = api.command.call_args.args[1]
        assert "mkdir -p" in mkdir_request.command
        write_request = api.write_file.call_args.args[1]
        assert write_request.path == "/tmp/dir/file.bin"
        assert write_request.encoding == "base64"
        assert base64.b64decode(write_request.content) == b"\x00\x01"

    def test_write_file_stops_when_the_parent_directory_cannot_be_made(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(
            exit_code=1, stderr="mkdir: cannot create directory '/opt/app': Permission denied\n"
        )

        with pytest.raises(SandboxError, match="Permission denied") as raised:
            backend.write_file("bx_1", "/opt/app/main.py", b"x")

        assert not isinstance(raised.value, SandboxTerminalError)
        api.write_file.assert_not_called()

    def test_write_file_is_terminal_when_boats_deadline_ends_the_mkdir(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=None, timed_out=True)

        with pytest.raises(SandboxTerminalError, match="ended while the parent directory"):
            backend.write_file("bx_1", "/tmp/dir/file.bin", b"x")

        api.write_file.assert_not_called()
        api.delete_sandbox.assert_called_once_with("bx_1", "bx_1", _request_timeout=mock.ANY)

    @pytest.mark.parametrize(
        ("error", "sandbox_gone", "expected", "match"),
        [
            pytest.param(
                _api_error(404), False, SandboxError, "Could not write", id="missing_in_live_sandbox"
            ),
            pytest.param(_api_error(404), True, SandboxTerminalError, "HTTP 404", id="sandbox_gone"),
            pytest.param(
                _api_error(
                    400,
                    json.dumps(
                        {"error": {"code": "invalid_path", "message": "must be under /home/user or /tmp"}}
                    ),
                ),
                False,
                SandboxError,
                r"HTTP 400\)\. invalid_path: must be under",
                id="refused_path",
            ),
            pytest.param(_api_error(500), False, SandboxTerminalError, "HTTP 500", id="server_error"),
        ],
    )
    def test_write_file_maps_api_errors(self, error, sandbox_gone, expected, match):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response()
        api.write_file.side_effect = error
        if sandbox_gone:
            api.get.side_effect = _api_error(404)
        else:
            api.get.return_value = _sandbox_info("idle")

        with pytest.raises(SandboxError, match=match) as raised:
            backend.write_file("bx_1", "/opt/app/main.py", b"x")

        assert type(raised.value) is expected


class TestDestroy:
    @pytest.mark.parametrize("error", [None, _api_error(404)], ids=["accepted", "already_gone"])
    def test_delete_clears_retained_environment(self, error):
        backend, api = _backend_with_api()
        backend._sandbox_env["bx_1"] = {"SPEC_MARKER": "kept"}
        api.delete_sandbox.side_effect = error

        backend.destroy("bx_1")

        assert "bx_1" not in backend._sandbox_env

    def test_delete_sends_the_sandbox_id_as_its_own_confirmation(self):
        backend, api = _backend_with_api()

        backend.destroy("bx_gone01")

        api.delete_sandbox.assert_called_once_with("bx_gone01", "bx_gone01", _request_timeout=mock.ANY)

    def test_a_failed_delete_is_terminal(self):
        backend, api = _backend_with_api()
        api.delete_sandbox.side_effect = _api_error(500)

        with pytest.raises(SandboxTerminalError, match="delete a sandbox"):
            backend.destroy("bx_1")
