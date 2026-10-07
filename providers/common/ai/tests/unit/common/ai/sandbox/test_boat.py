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
    )
    return SimpleNamespace(actual_instance=result)


def _created(sandbox_id: str):
    return SimpleNamespace(sandbox=SimpleNamespace(id=sandbox_id))


def _backend_with_api(**kwargs) -> tuple[BoatSandboxBackend, mock.MagicMock]:
    backend = BoatSandboxBackend(**kwargs)
    api = mock.MagicMock(
        spec=["create", "update", "get", "command", "read_file", "write_file", "delete_sandbox"]
    )
    backend._boat_api = api
    return backend, api


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
        ({"ready_timeout": 0}, "ready_timeout"),
        ({"ready_timeout": False}, "ready_timeout"),
        ({"request_timeout": 0}, "request_timeout"),
        ({"no_env": "false"}, "no_env"),
    ],
)
def test_constructor_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        BoatSandboxBackend(**kwargs)


class TestCredentials:
    @mock.patch("boat_sdk.ApiClient", autospec=True)
    def test_construction_reads_no_credentials_and_opens_no_client(self, api_client):
        with mock.patch.dict("os.environ", {}, clear=True):
            BoatSandboxBackend()

        api_client.assert_not_called()

    @mock.patch("boat_sdk.api.boat_api.BoatApi", autospec=True)
    @mock.patch("boat_sdk.ApiClient", autospec=True)
    @mock.patch("boat_sdk.Configuration", autospec=True)
    def test_environment_key_and_constructor_knobs_are_forwarded(self, configuration, _client, boat_api):
        with mock.patch.dict(
            "os.environ",
            {"BOAT_API_KEY": " env-key ", "BOAT_BASE_URL": "https://custom.example/api/v1/"},
            clear=False,
        ):
            backend = BoatSandboxBackend(request_timeout=12.5)
            backend._get_api()

        configuration.assert_called_once_with(host="https://custom.example/api/v1", access_token="env-key")
        boat_api.assert_called_once()
        assert backend._request_timeout == 12.5

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
    @mock.patch("boat_sdk.wait_until_ready", autospec=True)
    def test_spec_and_sizing_are_passed_at_creation(self, wait_ready, no_env):
        backend, api = _backend_with_api(
            machine_type="small", ttl_seconds=120, ready_timeout=45, no_env=no_env
        )
        api.create.return_value = _created("bx_created1")

        sandbox_id = backend.create(spec=SandboxSpec(block_network=False, env={"TOKEN": "value"}))

        assert sandbox_id == "bx_created1"
        request = api.create.call_args.kwargs["create_sandbox_request"]
        assert request.type == "small"
        assert request.ttl_seconds == 120
        assert request.no_env is no_env
        assert request.env == {"TOKEN": "value"}
        wait_ready.assert_called_once_with(
            mock.ANY, "bx_created1", timeout_seconds=45, poll_interval_seconds=2.0
        )
        assert api.update.called

    @mock.patch("boat_sdk.wait_until_ready", autospec=True)
    def test_none_spec_is_allowed(self, _wait_ready):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_created1")

        backend.create()

        request = api.create.call_args.kwargs["create_sandbox_request"]
        assert request.env is None

    @mock.patch("boat_sdk.wait_until_ready", autospec=True)
    def test_a_sandbox_that_never_becomes_ready_is_destroyed(self, wait_ready):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_stuck01")
        wait_ready.side_effect = TimeoutError("never became ready")

        with pytest.raises(SandboxTerminalError):
            backend.create(spec=SandboxSpec(block_network=False))

        # The id never reached the caller, so create is the only place that can
        # still tear this sandbox down.
        api.delete_sandbox.assert_called_once_with("bx_stuck01", "bx_stuck01", _request_timeout=mock.ANY)

    @mock.patch("boat_sdk.wait_until_ready", autospec=True)
    def test_an_unnameable_sandbox_is_still_created(self, _wait_ready):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_created1")
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
        api.get.return_value = SimpleNamespace(sandbox=SimpleNamespace(id="bx_1", state=state))

        backend._confirm_sandbox_exists("bx_1")

    @pytest.mark.parametrize("state", ["archiving", "archived", "error"])
    def test_confirm_exists_rejects_a_sandbox_that_cannot_run(self, state):
        backend, api = _backend_with_api()
        api.get.return_value = SimpleNamespace(sandbox=SimpleNamespace(id="bx_1", state=state))

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
        ],
    )
    def test_wrapper_preserves_shell_syntax_and_results(self, command, stdout, stderr, exit_code):
        backend, api = _backend_with_api()
        backend._sandbox_env["bx_1"] = {"SPEC_MARKER": "kept"}
        api.command.return_value = _command_response()

        backend.run_command("bx_1", command, timeout=5, max_output_bytes=1024)
        wrapped = api.command.call_args.args[1].command
        result = subprocess.run(
            ["bash", "-c", wrapped], capture_output=True, text=True, timeout=5, check=False
        )

        assert (result.stdout, result.stderr, result.returncode) == (stdout, stderr, exit_code)

    def test_forwards_command_and_bounds_output(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(
            stdout="0" * 20, stderr="err", stdout_truncated=True, stderr_truncated=False
        )

        result = backend.run_command("bx_1", "echo hi", timeout=5, max_output_bytes=8)

        request = api.command.call_args.args[1]
        assert "echo hi" in request.command
        assert "mktemp -d" in request.command
        assert '>"$tmp_dir/stdout" 2>"$tmp_dir/stderr"' in request.command
        assert 'command_pid=$!; wait "$command_pid"' in request.command
        assert request.timeout_seconds == 5
        assert result.stdout == "0" * 8
        assert result.stdout_truncated
        assert result.stderr == "err"
        assert result.exit_code == 0

    @mock.patch("boat_sdk.wait_until_ready", autospec=True)
    def test_spec_environment_is_exported_after_shell_profile(self, _wait_ready):
        backend, api = _backend_with_api()
        api.create.return_value = _created("bx_env01")
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

    @pytest.mark.parametrize("timed_out", [False, True], ids=["finished", "timed_out"])
    def test_the_whole_second_deadline_the_command_got_is_reported(self, timed_out):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=-1 if timed_out else 0, timed_out=timed_out)

        with mock.patch.object(backend, "destroy", autospec=True):
            result = backend.run_command("bx_1", "true", timeout=2.2, max_output_bytes=1024)

        assert result.applied_timeout == 3.0

    def test_timeout_destroys_sandbox(self):
        backend, api = _backend_with_api()
        api.command.return_value = _command_response(exit_code=-1, timed_out=True, stdout="partial")

        result = backend.run_command("bx_1", "sleep 99", timeout=1, max_output_bytes=1024)

        assert result.timed_out
        assert result.sandbox_terminated
        api.delete_sandbox.assert_called_once_with("bx_1", "bx_1", _request_timeout=mock.ANY)


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
            api.get.return_value = SimpleNamespace(sandbox=SimpleNamespace(id="bx_1", state="idle"))

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
