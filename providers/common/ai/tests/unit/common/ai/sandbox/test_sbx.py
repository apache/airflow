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
import subprocess
from unittest.mock import patch

import pytest

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.sbx import _KILL_AFTER, SbxSandboxBackend


def _completed(returncode=0, stdout=b"", stderr=b""):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


@pytest.fixture
def backend():
    return SbxSandboxBackend(host_network_policy="deny-all")


class TestInit:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"image": ""}, "image"),
            ({"memory": ""}, "memory"),
            ({"sbx_path": ""}, "sbx_path"),
            ({"cpus": 0}, "cpus"),
            ({"create_timeout": -1}, "create_timeout"),
            ({"host_network_policy": "nope"}, "host_network_policy"),
        ],
    )
    def test_rejects_invalid_configuration(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            SbxSandboxBackend(**kwargs)


class TestSpecEnforcement:
    """A backend must refuse a restriction it cannot actually apply."""

    def test_an_allowlist_needs_a_deny_all_host_policy_to_mean_anything(self):
        # A per-sandbox allow rule only narrows a deny-all global policy; against
        # an open host policy it grants nothing and would imply a restriction
        # that is not in force.
        with pytest.raises(SandboxTerminalError, match=r"only\s+narrows a deny-all"):
            SbxSandboxBackend().create(spec=SandboxSpec(allow_egress_to=["example.com"]))

    def test_the_allowlist_is_applied_scoped_to_the_new_sandbox(self, backend):
        calls = []

        def fake_run_cli(args, *, timeout, stdin=None):
            calls.append(args)
            return _completed()

        with (
            patch("shutil.which", autospec=True, return_value="/usr/bin/sbx"),
            patch.object(backend, "_run_cli", autospec=True, side_effect=fake_run_cli),
        ):
            name = backend.create(spec=SandboxSpec(allow_egress_to=["pypi.org", "files.pythonhosted.org"]))

        policy = next(c for c in calls if c[:3] == ["policy", "allow", "network"])
        assert policy[3:5] == ["--sandbox", name]
        assert policy[5:] == ["pypi.org", "files.pythonhosted.org"]

    def test_refuses_no_egress_when_the_host_policy_is_undeclared(self):
        with pytest.raises(SandboxError, match="host policy has not been declared"):
            SbxSandboxBackend().create(spec=SandboxSpec(block_network=True))

    def test_refuses_no_egress_when_the_host_policy_allows_it(self):
        with pytest.raises(SandboxError, match="host policy has not been declared"):
            SbxSandboxBackend(host_network_policy="allow-all").create(spec=SandboxSpec())

    def test_accepts_an_acknowledged_open_network(self):
        undeclared = SbxSandboxBackend()
        with (
            patch("shutil.which", autospec=True, return_value="/usr/bin/sbx"),
            patch.object(undeclared, "_run_cli", autospec=True, return_value=_completed()),
        ):
            name = undeclared.create(spec=SandboxSpec(block_network=False))

        assert name.startswith("airflow-sandbox-")


class TestCreate:
    def test_missing_binary_is_terminal_and_says_why(self):
        with patch("shutil.which", autospec=True, return_value=None):
            with pytest.raises(SandboxTerminalError, match="was not found on PATH"):
                SbxSandboxBackend().create()

    def test_failed_create_cleans_up_the_partial_sandbox(self, backend):
        calls = []

        def fake_run_cli(args, *, timeout, stdin=None):
            calls.append(args)
            return _completed(returncode=1, stderr=b"nope")

        with (
            patch("shutil.which", autospec=True, return_value="/usr/bin/sbx"),
            patch.object(backend, "_run_cli", autospec=True, side_effect=fake_run_cli),
        ):
            with pytest.raises(SandboxTerminalError, match="sbx create' failed"):
                backend.create()

        assert calls[0][0] == "create"
        assert calls[1][:2] == ["rm", "-f"]

    def test_env_is_applied_through_the_login_profile(self, backend):
        seen = []

        with (
            patch("shutil.which", autospec=True, return_value="/usr/bin/sbx"),
            patch.object(backend, "_run_cli", autospec=True, return_value=_completed()),
            patch.object(
                backend,
                "_exec_capped_bytes",
                autospec=True,
                side_effect=lambda args, **kw: (
                    seen.append((args, kw.get("stdin"))) or (0, bytearray(), bytearray(), False, False)
                ),
            ),
        ):
            backend.create(spec=SandboxSpec(env={"TOKEN": "s3cret"}, block_network=True))

        profile_call = next(c for c in seen if "/etc/profile" in " ".join(c[0]))
        assert b"export TOKEN=s3cret" in profile_call[1]

    def test_env_values_are_shell_quoted(self, backend):
        seen = []

        with (
            patch("shutil.which", autospec=True, return_value="/usr/bin/sbx"),
            patch.object(backend, "_run_cli", autospec=True, return_value=_completed()),
            patch.object(
                backend,
                "_exec_capped_bytes",
                autospec=True,
                side_effect=lambda args, **kw: (
                    seen.append((args, kw.get("stdin"))) or (0, bytearray(), bytearray(), False, False)
                ),
            ),
        ):
            backend.create(spec=SandboxSpec(env={"X": "a b; rm -rf /"}))

        profile_call = next(c for c in seen if "/etc/profile" in " ".join(c[0]))
        assert b"'a b; rm -rf /'" in profile_call[1]

    def test_a_failed_env_application_does_not_orphan_the_microvm(self, backend):
        # sbx has no server-side TTL, so anything left behind here survives until
        # an operator notices it.
        calls = []

        def fake_run_cli(args, *, timeout, stdin=None):
            calls.append(args)
            return _completed()

        with (
            patch("shutil.which", autospec=True, return_value="/usr/bin/sbx"),
            patch.object(backend, "_run_cli", autospec=True, side_effect=fake_run_cli),
            patch.object(
                backend, "_exec_capped_bytes", autospec=True, side_effect=subprocess.TimeoutExpired("sbx", 1)
            ),
        ):
            with pytest.raises(subprocess.TimeoutExpired):
                backend.create(spec=SandboxSpec(env={"A": "1"}, block_network=True))

        assert calls[0][0] == "create"
        assert ["rm", "-f"] in [c[:2] for c in calls]
        assert backend._workspaces == {}

    def test_a_non_string_env_value_does_not_orphan_the_microvm(self, backend):
        # SandboxSpec.env is typed Mapping[str, str] but nothing validates it, and
        # shlex.quote raises TypeError on an int.
        calls = []

        def fake_run_cli(args, *, timeout, stdin=None):
            calls.append(args)
            return _completed()

        with (
            patch("shutil.which", autospec=True, return_value="/usr/bin/sbx"),
            patch.object(backend, "_run_cli", autospec=True, side_effect=fake_run_cli),
        ):
            with pytest.raises(TypeError):
                backend.create(spec=SandboxSpec(env={"PORT": 8080}, block_network=True))  # type: ignore[dict-item]

        assert ["rm", "-f"] in [c[:2] for c in calls]
        assert backend._workspaces == {}


class TestRunCommand:
    @pytest.mark.parametrize(
        ("returncode", "elapsed", "expected"),
        [
            (124, 0.0, True),  # SIGTERM path: timeout's own exit code
            (137, 100.0, True),  # SIGKILL escalation, after the kill-after point
            (137, 1.0, False),  # fast 137 is an OOM kill, not a timeout
            (0, 0.0, False),
            (1, 0.0, False),
        ],
    )
    def test_timeout_classification(self, backend, returncode, elapsed, expected):
        times = iter([0.0, elapsed])
        with (
            patch.object(
                backend, "_exec_capped", autospec=True, return_value=(returncode, "", "", False, False)
            ),
            patch("time.monotonic", autospec=True, side_effect=lambda: next(times)),
        ):
            result = backend.run_command("box", "x", timeout=10, max_output_bytes=1024)

        assert result.timed_out is expected

    def test_137_between_the_budget_and_the_kill_point_is_not_a_timeout(self, backend):
        # GNU timeout only escalates to SIGKILL at budget + --kill-after, so a
        # 137 arriving before that cannot be escalation.
        times = iter([0.0, 10 + _KILL_AFTER - 1])
        with (
            patch.object(backend, "_exec_capped", autospec=True, return_value=(137, "", "", False, False)),
            patch("time.monotonic", autospec=True, side_effect=lambda: next(times)),
        ):
            result = backend.run_command("box", "x", timeout=10, max_output_bytes=1024)

        assert result.timed_out is False

    def test_hung_cli_destroys_the_sandbox_and_asks_for_a_fresh_one(self, backend):
        with (
            patch.object(
                backend, "_exec_capped", autospec=True, side_effect=subprocess.TimeoutExpired("sbx", 1)
            ),
            patch.object(backend, "destroy", autospec=True) as destroy,
        ):
            result = backend.run_command("box", "x", timeout=1, max_output_bytes=1024)

        destroy.assert_called_once_with("box")
        assert result.timed_out
        assert result.sandbox_terminated

    def test_command_runs_through_a_login_shell_under_gnu_timeout(self, backend):
        with patch.object(
            SbxSandboxBackend, "_exec_capped", return_value=(0, "", "", False, False)
        ) as exec_capped:
            backend.run_command("box", "echo hi", timeout=5, max_output_bytes=1024)

        args = exec_capped.call_args[0][0]
        assert args[:3] == ["exec", "box", "timeout"]
        assert args[-3:] == ["sh", "-lc", "echo hi"]

    def test_per_stream_truncation_flags_are_carried_through(self, backend):
        with patch.object(SbxSandboxBackend, "_exec_capped", return_value=(0, "a", "b", True, False)):
            result = backend.run_command("box", "x", timeout=5, max_output_bytes=1024)

        assert result.stdout_truncated
        assert not result.stderr_truncated


class TestWriteFileOverride:
    """sbx overrides the inherited write_file because it can stream stdin."""

    def test_payload_goes_on_stdin_not_in_the_command(self, backend):
        # The inherited default embeds the content in the command, which the
        # guest's command-line length caps. stdin has no such ceiling.
        with patch.object(
            backend,
            "_exec_capped_bytes",
            autospec=True,
            return_value=(0, bytearray(), bytearray(), False, False),
        ) as exec_capped:
            backend.write_file("box", "/w/a", b"data")

        assert exec_capped.call_args.kwargs["stdin"] == base64.b64encode(b"data")
        assert exec_capped.call_args[0][0][:3] == ["exec", "-i", "box"]

    def test_paths_are_shell_quoted(self, backend):
        with patch.object(
            backend,
            "_exec_capped_bytes",
            autospec=True,
            return_value=(0, bytearray(), bytearray(), False, False),
        ) as exec_capped:
            backend.write_file("box", "/w/a b; rm -rf /", b"x")

        assert "'/w/a b; rm -rf /'" in " ".join(exec_capped.call_args[0][0])

    def test_a_failed_write_is_recoverable(self, backend):
        with patch.object(
            backend,
            "_exec_capped_bytes",
            autospec=True,
            return_value=(1, bytearray(), bytearray(b"read-only fs"), False, False),
        ):
            with pytest.raises(SandboxError, match="read-only fs"):
                backend.write_file("box", "/w/a", b"x")


class TestDestroy:
    def test_is_idempotent_when_the_sandbox_is_already_gone(self, backend):
        with patch.object(backend, "_run_cli", autospec=True, return_value=_completed(returncode=1)):
            backend.destroy("box")  # must not raise

    def test_removes_the_workspace_even_if_the_cli_times_out(self, backend, tmp_path):
        workspace = tmp_path / "ws"
        workspace.mkdir()
        backend._workspaces["box"] = str(workspace)

        with patch.object(
            backend, "_run_cli", autospec=True, side_effect=subprocess.TimeoutExpired("sbx", 1)
        ):
            backend.destroy("box")

        assert not workspace.exists()
