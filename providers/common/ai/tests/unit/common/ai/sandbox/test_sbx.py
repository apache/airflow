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

import subprocess
import sys
from unittest.mock import patch

import pytest

from airflow.providers.common.ai.sandbox.base import _MAX_OUTPUT_CHARS
from airflow.providers.common.ai.sandbox.sbx import SbxSandboxBackend
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

_MOD = "airflow.providers.common.ai.sandbox.sbx"


def _completed(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["sbx"], returncode=returncode, stdout=stdout, stderr=stderr)


class TestSbxSandboxBackendInit:
    def test_init_makes_no_cli_calls(self):
        with patch(f"{_MOD}.subprocess.run") as run, patch(f"{_MOD}.shutil.which") as which:
            SbxSandboxBackend()
        run.assert_not_called()
        which.assert_not_called()

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"image": ""}, "image"),
            ({"memory": ""}, "memory"),
            ({"cpus": 0}, "cpus"),
            ({"sbx_path": ""}, "sbx_path"),
            ({"create_timeout": float("nan")}, "create_timeout"),
        ],
    )
    def test_init_rejects_invalid(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            SbxSandboxBackend(**kwargs)


class TestSbxSandboxBackendCreate:
    @patch(f"{_MOD}.tempfile.mkdtemp", return_value="/tmp/ws")
    @patch(f"{_MOD}.shutil.which", return_value="/usr/local/bin/sbx")
    @patch(f"{_MOD}.subprocess.run", return_value=_completed(0))
    def test_create_builds_command_and_returns_name(self, run, which, mkdtemp):
        backend = SbxSandboxBackend()

        name = backend.create()

        assert name.startswith("airflow-sandbox-")
        args = run.call_args.args[0]
        assert args == [
            "sbx", "create", "--name", name, "--memory", "2g",
            "--template", "python:3.12-slim", "shell", "/tmp/ws",
        ]  # fmt: skip
        assert backend._workspaces[name] == "/tmp/ws"

    @patch(f"{_MOD}.tempfile.mkdtemp", return_value="/tmp/ws")
    @patch(f"{_MOD}.shutil.which", return_value="/usr/local/bin/sbx")
    @patch(f"{_MOD}.subprocess.run", return_value=_completed(0))
    def test_create_passes_cpus_image_and_memory(self, run, which, mkdtemp):
        backend = SbxSandboxBackend(image="python:3.13-slim", memory="4g", cpus=2)

        backend.create()

        args = run.call_args.args[0]
        assert args[:8] == [
            "sbx", "create", "--name", args[3], "--memory", "4g", "--template", "python:3.13-slim",
        ]  # fmt: skip
        assert "--cpus" in args
        assert args[args.index("--cpus") + 1] == "2"

    @patch(f"{_MOD}.shutil.which", return_value=None)
    def test_create_raises_when_binary_missing(self, which):
        with pytest.raises(AirflowOptionalProviderFeatureException, match="sbx"):
            SbxSandboxBackend().create()

    @patch(f"{_MOD}.shutil.rmtree")
    @patch(f"{_MOD}.tempfile.mkdtemp", return_value="/tmp/ws")
    @patch(f"{_MOD}.shutil.which", return_value="/usr/local/bin/sbx")
    @patch(f"{_MOD}.subprocess.run", return_value=_completed(1, stderr="boom"))
    def test_create_cleans_up_on_nonzero_exit(self, run, which, mkdtemp, rmtree):
        backend = SbxSandboxBackend()

        with pytest.raises(RuntimeError, match="sbx create.*boom"):
            backend.create()

        rmtree.assert_called_once_with("/tmp/ws", ignore_errors=True)
        # A best-effort 'sbx rm' of the possibly-orphaned sandbox was attempted.
        assert any(call.args[0][:2] == ["sbx", "rm"] for call in run.call_args_list)
        assert backend._workspaces == {}

    @patch(f"{_MOD}.shutil.rmtree")
    @patch(f"{_MOD}.tempfile.mkdtemp", return_value="/tmp/ws")
    @patch(f"{_MOD}.shutil.which", return_value="/usr/local/bin/sbx")
    @patch(f"{_MOD}.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="sbx", timeout=600))
    def test_create_cleans_up_on_timeout(self, run, which, mkdtemp, rmtree):
        backend = SbxSandboxBackend()

        with pytest.raises(subprocess.TimeoutExpired):
            backend.create()

        rmtree.assert_called_once_with("/tmp/ws", ignore_errors=True)
        assert backend._workspaces == {}


class TestSbxSandboxBackendRun:
    def test_run_wraps_in_timeout_and_maps_output(self):
        backend = SbxSandboxBackend()
        with patch.object(backend, "_exec_capped", return_value=(0, "42\n", "", False)) as ec:
            result = backend.run("sbx-1", ["python3", "-c", "print(42)"], timeout=10.0)

        args = ec.call_args.args[0]
        assert args == [
            "exec", "sbx-1", "timeout", "--kill-after=10", "10", "python3", "-c", "print(42)",
        ]  # fmt: skip
        assert ec.call_args.kwargs["timeout"] == pytest.approx(40.0)  # 10 + _EXEC_GRACE
        assert result.exit_code == 0
        assert result.stdout == "42\n"
        assert result.timed_out is False
        assert result.truncated is False

    def test_run_exit_124_is_timeout(self):
        # GNU timeout exits exactly 124 when the budget is hit.
        backend = SbxSandboxBackend()
        with patch.object(backend, "_exec_capped", return_value=(124, "", "", False)):
            result = backend.run("sbx-1", ["sleep", "60"], timeout=10.0)
        assert result.timed_out is True

    @pytest.mark.parametrize("exit_code", [0, 1])
    def test_run_normal_exit_is_not_timeout(self, exit_code):
        backend = SbxSandboxBackend()
        with patch.object(backend, "_exec_capped", return_value=(exit_code, "", "x", False)):
            result = backend.run("sbx-1", ["python3", "-c", "..."], timeout=10.0)
        assert result.timed_out is False
        assert result.exit_code == exit_code

    @pytest.mark.parametrize(
        ("elapsed", "expected_timed_out"),
        [
            # SIGKILL escalation after the budget elapsed (command ignored SIGTERM).
            (15.0, True),
            # Fast SIGKILL well before the budget — an OOM kill, not a timeout.
            (2.0, False),
        ],
    )
    def test_run_exit_137_is_timeout_only_after_budget(self, elapsed, expected_timed_out):
        backend = SbxSandboxBackend()
        with (
            patch(f"{_MOD}.time.monotonic", side_effect=[100.0, 100.0 + elapsed]),
            patch.object(backend, "_exec_capped", return_value=(137, "", "", False)),
        ):
            result = backend.run("sbx-1", ["python3", "-c", "..."], timeout=10.0)
        assert result.timed_out is expected_timed_out
        assert result.exit_code == 137

    def test_run_propagates_truncation_flag(self):
        backend = SbxSandboxBackend()
        with patch.object(backend, "_exec_capped", return_value=(0, "x", "", True)):
            result = backend.run("sbx-1", ["yes"], timeout=10.0)
        assert result.truncated is True

    def test_run_cli_hang_destroys_and_replaces(self):
        backend = SbxSandboxBackend()
        with (
            patch.object(backend, "_exec_capped", side_effect=subprocess.TimeoutExpired("sbx", 40)),
            patch(f"{_MOD}.subprocess.run", return_value=_completed(0)) as run,
        ):
            result = backend.run("sbx-1", ["sleep", "999"], timeout=10.0)

        assert result.timed_out is True
        assert result.sandbox_terminated is True
        assert any(call.args[0][:2] == ["sbx", "rm"] for call in run.call_args_list)

    @pytest.mark.parametrize("timeout", [0, -1, float("nan")])
    def test_run_rejects_invalid_timeout(self, timeout):
        with pytest.raises(ValueError, match="timeout"):
            SbxSandboxBackend().run("sbx-1", ["true"], timeout=timeout)


class TestSbxSandboxBackendExecCapped:
    """Exercises the real Popen + capped-drain path (no sbx needed: run Python directly)."""

    def test_bounds_stdout_and_stderr_and_flags_truncation(self):
        backend = SbxSandboxBackend(sbx_path=sys.executable)
        code = "import sys; sys.stdout.write('x'*200000); sys.stderr.write('y'*200000)"
        rc, out, err, truncated = backend._exec_capped(["-c", code], timeout=30.0)

        assert rc == 0
        assert len(out) == _MAX_OUTPUT_CHARS
        assert len(err) == _MAX_OUTPUT_CHARS
        assert truncated is True

    def test_small_output_is_untruncated(self):
        backend = SbxSandboxBackend(sbx_path=sys.executable)
        rc, out, err, truncated = backend._exec_capped(["-c", "print('hi')"], timeout=30.0)
        assert rc == 0
        assert out.strip() == "hi"
        assert truncated is False

    def test_raises_on_timeout(self):
        backend = SbxSandboxBackend(sbx_path=sys.executable)
        with pytest.raises(subprocess.TimeoutExpired):
            backend._exec_capped(["-c", "import time; time.sleep(30)"], timeout=1.0)


class TestSbxSandboxBackendDestroy:
    @patch(f"{_MOD}.shutil.rmtree")
    @patch(f"{_MOD}.subprocess.run", return_value=_completed(0))
    def test_destroy_removes_sandbox_and_workspace(self, run, rmtree):
        backend = SbxSandboxBackend()
        backend._workspaces["sbx-1"] = "/tmp/ws"

        backend.destroy("sbx-1")

        assert run.call_args.args[0] == ["sbx", "rm", "-f", "sbx-1"]
        rmtree.assert_called_once_with("/tmp/ws", ignore_errors=True)
        assert backend._workspaces == {}

    @patch(f"{_MOD}.subprocess.run", return_value=_completed(1, stderr="not found"))
    def test_destroy_ignores_missing_sandbox(self, run):
        SbxSandboxBackend().destroy("gone")  # Does not raise.

    @patch(f"{_MOD}.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="sbx", timeout=120))
    def test_destroy_swallows_cli_timeout(self, run):
        SbxSandboxBackend().destroy("sbx-1")  # Does not raise.
