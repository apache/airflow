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
from unittest.mock import patch

import pytest

from airflow.providers.common.ai.sandbox.sbx import _MAX_OUTPUT_CHARS, SbxSandboxBackend
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

        assert name.startswith("airflow-sbx-")
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
            "sbx",
            "create",
            "--name",
            args[3],
            "--memory",
            "4g",
            "--template",
            "python:3.13-slim",
        ]
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
    def test_create_cleans_workspace_and_raises_on_failure(self, run, which, mkdtemp, rmtree):
        backend = SbxSandboxBackend()

        with pytest.raises(RuntimeError, match="sbx create.*boom"):
            backend.create()

        rmtree.assert_called_once_with("/tmp/ws", ignore_errors=True)
        assert backend._workspaces == {}


class TestSbxSandboxBackendRun:
    @patch(f"{_MOD}.time.monotonic", side_effect=[0.0, 0.5])
    @patch(f"{_MOD}.subprocess.run", return_value=_completed(0, stdout="42\n"))
    def test_run_wraps_in_timeout_and_maps_output(self, run, monotonic):
        result = SbxSandboxBackend().run("sbx-1", ["python3", "-c", "print(42)"], timeout=10.0)

        args = run.call_args.args[0]
        assert args == [
            "sbx",
            "exec",
            "sbx-1",
            "timeout",
            "--signal=KILL",
            "10",
            "python3",
            "-c",
            "print(42)",
        ]
        assert run.call_args.kwargs["timeout"] == pytest.approx(40.0)  # 10 + _EXEC_GRACE
        assert result.exit_code == 0
        assert result.stdout == "42\n"
        assert result.timed_out is False
        assert result.truncated is False

    @patch(f"{_MOD}.time.monotonic", side_effect=[0.0, 0.1])
    @patch(f"{_MOD}.subprocess.run", return_value=_completed(3, stderr="boom"))
    def test_run_nonzero_exit_is_not_timed_out(self, run, monotonic):
        result = SbxSandboxBackend().run("sbx-1", ["false"], timeout=10.0)

        assert result.exit_code == 3
        assert result.stderr == "boom"
        assert result.timed_out is False

    @pytest.mark.parametrize("exit_code", [124, 137])
    @patch(f"{_MOD}.time.monotonic", side_effect=[0.0, 50.0])
    def test_run_timeout_after_full_budget(self, monotonic, exit_code):
        with patch(f"{_MOD}.subprocess.run", return_value=_completed(exit_code)):
            result = SbxSandboxBackend().run("sbx-1", ["sleep", "60"], timeout=10.0)
        assert result.timed_out is True

    @pytest.mark.parametrize("exit_code", [124, 137])
    @patch(f"{_MOD}.time.monotonic", side_effect=[0.0, 0.2])
    def test_run_fast_124_137_is_not_timed_out(self, monotonic, exit_code):
        with patch(f"{_MOD}.subprocess.run", return_value=_completed(exit_code)):
            result = SbxSandboxBackend().run("sbx-1", ["python3", "-c", "..."], timeout=300.0)
        assert result.timed_out is False
        assert result.exit_code == exit_code

    @patch(f"{_MOD}.time.monotonic", side_effect=[0.0, 0.5])
    def test_run_caps_output_and_flags_truncation(self, monotonic):
        big = "x" * (_MAX_OUTPUT_CHARS + 100)
        with patch(f"{_MOD}.subprocess.run", return_value=_completed(0, stdout=big)):
            result = SbxSandboxBackend().run("sbx-1", ["yes"], timeout=10.0)
        assert len(result.stdout) == _MAX_OUTPUT_CHARS
        assert result.truncated is True

    @patch(f"{_MOD}.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="sbx", timeout=40))
    def test_run_cli_hang_destroys_and_replaces(self, run):
        result = SbxSandboxBackend().run("sbx-1", ["sleep", "999"], timeout=10.0)

        assert result.timed_out is True
        assert result.sandbox_terminated is True
        # A destroy (another subprocess.run for 'rm') was attempted after the hang.
        assert any(call.args[0][:2] == ["sbx", "rm"] for call in run.call_args_list)

    @pytest.mark.parametrize("timeout", [0, -1, float("nan")])
    def test_run_rejects_invalid_timeout(self, timeout):
        with pytest.raises(ValueError, match="timeout"):
            SbxSandboxBackend().run("sbx-1", ["true"], timeout=timeout)


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
