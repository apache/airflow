#
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
import contextlib
import os
import pty
import select
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from airflow.providers.ssh.utils.remote_job import (
    RemoteJobPaths,
    build_posix_cleanup_command,
    build_posix_completion_check_command,
    build_posix_file_size_command,
    build_posix_kill_command,
    build_posix_log_tail_command,
    build_posix_wrapper_command,
    build_windows_cleanup_command,
    build_windows_completion_check_command,
    build_windows_file_size_command,
    build_windows_kill_command,
    build_windows_log_tail_command,
    build_windows_wrapper_command,
    generate_job_id,
)


class TestGenerateJobId:
    def test_generates_unique_ids(self):
        """Test that job IDs are unique."""
        id1 = generate_job_id("dag1", "task1", "run1", 1)
        id2 = generate_job_id("dag1", "task1", "run1", 1)
        assert id1 != id2

    def test_includes_context_info(self):
        """Test that job ID includes context information."""
        job_id = generate_job_id("my_dag", "my_task", "manual__2024", 2)
        assert "af_" in job_id
        assert "my_dag" in job_id
        assert "my_task" in job_id
        assert "try2" in job_id

    def test_sanitizes_special_characters(self):
        """Test that special characters are sanitized."""
        job_id = generate_job_id("dag-with-dashes", "task.with.dots", "run:with:colons", 1)
        assert "-" not in job_id.split("_try")[0]
        assert "." not in job_id.split("_try")[0]
        assert ":" not in job_id.split("_try")[0]

    def test_suffix_length(self):
        """Test that suffix length is configurable."""
        job_id = generate_job_id("dag", "task", "run", 1, suffix_length=12)
        parts = job_id.split("_")
        assert len(parts[-1]) == 12


class TestRemoteJobPaths:
    def test_posix_default_paths(self):
        """Test POSIX default paths."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="posix")
        assert paths.base_dir == "/tmp/airflow-ssh-jobs"
        assert paths.job_dir == "/tmp/airflow-ssh-jobs/test_job"
        assert paths.log_file == "/tmp/airflow-ssh-jobs/test_job/stdout.log"
        assert paths.exit_code_file == "/tmp/airflow-ssh-jobs/test_job/exit_code"
        assert paths.pid_file == "/tmp/airflow-ssh-jobs/test_job/pid"
        assert paths.sep == "/"

    def test_windows_default_paths(self):
        """Test Windows default paths use $env:TEMP for portability."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="windows")
        assert paths.base_dir == "$env:TEMP\\airflow-ssh-jobs"
        assert paths.job_dir == "$env:TEMP\\airflow-ssh-jobs\\test_job"
        assert paths.log_file == "$env:TEMP\\airflow-ssh-jobs\\test_job\\stdout.log"
        assert paths.exit_code_file == "$env:TEMP\\airflow-ssh-jobs\\test_job\\exit_code"
        assert paths.pid_file == "$env:TEMP\\airflow-ssh-jobs\\test_job\\pid"
        assert paths.sep == "\\"

    def test_custom_base_dir(self):
        """Test custom base directory."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="posix", base_dir="/custom/path")
        assert paths.base_dir == "/custom/path"
        assert paths.job_dir == "/custom/path/test_job"


class TestBuildPosixWrapperCommand:
    def test_basic_command(self):
        """Test basic wrapper command generation."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="posix")
        wrapper = build_posix_wrapper_command("/path/to/script.sh", paths)

        assert "mkdir -p" in wrapper
        assert "nohup bash -c" in wrapper
        assert "/path/to/script.sh" in wrapper
        assert "exit_code" in wrapper
        assert "pid" in wrapper

    def test_with_environment(self):
        """Test wrapper with environment variables."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="posix")
        wrapper = build_posix_wrapper_command(
            "/path/to/script.sh",
            paths,
            environment={"MY_VAR": "my_value", "OTHER": "test"},
        )

        assert "export MY_VAR='my_value'" in wrapper
        assert "export OTHER='test'" in wrapper

    def test_escapes_quotes(self):
        """Test that single quotes in command are escaped."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="posix")
        wrapper = build_posix_wrapper_command("echo 'hello world'", paths)
        assert wrapper is not None

    def test_runs_in_own_process_group(self):
        """The job launches under setsid (when available) and self-reports its PGID."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="posix")
        wrapper = build_posix_wrapper_command("/path/to/script.sh", paths)

        # New session/process group when setsid exists, plain detached run otherwise
        assert "command -v setsid" in wrapper
        assert "setsid bash -c" in wrapper
        assert "nohup bash -c" in wrapper
        # The job self-reports its own pid ($$ == PGID after setsid), which is correct
        # even when setsid(1) forks under job control -- unlike the launcher's $!.
        assert 'echo -n "$$" > "' in wrapper
        # Launcher must NOT record $! (would be the short-lived setsid parent on a fork).
        assert 'echo -n $! > "$pid_file"' not in wrapper


class TestBuildWindowsWrapperCommand:
    def test_basic_command(self):
        """Test basic Windows wrapper command generation."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="windows")
        wrapper = build_windows_wrapper_command("C:\\scripts\\test.ps1", paths)

        assert "powershell.exe" in wrapper
        assert "-EncodedCommand" in wrapper
        # Decode and verify script content
        encoded_script = wrapper.split("-EncodedCommand ")[1]
        decoded_script = base64.b64decode(encoded_script).decode("utf-16-le")
        assert "New-Item -ItemType Directory" in decoded_script
        assert "Start-Process" in decoded_script

    def test_with_environment(self):
        """Test Windows wrapper with environment variables."""
        paths = RemoteJobPaths(job_id="test_job", remote_os="windows")
        wrapper = build_windows_wrapper_command(
            "C:\\scripts\\test.ps1",
            paths,
            environment={"MY_VAR": "my_value"},
        )
        assert wrapper is not None
        assert "-EncodedCommand" in wrapper


class TestLogTailCommands:
    def test_posix_log_tail(self):
        """Test POSIX log tail command uses efficient tail+head pipeline."""
        cmd = build_posix_log_tail_command("/tmp/log.txt", 100, 1024)
        assert "tail -c +101" in cmd  # offset 100 -> byte 101 (1-indexed)
        assert "head -c 1024" in cmd
        assert "/tmp/log.txt" in cmd

    def test_windows_log_tail(self):
        """Test Windows log tail command."""
        cmd = build_windows_log_tail_command("C:\\temp\\log.txt", 100, 1024)
        assert "powershell.exe" in cmd
        assert "-EncodedCommand" in cmd
        # Decode and verify the script content
        encoded_script = cmd.split("-EncodedCommand ")[1]
        decoded_script = base64.b64decode(encoded_script).decode("utf-16-le")
        assert "Seek(100" in decoded_script
        assert "1024" in decoded_script


class TestFileSizeCommands:
    def test_posix_file_size(self):
        """Test POSIX file size command."""
        cmd = build_posix_file_size_command("/tmp/file.txt")
        assert "stat" in cmd
        assert "/tmp/file.txt" in cmd

    def test_windows_file_size(self):
        """Test Windows file size command."""
        cmd = build_windows_file_size_command("C:\\temp\\file.txt")
        assert "powershell.exe" in cmd
        assert "-EncodedCommand" in cmd
        encoded_script = cmd.split("-EncodedCommand ")[1]
        decoded_script = base64.b64decode(encoded_script).decode("utf-16-le")
        assert "Get-Item" in decoded_script
        assert "Length" in decoded_script


class TestCompletionCheckCommands:
    def test_posix_completion_check(self):
        """Test POSIX completion check command."""
        cmd = build_posix_completion_check_command("/tmp/exit_code")
        assert "test -s" in cmd
        assert "cat" in cmd

    def test_windows_completion_check(self):
        """Test Windows completion check command."""
        cmd = build_windows_completion_check_command("C:\\temp\\exit_code")
        assert "powershell.exe" in cmd
        assert "-EncodedCommand" in cmd
        encoded_script = cmd.split("-EncodedCommand ")[1]
        decoded_script = base64.b64decode(encoded_script).decode("utf-16-le")
        assert "Test-Path" in decoded_script
        assert "Get-Content" in decoded_script


class TestKillCommands:
    def test_posix_kill_signals_process_group_then_falls_back(self):
        """POSIX kill targets the process group first, then a single PID as fallback."""
        cmd = build_posix_kill_command("/tmp/pid")
        assert "cat '/tmp/pid'" in cmd
        # Negative PID => signal the whole process group (kills the job's children too)
        assert 'kill -TERM -"$p"' in cmd
        # Fallback for jobs that are not group leaders (host without setsid)
        assert 'kill -TERM "$p"' in cmd
        # Guard against a corrupt/partial pid: -0/-1 would broadcast to every process
        assert '[ "$p" -gt 1 ]' in cmd
        assert cmd.endswith("fi")

    def test_windows_kill_terminates_process_tree(self):
        """Windows kill terminates the process and its child tree via taskkill /T."""
        cmd = build_windows_kill_command("C:\\temp\\pid")
        assert "powershell.exe" in cmd
        assert "-EncodedCommand" in cmd
        encoded_script = cmd.split("-EncodedCommand ")[1]
        decoded_script = base64.b64decode(encoded_script).decode("utf-16-le")
        assert "taskkill" in decoded_script
        assert "/T" in decoded_script  # tree kill (process + children)
        # $PID is a read-only automatic variable in PowerShell; must not be assigned
        assert "$procId" in decoded_script
        assert "$pid =" not in decoded_script


@pytest.mark.skipif(
    os.name != "posix" or shutil.which("setsid") is None or shutil.which("bash") is None,
    reason="needs a POSIX host with bash and setsid to exercise process-group teardown",
)
class TestPosixKillBehaviour:
    """End-to-end check that on_kill tears down the whole job tree, not just the wrapper.

    Regression test for the orphaned-process bug: killing only the recorded PID left the
    user command (and its children) running, so the exit_code file was never written and
    the trigger timed out. The job runs in its own process group and self-reports that
    group's PGID, so the kill signals the whole group even when setsid(1) forks.
    """

    def _marker(self, tag: str) -> str:
        # Unique per (test, xdist worker): CI runs these with ``-n auto`` (default
        # ``load`` distribution), so sibling tests can execute concurrently in separate
        # workers against the same OS process table. A shared literal would let one
        # test's ``pgrep -f`` / ``pkill -f`` match or kill another's job. os.getpid()
        # differs per worker; the tag differs per test.
        return f"sleep 9{tag}{os.getpid()}"

    @staticmethod
    def _group_alive(pgid: int) -> bool:
        # pgrep -g matches by process-group id; rc 0 => at least one member alive.
        return subprocess.run(["pgrep", "-g", str(pgid)], capture_output=True, check=False).returncode == 0

    @staticmethod
    def _job_running(marker: str) -> bool:
        return subprocess.run(["pgrep", "-f", marker], capture_output=True, check=False).returncode == 0

    @staticmethod
    def _pgid_of(pid: str) -> str:
        return subprocess.run(
            ["ps", "-o", "pgid=", "-p", pid], capture_output=True, text=True, check=False
        ).stdout.strip()

    def _await_recorded_pid(self, paths) -> int:
        # The job writes its pid asynchronously (the launcher does not wait), so poll.
        pid_path = Path(paths.pid_file)
        deadline = time.monotonic() + 5
        pid_text = ""
        while time.monotonic() < deadline:
            pid_text = pid_path.read_text().strip() if pid_path.exists() else ""
            if pid_text:
                break
            time.sleep(0.02)
        assert pid_text, "job never wrote its pid file"
        return int(pid_text)

    @staticmethod
    def _run_bash_mc_under_pty(script: str, marker: bytes, timeout: float = 8.0) -> None:
        """Run ``bash -mc script`` under a pty we own so job control genuinely activates
        (bash silently disables ``-m`` without a controlling terminal). Read until the
        marker, NOT to EOF: the detached job inherits the pty slave as its stdin, so EOF
        would not arrive until the job itself exits (the full sleep runtime)."""
        pid, fd = pty.fork()
        if pid == 0:
            try:
                os.execvp("bash", ["bash", "-mc", script])
            except OSError:
                os._exit(127)  # never fall through as a duplicate pytest process
        try:
            deadline = time.monotonic() + timeout
            buf = b""
            while time.monotonic() < deadline:
                r, _, _ = select.select([fd], [], [], 0.2)
                if fd in r:
                    try:
                        chunk = os.read(fd, 4096)
                    except OSError:
                        break
                    if not chunk:
                        break
                    buf += chunk
                    if marker in buf:
                        break
        finally:
            os.close(fd)  # hangs up the pty; the launcher (not the detached job) exits
            with contextlib.suppress(ChildProcessError):
                os.waitpid(pid, 0)  # reap the launcher so it does not linger as a zombie

    def _assert_kill_tears_down(self, paths, pgid: int, marker: str) -> None:
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not self._group_alive(pgid):
                time.sleep(0.02)
            assert self._group_alive(pgid), "job group should be running before kill"
            subprocess.run(["bash", "-c", build_posix_kill_command(paths.pid_file)], check=True)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and self._job_running(marker):
                time.sleep(0.05)
            assert not self._job_running(marker), "kill left the job running (orphaned)"
        finally:
            subprocess.run(["bash", "-c", f"kill -9 -{pgid} 2>/dev/null || true"], check=False)
            subprocess.run(["bash", "-c", f"pkill -9 -f '{marker}' 2>/dev/null || true"], check=False)

    def test_kill_terminates_whole_job_tree(self, tmp_path):
        """Default path (no job control): the job self-reports its PGID and the kill
        signals the whole group."""
        marker = self._marker("1")
        paths = RemoteJobPaths(job_id="killtree", remote_os="posix", base_dir=str(tmp_path / "jobs"))
        wrapper = build_posix_wrapper_command(marker, paths)
        subprocess.run(["bash", "-c", wrapper], check=True, capture_output=True, text=True)
        pgid = self._await_recorded_pid(paths)
        self._assert_kill_tears_down(paths, pgid, marker)

    def test_kill_terminates_whole_job_tree_under_job_control(self, tmp_path):
        """With job control on, setsid(1) forks and the launcher's ``$!`` would name the
        short-lived setsid parent, not the job -- the condition the old wrapper orphaned
        the job under. Force it deterministically via a real controlling terminal and
        assert the recorded pid IS the job's true PGID and the kill reaches the job."""
        marker = self._marker("2")
        paths = RemoteJobPaths(job_id="killtree_jc", remote_os="posix", base_dir=str(tmp_path / "jobs"))
        wrapper = build_posix_wrapper_command(marker, paths)
        self._run_bash_mc_under_pty(wrapper + "\necho SUBMIT_DONE\n", b"SUBMIT_DONE")
        pgid = self._await_recorded_pid(paths)

        job_pids = subprocess.run(
            ["pgrep", "-f", marker], capture_output=True, text=True, check=False
        ).stdout.split()
        assert job_pids, "job never started"
        true_pgid = self._pgid_of(job_pids[0])
        # Core regression assertion: recorded pid == the job's real PGID. Under the old
        # $!-based wrapper this differs (setsid forked) and on_kill orphans the job.
        assert str(pgid) == true_pgid, f"recorded pid {pgid} is not the job PGID {true_pgid}"
        self._assert_kill_tears_down(paths, pgid, marker)


class TestCleanupCommands:
    def test_posix_cleanup(self):
        """Test POSIX cleanup command."""
        cmd = build_posix_cleanup_command("/tmp/airflow-ssh-jobs/job_123")
        assert "rm -rf" in cmd
        assert "/tmp/airflow-ssh-jobs/job_123" in cmd

    def test_windows_cleanup(self):
        """Test Windows cleanup command."""
        cmd = build_windows_cleanup_command("$env:TEMP\\airflow-ssh-jobs\\job_123")
        assert "powershell.exe" in cmd
        assert "-EncodedCommand" in cmd
        encoded_script = cmd.split("-EncodedCommand ")[1]
        decoded_script = base64.b64decode(encoded_script).decode("utf-16-le")
        assert "Remove-Item" in decoded_script
        assert "-Recurse" in decoded_script

    def test_posix_cleanup_rejects_invalid_path(self):
        """Test POSIX cleanup rejects paths outside expected base directory."""
        with pytest.raises(ValueError, match="Invalid job directory"):
            build_posix_cleanup_command("/tmp/other_dir")

    def test_windows_cleanup_rejects_invalid_path(self):
        """Test Windows cleanup rejects paths outside expected base directory."""
        with pytest.raises(ValueError, match="Invalid job directory"):
            build_windows_cleanup_command("C:\\temp\\other_dir")
