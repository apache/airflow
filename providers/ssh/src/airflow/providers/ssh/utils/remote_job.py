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
"""Utilities for SSH remote job execution."""

from __future__ import annotations

import base64
import re
import secrets
import string
from dataclasses import dataclass
from typing import Literal

POSIX_DEFAULT_BASE_DIR = "/tmp/airflow-ssh-jobs"
WINDOWS_DEFAULT_BASE_DIR = "$env:TEMP\\airflow-ssh-jobs"


def _validate_job_dir(job_dir: str, remote_os: Literal["posix", "windows"]) -> None:
    """
    Validate that job_dir is under the expected base directory.

    :param job_dir: The job directory path to validate
    :param remote_os: Operating system type
    :raises ValueError: If job_dir doesn't start with the expected base path
    """
    if remote_os == "posix":
        expected_prefix = POSIX_DEFAULT_BASE_DIR + "/"
    else:
        expected_prefix = WINDOWS_DEFAULT_BASE_DIR + "\\"

    if not job_dir.startswith(expected_prefix):
        raise ValueError(
            f"Invalid job directory '{job_dir}'. Expected path under '{expected_prefix[:-1]}' for safety."
        )


def _validate_env_var_name(name: str) -> None:
    """
    Validate environment variable name for security.

    :param name: Environment variable name
    :raises ValueError: If name contains dangerous characters
    """
    if not name:
        raise ValueError("Environment variable name cannot be empty")

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(
            f"Invalid environment variable name '{name}'. "
            "Only alphanumeric characters and underscores are allowed, "
            "and the name must start with a letter or underscore."
        )


def generate_job_id(
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number: int,
    suffix_length: int = 8,
) -> str:
    """
    Generate a unique job ID for remote execution.

    Creates a deterministic identifier from the task context with a random suffix
    to ensure uniqueness across retries and potential race conditions.

    :param dag_id: The DAG identifier
    :param task_id: The task identifier
    :param run_id: The run identifier
    :param try_number: The attempt number
    :param suffix_length: Length of random suffix (default 8)
    :return: Sanitized job ID string
    """

    def sanitize(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9]", "_", value)[:50]

    sanitized_dag = sanitize(dag_id)
    sanitized_task = sanitize(task_id)
    sanitized_run = sanitize(run_id)

    alphabet = string.ascii_lowercase + string.digits
    suffix = "".join(secrets.choice(alphabet) for _ in range(suffix_length))

    return f"af_{sanitized_dag}_{sanitized_task}_{sanitized_run}_try{try_number}_{suffix}"


@dataclass
class RemoteJobPaths:
    """Paths for remote job artifacts on the target system."""

    job_id: str
    remote_os: Literal["posix", "windows"]
    base_dir: str | None = None

    def __post_init__(self):
        if self.base_dir is None:
            if self.remote_os == "posix":
                self.base_dir = POSIX_DEFAULT_BASE_DIR
            else:
                self.base_dir = WINDOWS_DEFAULT_BASE_DIR

    @property
    def sep(self) -> str:
        """Path separator for the remote OS."""
        return "\\" if self.remote_os == "windows" else "/"

    @property
    def job_dir(self) -> str:
        """Directory containing all job artifacts."""
        return f"{self.base_dir}{self.sep}{self.job_id}"

    @property
    def log_file(self) -> str:
        """Path to stdout/stderr log file."""
        return f"{self.job_dir}{self.sep}stdout.log"

    @property
    def exit_code_file(self) -> str:
        """Path to exit code file (written on completion)."""
        return f"{self.job_dir}{self.sep}exit_code"

    @property
    def exit_code_tmp_file(self) -> str:
        """Temporary exit code file (for atomic write)."""
        return f"{self.job_dir}{self.sep}exit_code.tmp"

    @property
    def pid_file(self) -> str:
        """Path to PID file for the background process."""
        return f"{self.job_dir}{self.sep}pid"

    @property
    def status_file(self) -> str:
        """Path to optional status file for progress updates."""
        return f"{self.job_dir}{self.sep}status"


def build_posix_wrapper_command(
    command: str,
    paths: RemoteJobPaths,
    environment: dict[str, str] | None = None,
) -> str:
    """
    Build a POSIX shell wrapper that runs the command detached via nohup.

    The wrapper:
    - Creates the job directory
    - Starts the command in the background with nohup
    - Redirects stdout/stderr to the log file
    - Writes the exit code atomically on completion
    - Writes the PID for potential cancellation

    :param command: The command to execute
    :param paths: RemoteJobPaths instance with all paths
    :param environment: Optional environment variables to set
    :return: Shell command string to submit via SSH
    """
    env_exports = ""
    if environment:
        for key, value in environment.items():
            _validate_env_var_name(key)
            escaped_value = value.replace("'", "'\"'\"'")
            env_exports += f"export {key}='{escaped_value}'\n"

    escaped_command = command.replace("'", "'\"'\"'")

    wrapper = f"""set -euo pipefail
job_dir='{paths.job_dir}'
log_file='{paths.log_file}'
exit_code_file='{paths.exit_code_file}'
exit_code_tmp='{paths.exit_code_tmp_file}'
pid_file='{paths.pid_file}'
status_file='{paths.status_file}'

mkdir -p "$job_dir"
: > "$log_file"

nohup bash -c '
set +e
export LOG_FILE="'"$log_file"'"
export STATUS_FILE="'"$status_file"'"
{env_exports}{escaped_command} >>"'"$log_file"'" 2>&1
ec=$?
echo -n "$ec" > "'"$exit_code_tmp"'"
mv "'"$exit_code_tmp"'" "'"$exit_code_file"'"
exit 0
' >/dev/null 2>&1 &

echo -n $! > "$pid_file"
echo "{paths.job_id}"
"""
    return wrapper


def build_windows_wrapper_command(
    command: str,
    paths: RemoteJobPaths,
    environment: dict[str, str] | None = None,
) -> str:
    """
    Build a PowerShell wrapper that runs the command detached via Start-Process.

    The wrapper:
    - Creates the job directory
    - Starts the command in a new detached PowerShell process
    - Redirects stdout/stderr to the log file
    - Writes the exit code atomically on completion
    - Writes the PID for potential cancellation

    :param command: The command to execute (PowerShell script path or command)
    :param paths: RemoteJobPaths instance with all paths
    :param environment: Optional environment variables to set
    :return: PowerShell command string to submit via SSH
    """
    env_setup = ""
    if environment:
        for key, value in environment.items():
            _validate_env_var_name(key)
            escaped_value = value.replace("'", "''")
            env_setup += f"$env:{key} = '{escaped_value}'; "

    def ps_escape(s: str) -> str:
        return s.replace("'", "''")

    job_dir = ps_escape(paths.job_dir)
    log_file = ps_escape(paths.log_file)
    exit_code_file = ps_escape(paths.exit_code_file)
    exit_code_tmp = ps_escape(paths.exit_code_tmp_file)
    pid_file = ps_escape(paths.pid_file)
    status_file = ps_escape(paths.status_file)
    escaped_command = ps_escape(command)
    job_id = ps_escape(paths.job_id)

    child_script = f"""$ErrorActionPreference = 'Continue'
$env:LOG_FILE = '{log_file}'
$env:STATUS_FILE = '{status_file}'
{env_setup}
{escaped_command}
$ec = $LASTEXITCODE
if ($null -eq $ec) {{ $ec = 0 }}
Set-Content -NoNewline -Path '{exit_code_tmp}' -Value $ec
Move-Item -Force -Path '{exit_code_tmp}' -Destination '{exit_code_file}'
"""
    child_script_bytes = child_script.encode("utf-16-le")
    encoded_script = base64.b64encode(child_script_bytes).decode("ascii")

    wrapper = f"""$jobDir = '{job_dir}'
New-Item -ItemType Directory -Force -Path $jobDir | Out-Null
$log = '{log_file}'
'' | Set-Content -Path $log

$p = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-NonInteractive', '-EncodedCommand', '{encoded_script}') -RedirectStandardOutput $log -RedirectStandardError $log -PassThru -WindowStyle Hidden
Set-Content -NoNewline -Path '{pid_file}' -Value $p.Id
Write-Output '{job_id}'
"""
    wrapper_bytes = wrapper.encode("utf-16-le")
    encoded_wrapper = base64.b64encode(wrapper_bytes).decode("ascii")

    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_wrapper}"


def build_posix_log_tail_command(log_file: str, offset: int, max_bytes: int) -> str:
    """
    Build a POSIX command to read log bytes from offset.

    :param log_file: Path to the log file
    :param offset: Byte offset to start reading from
    :param max_bytes: Maximum bytes to read
    :return: Shell command that outputs the log chunk
    """
    # tail -c +N is 1-indexed, so offset 0 means start at byte 1
    tail_offset = offset + 1
    return f"tail -c +{tail_offset} '{log_file}' 2>/dev/null | head -c {max_bytes} || true"


def build_windows_log_tail_command(log_file: str, offset: int, max_bytes: int) -> str:
    """
    Build a PowerShell command to read log bytes from offset.

    :param log_file: Path to the log file
    :param offset: Byte offset to start reading from
    :param max_bytes: Maximum bytes to read
    :return: PowerShell command that outputs the log chunk
    """
    escaped_path = log_file.replace("'", "''")
    script = f"""$path = '{escaped_path}'
if (Test-Path $path) {{
  try {{
    $fs = [System.IO.File]::Open($path, 'Open', 'Read', 'ReadWrite')
    $fs.Seek({offset}, [System.IO.SeekOrigin]::Begin) | Out-Null
    $buf = New-Object byte[] {max_bytes}
    $n = $fs.Read($buf, 0, $buf.Length)
    $fs.Close()
    if ($n -gt 0) {{
      [System.Text.Encoding]::UTF8.GetString($buf, 0, $n)
    }}
  }} catch {{}}
}}"""
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"


def build_posix_file_size_command(file_path: str) -> str:
    """
    Build a POSIX command to get file size in bytes.

    :param file_path: Path to the file
    :return: Shell command that outputs the file size
    """
    return f"stat -c%s '{file_path}' 2>/dev/null || stat -f%z '{file_path}' 2>/dev/null || echo 0"


def build_windows_file_size_command(file_path: str) -> str:
    """
    Build a PowerShell command to get file size in bytes.

    :param file_path: Path to the file
    :return: PowerShell command that outputs the file size
    """
    escaped_path = file_path.replace("'", "''")
    script = f"""$path = '{escaped_path}'
if (Test-Path $path) {{
  (Get-Item $path).Length
}} else {{
  0
}}"""
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"


def build_posix_completion_check_command(exit_code_file: str) -> str:
    """
    Build a POSIX command to check if job completed and get exit code.

    :param exit_code_file: Path to the exit code file
    :return: Shell command that outputs exit code if done, empty otherwise
    """
    return f"test -s '{exit_code_file}' && cat '{exit_code_file}' || true"


def build_windows_completion_check_command(exit_code_file: str) -> str:
    """
    Build a PowerShell command to check if job completed and get exit code.

    :param exit_code_file: Path to the exit code file
    :return: PowerShell command that outputs exit code if done, empty otherwise
    """
    escaped_path = exit_code_file.replace("'", "''")
    script = f"""$path = '{escaped_path}'
if (Test-Path $path) {{
  $txt = Get-Content -Raw -Path $path
  if ($txt -match '^[0-9]+$') {{ $txt.Trim() }}
}}"""
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"


def build_posix_kill_command(pid_file: str) -> str:
    """
    Build a POSIX command to kill the remote process.

    :param pid_file: Path to the PID file
    :return: Shell command to kill the process
    """
    return f"test -f '{pid_file}' && kill $(cat '{pid_file}') 2>/dev/null || true"


def build_windows_kill_command(pid_file: str) -> str:
    """
    Build a PowerShell command to kill the remote process.

    :param pid_file: Path to the PID file
    :return: PowerShell command to kill the process
    """
    escaped_path = pid_file.replace("'", "''")
    script = f"""$path = '{escaped_path}'
if (Test-Path $path) {{
  $pid = Get-Content $path
  Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
}}"""
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"


def build_posix_cleanup_command(job_dir: str) -> str:
    """
    Build a POSIX command to clean up the job directory.

    :param job_dir: Path to the job directory
    :return: Shell command to remove the directory
    :raises ValueError: If job_dir is not under the expected base directory
    """
    _validate_job_dir(job_dir, "posix")
    return f"rm -rf '{job_dir}'"


def build_windows_cleanup_command(job_dir: str) -> str:
    """
    Build a PowerShell command to clean up the job directory.

    :param job_dir: Path to the job directory
    :return: PowerShell command to remove the directory
    :raises ValueError: If job_dir is not under the expected base directory
    """
    _validate_job_dir(job_dir, "windows")
    escaped_path = job_dir.replace("'", "''")
    script = f"Remove-Item -Recurse -Force -Path '{escaped_path}' -ErrorAction SilentlyContinue"
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"


def build_posix_os_detection_command() -> str:
    """
    Build a command to detect if the remote system is POSIX-compliant.

    Returns the OS name (Linux, Darwin, FreeBSD, etc.) or UNKNOWN.
    """
    return "uname -s 2>/dev/null || echo UNKNOWN"


def build_windows_os_detection_command() -> str:
    """Build a command to detect if the remote system is Windows."""
    script = '$PSVersionTable.PSVersion.Major; if ($?) { "WINDOWS" }'
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"
