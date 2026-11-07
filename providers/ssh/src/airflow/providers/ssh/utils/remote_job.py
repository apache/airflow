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
import shlex
import string
from dataclasses import dataclass
from typing import Literal

POSIX_DEFAULT_BASE_DIR = "/tmp/airflow-ssh-jobs"
WINDOWS_DEFAULT_BASE_DIR = "$env:TEMP\\airflow-ssh-jobs"

# PowerShell ends a single-quoted string at any of these, not only the ASCII
# apostrophe; a doubled one stands for itself.
_PS_SINGLE_QUOTES = re.compile("(['\u2018\u2019\u201a\u201b])")
_PS_ENV_PREFIX = re.compile(r"\$env:([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)


def _ps_quote(value: str) -> str:
    """Quote ``value`` as a PowerShell single-quoted string, which is never interpolated."""
    return "'" + _PS_SINGLE_QUOTES.sub(r"\1\1", value) + "'"


def _ps_path(path: str) -> str:
    """
    Return a PowerShell expression that evaluates to ``path``.

    A leading ``$env:NAME``, as in the default base directory, is expanded on the remote
    host. The rest of the path is literal, so a ``$``, backtick or quote in
    ``remote_base_dir`` stays part of the directory name. Pass the result to
    ``-LiteralPath``, since ``-Path`` would read ``[`` and ``]`` as wildcards.
    """
    if match := _PS_ENV_PREFIX.match(path):
        return f"($env:{match.group(1)} + {_ps_quote(path[match.end() :])})"
    return _ps_quote(path)


def _validate_job_dir(
    job_dir: str, remote_os: Literal["posix", "windows"], base_dir: str | None = None
) -> None:
    """
    Validate that job_dir is under the expected base directory.

    :param job_dir: The job directory path to validate
    :param remote_os: Operating system type
    :param base_dir: Base directory the job was created under (e.g. the operator's
        ``remote_base_dir``). Falls back to the OS-specific default base directory.
    :raises ValueError: If job_dir doesn't start with the expected base path
    """
    if remote_os == "posix":
        expected_prefix = (base_dir or POSIX_DEFAULT_BASE_DIR) + "/"
    else:
        expected_prefix = (base_dir or WINDOWS_DEFAULT_BASE_DIR) + "\\"

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
    Build a POSIX shell wrapper that runs the command detached.

    The wrapper:

    - Creates the job directory
    - Starts the command detached in its own session via ``setsid`` (falling back to
      ``nohup`` when ``setsid`` is unavailable)
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
            env_exports += f"export {key}={shlex.quote(value)}\n"

    # The exports and the command are spliced into the single-quoted ``job_script`` literal
    # below, so escape them for it once more on top of the per-value quoting.
    escaped_job_body = (env_exports + command).replace("'", "'\"'\"'")

    # Launch detached under ``setsid`` so the job is its own session/process-group
    # leader, letting cancellation signal the whole job tree instead of orphaning the
    # user command. We do NOT rely on the launcher's ``$!`` to identify that group:
    # ``setsid(1)`` forks internally when the process about to exec into it is already
    # a process-group leader (``setsid(2)`` cannot create a new session from a group
    # leader), which happens whenever job control is on in the launching shell. When
    # it forks, ``$!`` is the short-lived setsid parent, not the job's real PGID, and
    # cancellation signals a dead group. Instead the job script reports its OWN pid:
    # immediately after ``setsid(2)`` POSIX guarantees ``pid == pgid == sid`` for the
    # caller, and that identity survives the following exec, so ``$$`` inside the job
    # script is always the true PGID regardless of whether setsid forked to get there.
    # The pid file is read only when cancelling (:func:`build_posix_kill_command`),
    # which happens long after submission, so the job's asynchronous write lands well
    # before any reader and the launcher does not wait for it (recording the launcher's
    # ``$!`` would just reintroduce the wrong-pid bug on the fork path). Without
    # ``setsid`` (some macOS/BSD hosts) ``$$`` is just the job's own PID and
    # cancellation degrades to the previous single-process behaviour.
    wrapper = f"""set -euo pipefail
job_dir={shlex.quote(paths.job_dir)}
log_file={shlex.quote(paths.log_file)}
exit_code_file={shlex.quote(paths.exit_code_file)}
exit_code_tmp={shlex.quote(paths.exit_code_tmp_file)}
pid_file={shlex.quote(paths.pid_file)}
status_file={shlex.quote(paths.status_file)}

mkdir -p "$job_dir"
: > "$log_file"

job_script='
set +e
echo -n "$$" > "'"$pid_file"'"
export LOG_FILE="'"$log_file"'"
export STATUS_FILE="'"$status_file"'"
{escaped_job_body} >>"'"$log_file"'" 2>&1
ec=$?
echo -n "$ec" > "'"$exit_code_tmp"'"
mv "'"$exit_code_tmp"'" "'"$exit_code_file"'"
exit 0
'

# Redirect stdin from /dev/null too, not just stdout/stderr: a fresh setsid session
# leader that keeps the launching terminal on any fd re-acquires it as its controlling
# terminal, so a hangup (SSH session with a PTY dropping) would SIGHUP the detached job
# and defeat the whole point of running it in its own session.
if command -v setsid >/dev/null 2>&1; then
  setsid bash -c "$job_script" </dev/null >/dev/null 2>&1 &
else
  nohup bash -c "$job_script" </dev/null >/dev/null 2>&1 &
fi
echo {shlex.quote(paths.job_id)}
"""
    return wrapper


def build_windows_wrapper_command(
    command: str,
    paths: RemoteJobPaths,
    environment: dict[str, str] | None = None,
) -> str:
    """
    Build a PowerShell wrapper that runs the command detached via ``Win32_Process.Create``.

    The wrapper:

    - Creates the job directory and writes the command into it as ``cmd.ps1``, next to
      the job script that runs it
    - Starts the job script as a separate process that outlives the SSH session, in the
      SSH session's working directory
    - Appends everything the command writes, on every stream, to the log file as UTF-8
    - Writes the exit code on completion, including when the command calls ``exit``,
      throws, or fails to parse
    - Writes the PID for potential cancellation, and fails the submission if the job
      directory cannot be set up or the process cannot be started

    The command runs as a script, so the host's execution policy applies to it with
    ``-ExecutionPolicy Bypass`` for the process; a Group Policy that only allows signed
    scripts fails the job with the policy error in its log.

    :param command: The command to execute (PowerShell script path or command)
    :param paths: RemoteJobPaths instance with all paths
    :param environment: Optional environment variables to set
    :return: PowerShell command string to submit via SSH
    """
    env_setup = ""
    if environment:
        for key, value in environment.items():
            _validate_env_var_name(key)
            env_setup += f"$env:{key} = {_ps_quote(value)}; "

    sep = paths.sep

    # Use $d and $b for directories to avoid passing long paths multiple times, and write
    # the scripts to disk to avoid double base64 encoding. job.ps1 travels in a verbatim
    # here-string and holds nothing user-supplied but quoted environment values; the command
    # goes to its own cmd.ps1, so `exit N` in it ends only that script, with $LASTEXITCODE N.
    # *>&1 keeps Write-Host and warnings; error records are turned into their message before
    # Out-String formats objects the way a console would. The log writer shares the file for
    # reading and flushes every line, so the trigger can tail it while the job runs.
    # Windows reports a crashed process as a negative exit code; record it unsigned, since
    # the trigger only accepts digits. The writer opens $logPath, not $env:LOG_FILE, so an
    # environment entry of that name cannot redirect the job's log.
    child_script = f"""$ErrorActionPreference = "Continue"
$logPath = "$b{sep}stdout.log"
$env:LOG_FILE = $logPath
$env:STATUS_FILE = "$b{sep}status"
{env_setup}
$log = [System.IO.StreamWriter]::new([System.IO.FileStream]::new($logPath, 'Append', 'Write', 'ReadWrite'), [System.Text.UTF8Encoding]::new($false))
$log.AutoFlush = $true
$ec = 1
try {{
  & "$b{sep}cmd.ps1" *>&1 |
    ForEach-Object {{ if ($_ -is [System.Management.Automation.ErrorRecord]) {{ "$_" }} else {{ $_ }} }} |
    Out-String -Stream |
    ForEach-Object {{ $log.WriteLine($_) }}
  $ec = if ($null -eq $LASTEXITCODE) {{ 0 }} else {{ $LASTEXITCODE }}
}} catch {{
  $log.WriteLine("$_")
}} finally {{
  $log.Close()
  if ($ec -lt 0) {{ $ec += 4294967296 }}
  Set-Content -NoNewline -LiteralPath "$b{sep}exit_code.tmp" -Value $ec
  Move-Item -Force -LiteralPath "$b{sep}exit_code.tmp" -Destination "$b{sep}exit_code"
}}
"""

    # Launch as Win32_Process so that it doesn't die when SSH exits. job.ps1 is read and run
    # as a script block rather than with -File, so execution policy never blocks the job's
    # own bookkeeping, only the user's cmd.ps1. The launch quotes the already-resolved $d, so
    # the child does not re-expand $env:NAME in its own environment. Create reports failure
    # in ReturnValue instead of throwing.
    wrapper = f"""$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
trap {{ [Console]::Error.WriteLine("$_"); exit 1 }}
$d = {_ps_path(paths.job_dir)}
New-Item -ItemType Directory -Force -Path $d | Out-Null
[System.IO.File]::WriteAllText("$d{sep}cmd.ps1", {_ps_quote(command)}, [System.Text.UTF8Encoding]::new($true))
@'
{child_script}'@ | Set-Content -LiteralPath "$d{sep}job.ps1" -Encoding UTF8
New-Item -ItemType File -Force -Path "$d{sep}stdout.log" | Out-Null

$q = "'" + ($d -replace "{_PS_SINGLE_QUOTES.pattern}", '$1$1') + "'"
$launch = 'powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "$b = ' + $q + '; . ([scriptblock]::Create([IO.File]::ReadAllText($b + ''{sep}job.ps1'')))"'
$p = ([wmiclass]'Win32_Process').Create($launch, $PWD.ProviderPath, $null)
if ($p.ReturnValue -ne 0) {{
  [Console]::Error.WriteLine("Win32_Process.Create failed with return value $($p.ReturnValue)")
  exit 1
}}
Set-Content -NoNewline -LiteralPath "$d{sep}pid" -Value $p.ProcessId
Write-Output {_ps_quote(paths.job_id)}
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
    return f"tail -c +{tail_offset} {shlex.quote(log_file)} 2>/dev/null | head -c {max_bytes} || true"


def build_windows_log_tail_command(log_file: str, offset: int, max_bytes: int) -> str:
    """
    Build a PowerShell command to read log bytes from offset.

    The chunk is written to stdout as UTF-8 bytes. Printing it as a PowerShell string would
    re-encode it in the SSH session's console code page, which the trigger's strict UTF-8
    decoding rejects for any non-ASCII character.

    :param log_file: Path to the log file
    :param offset: Byte offset to start reading from
    :param max_bytes: Maximum bytes to read
    :return: PowerShell command that outputs the log chunk
    """
    script = f"""$path = {_ps_path(log_file)}
if (Test-Path -LiteralPath $path) {{
  try {{
    $fs = [System.IO.File]::Open($path, 'Open', 'Read', 'ReadWrite')
    $fs.Seek({offset}, [System.IO.SeekOrigin]::Begin) | Out-Null
    $buf = New-Object byte[] {max_bytes}
    $n = $fs.Read($buf, 0, $buf.Length)
    $fs.Close()
    if ($n -gt 0) {{
      $bytes = [System.Text.Encoding]::UTF8.GetBytes([System.Text.Encoding]::UTF8.GetString($buf, 0, $n))
      $out = [Console]::OpenStandardOutput()
      $out.Write($bytes, 0, $bytes.Length)
      $out.Flush()
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
    quoted = shlex.quote(file_path)
    return f"stat -c%s {quoted} 2>/dev/null || stat -f%z {quoted} 2>/dev/null || echo 0"


def build_windows_file_size_command(file_path: str) -> str:
    """
    Build a PowerShell command to get file size in bytes.

    :param file_path: Path to the file
    :return: PowerShell command that outputs the file size
    """
    script = f"""$path = {_ps_path(file_path)}
if (Test-Path -LiteralPath $path) {{
  (Get-Item -LiteralPath $path).Length
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
    quoted = shlex.quote(exit_code_file)
    return f"test -s {quoted} && cat {quoted} || true"


def build_windows_completion_check_command(exit_code_file: str) -> str:
    """
    Build a PowerShell command to check if job completed and get exit code.

    :param exit_code_file: Path to the exit code file
    :return: PowerShell command that outputs exit code if done, empty otherwise
    """
    script = f"""$path = {_ps_path(exit_code_file)}
if (Test-Path -LiteralPath $path) {{
  $txt = Get-Content -Raw -LiteralPath $path
  if ($txt -match '^[0-9]+$') {{ $txt.Trim() }}
}}"""
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"


def build_posix_kill_command(pid_file: str) -> str:
    """
    Build a POSIX command to kill the remote process.

    Signals the whole process group first (the negative-PID form ``kill -<pgid>``) so
    the user command and anything it spawned are terminated together, not just the
    wrapper. The recorded PID is the job's session/group leader when it was launched
    under ``setsid`` (see :func:`build_posix_wrapper_command`); if the job is not a
    group leader (host without ``setsid``), the group signal is a no-op and we fall
    back to killing the single PID, matching the previous behaviour.

    The pid value is validated as an integer ``> 1`` before being negated: a corrupt
    or partial pid of ``0``/``1`` would otherwise turn ``kill -<pid>`` into a broadcast
    to every process the SSH account can signal. Best-effort: the command never fails
    the SSH call.

    :param pid_file: Path to the PID file
    :return: Shell command to kill the process
    """
    quoted = shlex.quote(pid_file)
    return (
        f"if test -f {quoted}; then "
        f'p="$(cat {quoted})"; '
        f'if [ "$p" -gt 1 ] 2>/dev/null; then '
        f'kill -TERM -"$p" 2>/dev/null || kill -TERM "$p" 2>/dev/null || true; '
        "fi; fi"
    )


def build_windows_kill_command(pid_file: str) -> str:
    """
    Build a PowerShell command to kill the remote process.

    Uses ``taskkill /T`` to terminate the recorded process *and its child
    processes* (the Windows equivalent of a process-group kill), so the user
    command launched by the detached wrapper is not left orphaned. ``$procId`` is
    used instead of ``$pid`` because ``$PID`` is a read-only automatic variable in
    PowerShell.

    :param pid_file: Path to the PID file
    :return: PowerShell command to kill the process
    """
    script = f"""$path = {_ps_path(pid_file)}
if (Test-Path -LiteralPath $path) {{
  $procId = Get-Content -LiteralPath $path
  & taskkill.exe /PID $procId /T /F 2>$null
}}"""
    script_bytes = script.encode("utf-16-le")
    encoded_script = base64.b64encode(script_bytes).decode("ascii")
    return f"powershell.exe -NoProfile -NonInteractive -EncodedCommand {encoded_script}"


def build_posix_cleanup_command(job_dir: str, base_dir: str | None = None) -> str:
    """
    Build a POSIX command to clean up the job directory.

    :param job_dir: Path to the job directory
    :param base_dir: Base directory the job was created under. Defaults to the
        OS-specific default base directory.
    :return: Shell command to remove the directory
    :raises ValueError: If job_dir is not under the expected base directory
    """
    _validate_job_dir(job_dir, "posix", base_dir)
    return f"rm -rf {shlex.quote(job_dir)}"


def build_windows_cleanup_command(job_dir: str, base_dir: str | None = None) -> str:
    """
    Build a PowerShell command to clean up the job directory.

    :param job_dir: Path to the job directory
    :param base_dir: Base directory the job was created under. Defaults to the
        OS-specific default base directory.
    :return: PowerShell command to remove the directory
    :raises ValueError: If job_dir is not under the expected base directory
    """
    _validate_job_dir(job_dir, "windows", base_dir)
    script = f"Remove-Item -Recurse -Force -LiteralPath {_ps_path(job_dir)} -ErrorAction SilentlyContinue"
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
