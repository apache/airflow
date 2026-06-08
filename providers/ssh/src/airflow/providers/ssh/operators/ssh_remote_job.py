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
"""SSH Remote Job Operator for deferrable remote command execution."""

from __future__ import annotations

import time
import warnings
from collections.abc import Container, Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.compat.sdk import AirflowException, AirflowSkipException, BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.triggers.ssh_remote_job import SSHRemoteJobTrigger
from airflow.providers.ssh.utils.remote_job import (
    RemoteJobPaths,
    build_posix_cleanup_command,
    build_posix_kill_command,
    build_posix_os_detection_command,
    build_posix_wrapper_command,
    build_windows_cleanup_command,
    build_windows_kill_command,
    build_windows_os_detection_command,
    build_windows_wrapper_command,
    generate_job_id,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SSHRemoteJobOperator(BaseOperator):
    r"""
    Execute a command on a remote host via SSH with deferrable monitoring.

    This operator submits a job to run detached on the remote host, then
    uses a trigger to asynchronously monitor the job status and stream logs.
    This approach is resilient to network interruptions as the remote job
    continues running independently of the SSH connection.

    The remote job is wrapped to:
    - Run detached from the SSH session (via nohup on POSIX, Start-Process on Windows)
    - Redirect stdout/stderr to a log file
    - Write the exit code to a file on completion

    :param ssh_conn_id: SSH connection ID from Airflow Connections
    :param command: Command to execute on the remote host (templated)
    :param remote_host: Override the host from the connection (templated)
    :param environment: Environment variables to set for the command (templated)
    :param remote_base_dir: Base directory for job artifacts (templated).
        Defaults to /tmp/airflow-ssh-jobs on POSIX, C:\\Windows\\Temp\\airflow-ssh-jobs on Windows
    :param poll_interval: Seconds between status polls (default: 5)
    :param log_chunk_size: Max bytes to read per poll (default: 65536)
    :param timeout: Hard timeout in seconds for the entire operation
    :param cleanup: When to clean up remote job directory:
        'never', 'on_success', or 'always' (default: 'never')
    :param remote_os: Remote operating system: 'auto', 'posix', or 'windows' (default: 'auto')
    :param skip_on_exit_code: Exit codes that should skip the task instead of failing
    :param conn_timeout: SSH connection timeout in seconds
    :param banner_timeout: Timeout waiting for SSH banner in seconds
    :param conn_retry_attempts: How many times to attempt the initial SSH connection for
        submission/cleanup before failing (default 5). Helps when many mapped tasks hit the
        same host at once and ``sshd`` transiently refuses connections (``MaxStartups``).
    :param cleanup_retries: How many times to attempt remote directory cleanup before
        giving up and leaving the directory in place (default 3). Prevents a transient SSH
        failure during cleanup from orphaning the job directory on the remote host.
    :param command_timeout: Per-command timeout in seconds for the trigger's status/log polls
        (default 30.0).
    :param max_reconnect_attempts: Consecutive connection failures the trigger tolerates (with
        backoff) before failing the task while monitoring the remote job (default 5).

    .. note::
        A large ``expand()`` fan-out opens many SSH connections against one host. The remote
        ``sshd`` throttles concurrent unauthenticated connections via ``MaxStartups`` (default
        ``10:30:100``); when exceeded it drops connections, surfacing as
        ``paramiko ... Error reading SSH protocol banner``. For high fan-out, raise ``MaxStartups``
        on the server. The directory ``/tmp/airflow-ssh-jobs`` (POSIX) is only cleaned when
        ``cleanup`` is set and the job reaches completion, so also consider a server-side TTL
        reaper (for example ``systemd-tmpfiles``) for jobs that are killed or time out.
    """

    template_fields: Sequence[str] = ("command", "environment", "remote_host", "remote_base_dir")
    template_ext: Sequence[str] = (
        ".sh",
        ".bash",
        ".ps1",
    )
    template_fields_renderers = {
        "command": "bash",
        "environment": "python",
    }
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        ssh_conn_id: str,
        command: str,
        remote_host: str | None = None,
        environment: dict[str, str] | None = None,
        remote_base_dir: str | None = None,
        poll_interval: int = 5,
        log_chunk_size: int = 65536,
        timeout: int | None = None,
        cleanup: Literal["never", "on_success", "always"] = "never",
        remote_os: Literal["auto", "posix", "windows"] = "auto",
        skip_on_exit_code: int | Container[int] | None = None,
        conn_timeout: int | None = None,
        banner_timeout: float = 30.0,
        conn_retry_attempts: int = 5,
        cleanup_retries: int = 3,
        command_timeout: float = 30.0,
        max_reconnect_attempts: int = 5,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.command = command
        self.remote_host = remote_host
        self.environment = environment

        if remote_base_dir is not None:
            self._validate_base_dir(remote_base_dir)
        self.remote_base_dir = remote_base_dir

        self.poll_interval = poll_interval
        self.log_chunk_size = log_chunk_size
        self.timeout = timeout
        self.cleanup = cleanup
        self.remote_os = remote_os
        self.conn_timeout = conn_timeout
        self.banner_timeout = banner_timeout
        self.conn_retry_attempts = conn_retry_attempts
        self.cleanup_retries = max(1, cleanup_retries)
        self.command_timeout = command_timeout
        self.max_reconnect_attempts = max_reconnect_attempts
        self.skip_on_exit_code = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code is not None
            else []
        )

        self._job_id: str | None = None
        self._paths: RemoteJobPaths | None = None
        self._detected_os: Literal["posix", "windows"] | None = None

    @staticmethod
    def _validate_base_dir(path: str) -> None:
        """
        Validate the remote base directory path for security.

        :param path: Path to validate
        :raises ValueError: If path contains dangerous patterns
        """
        if not path:
            raise ValueError("remote_base_dir cannot be empty")

        if ".." in path:
            raise ValueError(f"remote_base_dir cannot contain '..' (path traversal not allowed). Got: {path}")

        if "\x00" in path:
            raise ValueError("remote_base_dir cannot contain null bytes")

        dangerous_patterns = ["/etc", "/bin", "/sbin", "/boot", "C:\\Windows", "C:\\Program Files"]
        for pattern in dangerous_patterns:
            if pattern in path:
                warnings.warn(
                    f"remote_base_dir '{path}' contains potentially sensitive path '{pattern}'. "
                    "Ensure you have appropriate permissions.",
                    UserWarning,
                    stacklevel=3,
                )

    @cached_property
    def ssh_hook(self) -> SSHHook:
        """Create the SSH hook for command submission."""
        return SSHHook(
            ssh_conn_id=self.ssh_conn_id,
            remote_host=self.remote_host or "",
            conn_timeout=self.conn_timeout,
            banner_timeout=self.banner_timeout,
            conn_retry_attempts=self.conn_retry_attempts,
        )

    def _detect_remote_os(self, ssh_client) -> Literal["posix", "windows"]:
        """
        Detect the remote operating system on an already-open SSH connection.

        Uses a two-stage detection:
        1. Try POSIX detection via `uname` (works on Linux, macOS, BSD, Solaris, AIX, etc.)
        2. Try Windows detection via PowerShell
        3. Raise error if both fail

        :param ssh_client: An open paramiko SSH client to reuse (avoids a second handshake).
        """
        if self.remote_os != "auto":
            return self.remote_os

        self.log.info("Auto-detecting remote operating system...")
        try:
            exit_status, stdout, _ = self.ssh_hook.exec_ssh_client_command(
                ssh_client,
                build_posix_os_detection_command(),
                get_pty=False,
                environment=None,
                timeout=10,
            )
            if exit_status == 0 and stdout:
                output = stdout.decode("utf-8", errors="replace").strip().lower()
                posix_systems = [
                    "linux",
                    "darwin",
                    "freebsd",
                    "openbsd",
                    "netbsd",
                    "sunos",
                    "aix",
                    "hp-ux",
                ]
                if any(system in output for system in posix_systems):
                    self.log.info("Detected POSIX system: %s", output)
                    return "posix"
        except Exception as e:
            self.log.debug("POSIX detection failed: %s", e)

        try:
            exit_status, stdout, _ = self.ssh_hook.exec_ssh_client_command(
                ssh_client,
                build_windows_os_detection_command(),
                get_pty=False,
                environment=None,
                timeout=10,
            )
            if exit_status == 0 and stdout:
                output = stdout.decode("utf-8", errors="replace").strip()
                if "WINDOWS" in output.upper():
                    self.log.info("Detected Windows system")
                    return "windows"
        except Exception as e:
            self.log.debug("Windows detection failed: %s", e)

        raise AirflowException(
            "Could not auto-detect remote OS. Please explicitly set remote_os='posix' or 'windows'"
        )

    def execute(self, context: Context) -> None:
        """
        Submit the remote job and defer to the trigger for monitoring.

        :param context: Airflow task context
        """
        if not self.command:
            raise AirflowException("SSH operator error: command not specified.")

        ti = context["ti"]
        self._job_id = generate_job_id(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            try_number=ti.try_number,
        )
        self.log.info("Generated job ID: %s", self._job_id)

        # Reuse a single connection for OS detection (when 'auto') and submission so the
        # operator opens one SSH handshake per task instead of two. Under a large fan-out
        # this halves the connection burst that triggers sshd MaxStartups throttling.
        self.log.info("Connecting to %s", self.ssh_hook.remote_host)
        try:
            ssh_conn = self.ssh_hook.get_conn()
        except Exception:
            self.log.error(
                "Failed to connect to %s to submit the remote job. When many SSH connections reach "
                "the same host at once, the server can start refusing new ones before the handshake "
                "(for example sshd MaxStartups). This is not limited to mapped tasks: parallel DAG "
                "runs or high concurrency can cause it too. Try raising MaxStartups/MaxSessions on "
                "the server, increasing conn_retry_attempts (currently %d), or reducing concurrency "
                "with a pool (or max_active_tis_per_dag for mapped tasks). See the "
                "SSHRemoteJobOperator 'High Fan-out' docs.",
                self.ssh_hook.remote_host,
                self.conn_retry_attempts,
            )
            raise
        with ssh_conn as ssh_client:
            self._detected_os = self._detect_remote_os(ssh_client)
            self.log.info("Remote OS: %s", self._detected_os)

            self._paths = RemoteJobPaths(
                job_id=self._job_id,
                remote_os=self._detected_os,
                base_dir=self.remote_base_dir,
            )

            if self._detected_os == "posix":
                wrapper_cmd = build_posix_wrapper_command(
                    command=self.command,
                    paths=self._paths,
                    environment=self.environment,
                )
            else:
                wrapper_cmd = build_windows_wrapper_command(
                    command=self.command,
                    paths=self._paths,
                    environment=self.environment,
                )

            self.log.info("Submitting remote job to %s", self.ssh_hook.remote_host)
            exit_status, stdout, stderr = self.ssh_hook.exec_ssh_client_command(
                ssh_client,
                wrapper_cmd,
                get_pty=False,
                environment=None,
                timeout=60,
            )

            if exit_status != 0:
                stderr_str = stderr.decode("utf-8", errors="replace") if stderr else ""
                raise AirflowException(
                    f"Failed to submit remote job. Exit code: {exit_status}. Error: {stderr_str}"
                )

            returned_job_id = stdout.decode("utf-8", errors="replace").strip() if stdout else ""
            if returned_job_id != self._job_id:
                self.log.warning("Job ID mismatch. Expected: %s, Got: %s", self._job_id, returned_job_id)

        self.log.info("Remote job submitted successfully. Job ID: %s", self._job_id)
        self.log.info("Job directory: %s", self._paths.job_dir)

        if self.do_xcom_push:
            ti.xcom_push(
                key="ssh_remote_job",
                value={
                    "job_id": self._job_id,
                    "job_dir": self._paths.job_dir,
                    "log_file": self._paths.log_file,
                    "exit_code_file": self._paths.exit_code_file,
                    "pid_file": self._paths.pid_file,
                    "remote_os": self._detected_os,
                },
            )

        self.defer(
            trigger=SSHRemoteJobTrigger(
                ssh_conn_id=self.ssh_conn_id,
                remote_host=self.remote_host,
                job_id=self._job_id,
                job_dir=self._paths.job_dir,
                log_file=self._paths.log_file,
                exit_code_file=self._paths.exit_code_file,
                remote_os=self._detected_os,
                poll_interval=self.poll_interval,
                log_chunk_size=self.log_chunk_size,
                log_offset=0,
                command_timeout=self.command_timeout,
                max_reconnect_attempts=self.max_reconnect_attempts,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout) if self.timeout else None,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Handle trigger events and re-defer if job is still running.

        :param context: Airflow task context
        :param event: Event data from the trigger
        """
        if not event:
            raise AirflowException("Received null event from trigger")

        required_keys = ["job_id", "job_dir", "log_file", "exit_code_file", "remote_os", "done"]
        missing_keys = [key for key in required_keys if key not in event]
        if missing_keys:
            raise AirflowException(
                f"Invalid trigger event: missing required keys {missing_keys}. Event: {event}"
            )

        log_chunk = event.get("log_chunk", "")
        if log_chunk:
            for line in log_chunk.splitlines():
                self.log.info("[remote] %s", line)

        if not event.get("done", False):
            self.log.debug("Job still running, continuing to monitor...")
            self.defer(
                trigger=SSHRemoteJobTrigger(
                    ssh_conn_id=self.ssh_conn_id,
                    remote_host=self.remote_host,
                    job_id=event["job_id"],
                    job_dir=event["job_dir"],
                    log_file=event["log_file"],
                    exit_code_file=event["exit_code_file"],
                    remote_os=event["remote_os"],
                    poll_interval=self.poll_interval,
                    log_chunk_size=self.log_chunk_size,
                    log_offset=event.get("log_offset", 0),
                    command_timeout=self.command_timeout,
                    max_reconnect_attempts=self.max_reconnect_attempts,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.timeout) if self.timeout else None,
            )
            return

        exit_code = event.get("exit_code")
        job_dir = event.get("job_dir", "")
        remote_os = event.get("remote_os", "posix")

        self.log.info("Remote job completed with exit code: %s", exit_code)

        should_cleanup = self.cleanup == "always" or (self.cleanup == "on_success" and exit_code == 0)
        if should_cleanup and job_dir:
            self._cleanup_remote_job(job_dir, remote_os)

        if exit_code is None:
            raise AirflowException(f"Remote job failed: {event.get('message', 'Unknown error')}")

        if exit_code in self.skip_on_exit_code:
            raise AirflowSkipException(f"Remote job returned skip exit code: {exit_code}")

        if exit_code != 0:
            raise AirflowException(f"Remote job failed with exit code: {exit_code}")

        self.log.info("Remote job completed successfully")

    def _cleanup_remote_job(self, job_dir: str, remote_os: str) -> None:
        """
        Clean up the remote job directory, retrying on transient SSH failures.

        Under a large fan-out the cleanup connection can itself be refused by the
        remote ``sshd`` (``MaxStartups``). Retrying a few times keeps a transient drop
        from orphaning the job directory; if every attempt fails we log loudly and
        leave the directory rather than failing the (already finished) task.
        """
        self.log.info("Cleaning up remote job directory: %s", job_dir)
        if remote_os == "posix":
            cleanup_cmd = build_posix_cleanup_command(job_dir)
        else:
            cleanup_cmd = build_windows_cleanup_command(job_dir)

        last_error: Exception | None = None
        for attempt in range(1, self.cleanup_retries + 1):
            try:
                with self.ssh_hook.get_conn() as ssh_client:
                    self.ssh_hook.exec_ssh_client_command(
                        ssh_client,
                        cleanup_cmd,
                        get_pty=False,
                        environment=None,
                        timeout=30,
                    )
                self.log.info("Remote cleanup completed")
                return
            except Exception as e:
                last_error = e
                self.log.warning("Cleanup attempt %d/%d failed: %s", attempt, self.cleanup_retries, e)
                if attempt < self.cleanup_retries:
                    time.sleep(min(2**attempt, 10))

        self.log.warning(
            "Failed to clean up remote job directory after %d attempts; leaving orphaned "
            "directory %s on the remote host (last error: %s)",
            self.cleanup_retries,
            job_dir,
            last_error,
        )

    def on_kill(self) -> None:
        """
        Attempt to kill the remote process when the task is killed.

        Since the operator is recreated after deferral, instance variables may not
        be set. We retrieve job information from XCom if needed.
        """
        job_id = self._job_id
        pid_file = self._paths.pid_file if self._paths else None
        remote_os = self._detected_os

        if not job_id or not pid_file or not remote_os:
            try:
                if hasattr(self, "task_instance") and self.task_instance:
                    job_info = self.task_instance.xcom_pull(key="ssh_remote_job")
                    if job_info:
                        job_id = job_info.get("job_id")
                        pid_file = job_info.get("pid_file")
                        remote_os = job_info.get("remote_os")
            except Exception as e:
                self.log.debug("Could not retrieve job info from XCom: %s", e)

        if not job_id or not pid_file or not remote_os:
            self.log.info("No active job information available for kill")
            return

        self.log.info("Attempting to kill remote job: %s", job_id)
        try:
            if remote_os == "posix":
                kill_cmd = build_posix_kill_command(pid_file)
            else:
                kill_cmd = build_windows_kill_command(pid_file)

            with self.ssh_hook.get_conn() as ssh_client:
                self.ssh_hook.exec_ssh_client_command(
                    ssh_client,
                    kill_cmd,
                    get_pty=False,
                    environment=None,
                    timeout=30,
                )
            self.log.info("Kill command sent to remote process")
        except Exception as e:
            self.log.warning("Failed to kill remote process: %s", e)
