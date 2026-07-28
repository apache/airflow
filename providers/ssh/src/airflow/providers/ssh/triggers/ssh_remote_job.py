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
"""SSH Remote Job Trigger for deferrable execution."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Literal

import asyncssh

from airflow.providers.ssh.hooks.ssh import SSHHookAsync
from airflow.providers.ssh.utils.remote_job import (
    build_posix_completion_check_command,
    build_posix_file_size_command,
    build_posix_log_tail_command,
    build_windows_completion_check_command,
    build_windows_file_size_command,
    build_windows_log_tail_command,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from asyncssh import SSHClientConnection

# Errors that mean the connection itself is broken/refused and the poll should
# reconnect instead of failing the job. ``asyncssh.Error`` covers handshake,
# protocol and disconnect failures (e.g. an sshd that drops the connection under
# ``MaxStartups`` load); ``OSError`` covers TCP-level refusals; ``TimeoutError``
# covers a wedged command or connection.
_CONNECTION_ERRORS = (OSError, asyncssh.Error, TimeoutError)


class SSHRemoteJobTrigger(BaseTrigger):
    """
    Trigger that monitors a remote SSH job and streams logs.

    This trigger polls the remote host to check job completion status
    and reads log output incrementally.

    A single SSH connection is opened and reused for the whole poll loop instead
    of reconnecting for every command. Opening a fresh TCP/SSH connection per poll
    multiplies the connection rate against the remote ``sshd`` (which throttles
    concurrent unauthenticated connections via ``MaxStartups``), so reuse keeps the
    load flat when many tasks target the same host. If the connection drops, the
    trigger transparently reconnects with backoff up to ``max_reconnect_attempts``.

    :param ssh_conn_id: SSH connection ID from Airflow Connections
    :param remote_host: Optional override for the remote host
    :param job_id: Unique identifier for the remote job
    :param job_dir: Remote directory containing job artifacts
    :param log_file: Path to the log file on the remote host
    :param exit_code_file: Path to the exit code file on the remote host
    :param remote_os: Operating system of the remote host ('posix' or 'windows')
    :param poll_interval: Seconds between polling attempts
    :param log_chunk_size: Maximum bytes to read per poll
    :param log_offset: Current byte offset in the log file
    :param command_timeout: Per-command timeout in seconds
    :param max_reconnect_attempts: Consecutive connection failures tolerated before the
        trigger gives up and emits an error event
    """

    def __init__(
        self,
        ssh_conn_id: str,
        remote_host: str | None,
        job_id: str,
        job_dir: str,
        log_file: str,
        exit_code_file: str,
        remote_os: Literal["posix", "windows"],
        poll_interval: int = 5,
        log_chunk_size: int = 65536,
        log_offset: int = 0,
        command_timeout: float = 30.0,
        max_reconnect_attempts: int = 5,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.job_id = job_id
        self.job_dir = job_dir
        self.log_file = log_file
        self.exit_code_file = exit_code_file
        self.remote_os = remote_os
        self.poll_interval = poll_interval
        self.log_chunk_size = log_chunk_size
        self.log_offset = log_offset
        self.command_timeout = command_timeout
        self.max_reconnect_attempts = max_reconnect_attempts

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for storage."""
        return (
            "airflow.providers.ssh.triggers.ssh_remote_job.SSHRemoteJobTrigger",
            {
                "ssh_conn_id": self.ssh_conn_id,
                "remote_host": self.remote_host,
                "job_id": self.job_id,
                "job_dir": self.job_dir,
                "log_file": self.log_file,
                "exit_code_file": self.exit_code_file,
                "remote_os": self.remote_os,
                "poll_interval": self.poll_interval,
                "log_chunk_size": self.log_chunk_size,
                "log_offset": self.log_offset,
                "command_timeout": self.command_timeout,
                "max_reconnect_attempts": self.max_reconnect_attempts,
            },
        )

    def _get_hook(self) -> SSHHookAsync:
        """Create the async SSH hook."""
        return SSHHookAsync(
            ssh_conn_id=self.ssh_conn_id,
            host=self.remote_host,
        )

    async def _connect(self) -> SSHClientConnection:
        """Open a reusable asyncssh connection. Separated out as a seam for testing."""
        return await self._get_hook().get_conn()

    @staticmethod
    async def _close(conn: SSHClientConnection) -> None:
        """Close a connection, swallowing teardown errors."""
        try:
            conn.close()
            await conn.wait_closed()
        except Exception:
            # Teardown is best-effort; a failing close has nothing actionable to recover.
            pass

    def _reconnect_delay(self, attempt: int) -> float:
        """Exponential backoff with randomness so reconnecting triggers do not retry in lockstep."""
        base = min(2 ** (attempt - 1), 30)
        return base + random.uniform(0, base)

    async def _run_command(self, conn: SSHClientConnection, command: str) -> tuple[int, str, str]:
        """Run a command on an existing connection, mirroring ``SSHHookAsync.run_command``."""
        result = await conn.run(command, timeout=self.command_timeout, check=False)
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        # asyncssh types stdout/stderr as bytes | str; with the default text encoding they are
        # str, but decode defensively so the helper holds if a binary connection is ever used.
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        return result.exit_status or 0, stdout, stderr

    async def _check_completion(self, conn: SSHClientConnection) -> int | None:
        """
        Check if the remote job has completed.

        :return: Exit code if completed, None if still running
        """
        if self.remote_os == "posix":
            cmd = build_posix_completion_check_command(self.exit_code_file)
        else:
            cmd = build_windows_completion_check_command(self.exit_code_file)

        _, stdout, _ = await self._run_command(conn, cmd)
        stdout = stdout.strip()
        if stdout and stdout.isdigit():
            return int(stdout)
        return None

    async def _get_log_size(self, conn: SSHClientConnection) -> int:
        """Get the current size of the log file in bytes."""
        if self.remote_os == "posix":
            cmd = build_posix_file_size_command(self.log_file)
        else:
            cmd = build_windows_file_size_command(self.log_file)

        _, stdout, _ = await self._run_command(conn, cmd)
        stdout = stdout.strip()
        if stdout and stdout.isdigit():
            return int(stdout)
        return 0

    async def _read_log_chunk(self, conn: SSHClientConnection) -> tuple[str, int]:
        """
        Read a chunk of logs from the current offset.

        :return: Tuple of (log_chunk, new_offset)
        """
        file_size = await self._get_log_size(conn)
        if file_size <= self.log_offset:
            return "", self.log_offset

        bytes_available = file_size - self.log_offset
        bytes_to_read = min(bytes_available, self.log_chunk_size)

        if self.remote_os == "posix":
            cmd = build_posix_log_tail_command(self.log_file, self.log_offset, bytes_to_read)
        else:
            cmd = build_windows_log_tail_command(self.log_file, self.log_offset, bytes_to_read)

        _, stdout, _ = await self._run_command(conn, cmd)

        # Advance offset by bytes requested, not decoded string length
        new_offset = self.log_offset + bytes_to_read if stdout else self.log_offset
        return stdout, new_offset

    def _error_event(self, message: str) -> TriggerEvent:
        return TriggerEvent(
            {
                "job_id": self.job_id,
                "job_dir": self.job_dir,
                "log_file": self.log_file,
                "exit_code_file": self.exit_code_file,
                "remote_os": self.remote_os,
                "status": "error",
                "done": True,
                "exit_code": None,
                "log_chunk": "",
                "log_offset": self.log_offset,
                "message": message,
            }
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Poll the remote job status and yield a completion event.

        One connection is held for the whole loop. On a connection-level failure the
        connection is dropped and re-established (with exponential backoff) up to
        ``max_reconnect_attempts`` consecutive times; any other error, or exhausting the
        reconnect budget, ends the trigger with an error event.
        """
        conn: SSHClientConnection | None = None
        # Consecutive failures since the last *fully successful* poll. A successful
        # handshake alone does not reset this: a connection that handshakes but whose
        # command channel keeps failing (e.g. ChannelOpenError under sshd MaxSessions)
        # must still exhaust the budget instead of looping forever.
        failures = 0

        try:
            while True:
                if conn is None:
                    try:
                        conn = await self._connect()
                    except _CONNECTION_ERRORS as e:
                        failures += 1
                        if failures > self.max_reconnect_attempts:
                            raise
                        delay = self._reconnect_delay(failures)
                        self.log.warning(
                            "Failed to connect to remote host (attempt %d/%d), retrying in %.1fs: %s",
                            failures,
                            self.max_reconnect_attempts,
                            delay,
                            e,
                        )
                        await asyncio.sleep(delay)
                        continue

                try:
                    exit_code = await self._check_completion(conn)
                    log_chunk, new_offset = await self._read_log_chunk(conn)
                except _CONNECTION_ERRORS as e:
                    failures += 1
                    self.log.warning(
                        "Lost SSH connection while polling (attempt %d/%d), reconnecting: %s",
                        failures,
                        self.max_reconnect_attempts,
                        e,
                    )
                    await self._close(conn)
                    conn = None
                    if failures > self.max_reconnect_attempts:
                        raise
                    await asyncio.sleep(self._reconnect_delay(failures))
                    continue

                # A full poll cycle succeeded on this connection; clear the failure budget.
                failures = 0

                if exit_code is not None:
                    yield TriggerEvent(
                        {
                            "job_id": self.job_id,
                            "job_dir": self.job_dir,
                            "log_file": self.log_file,
                            "exit_code_file": self.exit_code_file,
                            "remote_os": self.remote_os,
                            "status": "success" if exit_code == 0 else "failed",
                            "done": True,
                            "exit_code": exit_code,
                            "log_chunk": log_chunk,
                            "log_offset": new_offset,
                            "message": f"Job completed with exit code {exit_code}",
                        }
                    )
                    return

                self.log_offset = new_offset
                if log_chunk:
                    self.log.info("%s", log_chunk.rstrip())
                await asyncio.sleep(self.poll_interval)

        except Exception as e:
            self.log.exception("Error in SSH remote job trigger")
            yield self._error_event(f"Trigger error: {e}")
            return
        finally:
            if conn is not None:
                await self._close(conn)
