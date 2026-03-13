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
from collections.abc import AsyncIterator
from typing import Any, Literal

import tenacity

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


class SSHRemoteJobTrigger(BaseTrigger):
    """
    Trigger that monitors a remote SSH job and streams logs.

    This trigger polls the remote host to check job completion status
    and reads log output incrementally.

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
            },
        )

    def _get_hook(self) -> SSHHookAsync:
        """Create the async SSH hook."""
        return SSHHookAsync(
            ssh_conn_id=self.ssh_conn_id,
            host=self.remote_host,
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
        retry=tenacity.retry_if_exception_type((OSError, TimeoutError, ConnectionError)),
        reraise=True,
    )
    async def _check_completion(self, hook: SSHHookAsync) -> int | None:
        """
        Check if the remote job has completed.

        Retries transient network errors up to 3 times with exponential backoff.

        :return: Exit code if completed, None if still running
        """
        if self.remote_os == "posix":
            cmd = build_posix_completion_check_command(self.exit_code_file)
        else:
            cmd = build_windows_completion_check_command(self.exit_code_file)

        try:
            _, stdout, _ = await hook.run_command(cmd, timeout=self.command_timeout)
            stdout = stdout.strip()
            if stdout and stdout.isdigit():
                return int(stdout)
        except (OSError, TimeoutError, ConnectionError) as e:
            self.log.warning("Transient error checking completion (will retry): %s", e)
            raise
        except Exception as e:
            self.log.warning("Error checking completion status: %s", e)
        return None

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
        retry=tenacity.retry_if_exception_type((OSError, TimeoutError, ConnectionError)),
        reraise=True,
    )
    async def _get_log_size(self, hook: SSHHookAsync) -> int:
        """
        Get the current size of the log file in bytes.

        Retries transient network errors up to 3 times with exponential backoff.
        """
        if self.remote_os == "posix":
            cmd = build_posix_file_size_command(self.log_file)
        else:
            cmd = build_windows_file_size_command(self.log_file)

        try:
            _, stdout, _ = await hook.run_command(cmd, timeout=self.command_timeout)
            stdout = stdout.strip()
            if stdout and stdout.isdigit():
                return int(stdout)
        except (OSError, TimeoutError, ConnectionError) as e:
            self.log.warning("Transient error getting log size (will retry): %s", e)
            raise
        except Exception as e:
            self.log.warning("Error getting log file size: %s", e)
        return 0

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
        retry=tenacity.retry_if_exception_type((OSError, TimeoutError, ConnectionError)),
        reraise=True,
    )
    async def _read_log_chunk(self, hook: SSHHookAsync) -> tuple[str, int]:
        """
        Read a chunk of logs from the current offset.

        Retries transient network errors up to 3 times with exponential backoff.

        :return: Tuple of (log_chunk, new_offset)
        """
        file_size = await self._get_log_size(hook)
        if file_size <= self.log_offset:
            return "", self.log_offset

        bytes_available = file_size - self.log_offset
        bytes_to_read = min(bytes_available, self.log_chunk_size)

        if self.remote_os == "posix":
            cmd = build_posix_log_tail_command(self.log_file, self.log_offset, bytes_to_read)
        else:
            cmd = build_windows_log_tail_command(self.log_file, self.log_offset, bytes_to_read)

        try:
            exit_code, stdout, _ = await hook.run_command(cmd, timeout=self.command_timeout)

            # Advance offset by bytes requested, not decoded string length
            new_offset = self.log_offset + bytes_to_read if stdout else self.log_offset

            return stdout, new_offset
        except (OSError, TimeoutError, ConnectionError) as e:
            self.log.warning("Transient error reading logs (will retry): %s", e)
            raise
        except Exception as e:
            self.log.warning("Error reading log chunk: %s", e)
            return "", self.log_offset

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Poll the remote job status and yield events with log chunks.

        This method runs in a loop, checking the job status and reading
        logs at each poll interval. It yields a TriggerEvent each time
        with the current status and any new log output.
        """
        hook = self._get_hook()

        while True:
            try:
                exit_code = await self._check_completion(hook)
                log_chunk, new_offset = await self._read_log_chunk(hook)

                base_event = {
                    "job_id": self.job_id,
                    "job_dir": self.job_dir,
                    "log_file": self.log_file,
                    "exit_code_file": self.exit_code_file,
                    "remote_os": self.remote_os,
                }

                if exit_code is not None:
                    yield TriggerEvent(
                        {
                            **base_event,
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
                yield TriggerEvent(
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
                        "message": f"Trigger error: {e}",
                    }
                )
                return
