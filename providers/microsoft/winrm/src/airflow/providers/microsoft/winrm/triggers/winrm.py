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
"""Hook for winrm remote execution."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import suppress
from functools import cached_property
from typing import TYPE_CHECKING, Any

from winrm.exceptions import InvalidCredentialsError

from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from winrm import Protocol


class WinRMCommandOutputTrigger(BaseTrigger):
    """
    A trigger that polls the command output executed by the WinRMHook.

    This trigger avoids blocking a worker when using the WinRMOperator in deferred mode.

    The behavior of this trigger is as follows:
    - poll the command output from the shell launched by WinRM,
    - if command not done then sleep and retry,
    - when command done then return the output.

    :param ssh_conn_id: connection id from airflow Connections from where
        all the required parameters can be fetched like username and password,
        though priority is given to the params passed during init.
    :param shell_id: The shell id on the remote machine.
    :param command_id: The command id executed on the remote machine.
    :param output_encoding: the encoding used to decode stout and stderr, defaults to utf-8.
    :param return_output: Whether to accumulate and return the stdout or not, defaults to True.
    :param working_directory: specify working directory.
    :param poll_interval: How often, in seconds, the trigger should poll the output command of the launched command,
        defaults to 1.
    :param timeout: max time allowed for polling, if it goes beyond it will raise and fail.
    """

    def __init__(
        self,
        ssh_conn_id: str,
        shell_id: str,
        command_id: str,
        output_encoding: str = "utf-8",
        return_output: bool = True,
        working_directory: str | None = None,
        expected_return_code: int | list[int] | range = 0,
        poll_interval: float = 1,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.shell_id = shell_id
        self.command_id = command_id
        self.output_encoding = output_encoding
        self.return_output = return_output
        self.working_directory = working_directory
        self.expected_return_code = expected_return_code
        self.poll_interval = poll_interval
        self._stdout: list[str] = []
        self._stderr: list[str] = []

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize WinRMCommandOutputTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "ssh_conn_id": self.ssh_conn_id,
                "shell_id": self.shell_id,
                "command_id": self.command_id,
                "output_encoding": self.output_encoding,
                "return_output": self.return_output,
                "working_directory": self.working_directory,
                "expected_return_code": self.expected_return_code,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self) -> WinRMHook:
        return WinRMHook(ssh_conn_id=self.ssh_conn_id)

    async def get_command_output(
        self, conn: Protocol
    ) -> tuple[bytes, bytes, int, bool]:
        stdout, stderr, return_code, command_done = await asyncio.to_thread(
            self.hook.get_command_output, conn, self.shell_id, self.command_id
        )
        return stdout, stderr, return_code, command_done

    async def run(self) -> AsyncIterator[TriggerEvent]:
        command_done: bool = False
        conn_refreshed: bool = False
        conn: Protocol | None = None

        try:
            conn = await self.hook.get_async_conn()
            while not command_done:
                try:
                    (
                        stdout,
                        stderr,
                        return_code,
                        command_done,
                    ) = await self.get_command_output(conn)
                except InvalidCredentialsError:
                    if conn_refreshed:
                        raise
                    conn_refreshed = True
                    with suppress(Exception):
                        conn.close_shell(self.shell_id)
                    conn = await self.hook.get_async_conn()
                    (
                        stdout,
                        stderr,
                        return_code,
                        command_done,
                    ) = await self.get_command_output(conn)
                else:
                    conn_refreshed = False

                if self.return_output and stdout:
                    self._stdout.append(
                        base64.standard_b64encode(stdout).decode(self.output_encoding)
                    )
                if stderr:
                    self._stderr.append(
                        base64.standard_b64encode(stderr).decode(self.output_encoding)
                    )

                if not command_done:
                    await asyncio.sleep(self.poll_interval)
                    continue

                yield TriggerEvent(
                    {
                        "status": "success",
                        "shell_id": self.shell_id,
                        "command_id": self.command_id,
                        "return_code": return_code,
                        "stdout": self._stdout,
                        "stderr": self._stderr,
                    }
                )
                return
        except Exception as e:
            self.log.exception("An error occurred: %s", e)
            yield TriggerEvent(
                {
                    "status": "error",
                    "shell_id": self.shell_id,
                    "command_id": self.command_id,
                    "message": str(e),
                }
            )
            return
        finally:
            if conn:
                with suppress(Exception):
                    conn.cleanup_command(self.shell_id, self.command_id)
                with suppress(Exception):
                    conn.close_shell(self.shell_id)
