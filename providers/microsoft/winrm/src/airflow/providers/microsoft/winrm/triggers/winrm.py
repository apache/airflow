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
"""Trigger for winrm remote execution."""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from collections.abc import AsyncIterator
from functools import cached_property
from typing import TYPE_CHECKING, Any

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
    :param poll_interval: How often, in seconds, the trigger should poll the output command of the launched command,
        defaults to 1.
    :param max_output_chunks: Maximum number of stdout/stderr chunks to keep in a rolling buffer to prevent
        excessive memory usage for long-running commands, defaults to 100.
    """

    def __init__(
        self,
        ssh_conn_id: str,
        shell_id: str,
        command_id: str,
        output_encoding: str = "utf-8",
        return_output: bool = True,
        poll_interval: float = 1,
        max_output_chunks: int = 100,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.shell_id = shell_id
        self.command_id = command_id
        self.output_encoding = output_encoding
        self.return_output = return_output
        self.poll_interval = poll_interval
        self._stdout: deque[str] = deque(maxlen=max_output_chunks)
        self._stderr: deque[str] = deque(maxlen=max_output_chunks)

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
                "poll_interval": self.poll_interval,
                "max_output_chunks": self._stdout.maxlen,
            },
        )

    @cached_property
    def hook(self) -> WinRMHook:
        return WinRMHook(ssh_conn_id=self.ssh_conn_id)

    async def get_command_output(self, conn: Protocol) -> tuple[bytes, bytes, int | None, bool]:
        from asgiref.sync import sync_to_async

        return await sync_to_async(self.hook.get_command_output)(conn, self.shell_id, self.command_id)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        command_done: bool = False

        try:
            conn = await self.hook.get_async_conn()
            while not command_done:
                (
                    stdout,
                    stderr,
                    return_code,
                    command_done,
                ) = await self.get_command_output(conn)

                if self.return_output and stdout:
                    self._stdout.append(base64.standard_b64encode(stdout).decode(self.output_encoding))
                if stderr:
                    self._stderr.append(base64.standard_b64encode(stderr).decode(self.output_encoding))

                if not command_done:
                    await asyncio.sleep(self.poll_interval)
                    continue

                yield TriggerEvent(
                    {
                        "status": "success",
                        "shell_id": self.shell_id,
                        "command_id": self.command_id,
                        "return_code": return_code,
                        "stdout": list(self._stdout),
                        "stderr": list(self._stderr),
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
