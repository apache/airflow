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
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from functools import cached_property
from typing import Any

from winrm.exceptions import WinRMOperationTimeoutError

from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


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
        timeout: float | None = None,
        deadline: float | None = None,
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
        self.timeout = timeout
        self.deadline = (
            deadline
            if deadline is not None
            else time.monotonic() + self.timeout
            if self.timeout is not None
            else None
        )

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
                "timeout": self.timeout,
                "deadline": self.deadline,
            },
        )

    @cached_property
    def hook(self) -> WinRMHook:
        return WinRMHook(ssh_conn_id=self.ssh_conn_id)

    @property
    def is_expired(self) -> bool:
        return self.deadline is not None and time.monotonic() >= self.deadline

    async def run(self) -> AsyncIterator[TriggerEvent]:
        stdout: bytes = b""
        stderr: bytes = b""
        return_code: int | None = None
        command_done: bool = False

        while not command_done:
            try:
                if self.is_expired:
                    raise TimeoutError(
                        f"Command {self.command_id} did not finish within {self.timeout} seconds!"
                    )

                with suppress(WinRMOperationTimeoutError):
                    stdout, stderr, return_code, command_done = await asyncio.to_thread(
                        self.hook.get_conn().get_command_output_raw, self.shell_id, self.command_id
                    )

                self.log.debug("return_code: ", return_code)
                self.log.debug("command_done: ", command_done)

                if not command_done:
                    await asyncio.sleep(self.poll_interval)
                    continue

                yield TriggerEvent(
                    {
                        "status": "success",
                        "shell_id": self.shell_id,
                        "command_id": self.command_id,
                        "return_code": return_code,
                        "stdout": base64.standard_b64encode(stdout).decode(self.output_encoding)
                        if self.return_output
                        else "",
                        "stderr": base64.standard_b64encode(stderr).decode(self.output_encoding),
                    }
                )
                return

            except Exception as e:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "shell_id": self.shell_id,
                        "command_id": self.command_id,
                        "message": str(e),
                    }
                )
                return
