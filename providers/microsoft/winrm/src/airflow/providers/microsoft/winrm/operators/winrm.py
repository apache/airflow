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
import logging
from base64 import b64encode
from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator, conf
from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook
from airflow.providers.microsoft.winrm.triggers.winrm import WinRMCommandOutputTrigger

if TYPE_CHECKING:
    from airflow.sdk import Context

# Hide the following error message in urllib3 when making WinRM connections:
# requests.packages.urllib3.exceptions.HeaderParsingError: [StartBoundaryNotFoundDefect(),
#   MultipartInvariantViolationDefect()], unparsed data: ''

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


class WinRMOperator(BaseOperator):
    """
    WinRMOperator to execute commands on given remote host using the winrm_hook.

    :param winrm_hook: predefined ssh_hook to use for remote execution
    :param ssh_conn_id: connection id from airflow Connections
    :param remote_host: remote host to connect
    :param command: command to execute on remote host. (templated)
    :param ps_path: path to powershell, `powershell` for v5.1- and `pwsh` for v6+.
        If specified, it will execute the command as powershell script.
    :param output_encoding: the encoding used to decode stout and stderr
    :param timeout: timeout for executing the command, defaults to 10.
    :param poll_interval: How often, in seconds, the trigger should poll the output command of the launched command,
        defaults to 1.
    :param expected_return_code: expected return code value(s) of command.
    :param working_directory: specify working directory.
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = (
        "command",
        "working_directory",
    )
    template_fields_renderers = {"command": "powershell", "working_directory": "powershell"}

    def __init__(
        self,
        *,
        winrm_hook: WinRMHook | None = None,
        ssh_conn_id: str | None = None,
        remote_host: str | None = None,
        command: str | None = None,
        ps_path: str | None = None,
        output_encoding: str = "utf-8",
        timeout: int | timedelta = 10,
        poll_interval: int | timedelta | None = None,
        expected_return_code: int | list[int] | range = 0,
        working_directory: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.winrm_hook = winrm_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.ps_path = ps_path
        self.output_encoding = output_encoding
        self.timeout = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        self.poll_interval = (
            poll_interval.total_seconds()
            if isinstance(poll_interval, timedelta)
            else poll_interval
            if poll_interval is not None
            else 1.0
        )
        self.expected_return_code = expected_return_code
        self.working_directory = working_directory
        self.deferrable = deferrable

    @property
    def hook(self) -> WinRMHook:
        if not self.winrm_hook:
            if self.ssh_conn_id:
                self.log.info("Hook not found, creating...")
                self.winrm_hook = WinRMHook(ssh_conn_id=self.ssh_conn_id)
            else:
                raise AirflowException("Cannot operate without winrm_hook.")

            if not self.winrm_hook.ssh_conn_id and self.deferrable:
                raise AirflowException("Cannot operate in deferrable mode without ssh_conn_id.")

            if self.remote_host is not None:
                self.winrm_hook.remote_host = self.remote_host

        return self.winrm_hook

    def execute(self, context: Context) -> list | str:
        if self.deferrable:
            shell_id, command_id = self.hook.run_command(
                command=self.command,
                ps_path=self.ps_path,
                working_directory=self.working_directory,
            )
            return self.defer(
                trigger=WinRMCommandOutputTrigger(
                    ssh_conn_id=self.hook.ssh_conn_id,
                    shell_id=shell_id,
                    command_id=command_id,
                    output_encoding=self.output_encoding,
                    return_output=self.do_xcom_push,
                    working_directory=self.working_directory,
                    poll_interval=self.poll_interval,
                    timeout=self.timeout,
                ),
                method_name=self.execute_complete.__name__,
                # timeout must always be a timedelta for defer in Airflow 2.x!
                timeout=timedelta(seconds=self.timeout)
                if isinstance(self.timeout, (int | float))
                else self.timeout,
            )

        return_code, stdout_buffer, stderr_buffer = self.hook.run(
            command=self.command,
            ps_path=self.ps_path,
            output_encoding=self.output_encoding,
            return_output=self.do_xcom_push,
            working_directory=self.working_directory,
        )
        return self.evaluate_result(return_code, stdout_buffer, stderr_buffer)

    def validate_return_code(self, return_code: int | None) -> bool:
        if return_code is not None:
            if isinstance(self.expected_return_code, int):
                return return_code == self.expected_return_code
            if isinstance(self.expected_return_code, list) or isinstance(self.expected_return_code, range):
                return return_code in self.expected_return_code
        return False

    def evaluate_result(
        self,
        return_code: int | None,
        stdout_buffer: list[bytes],
        stderr_buffer: list[bytes],
    ) -> Any:
        success = self.validate_return_code(return_code)

        self.log.debug("success: %s", success)

        if success:
            # returning output if do_xcom_push is set
            # TODO: Remove this after minimum Airflow version is 3.0
            enable_pickling = conf.getboolean("core", "enable_xcom_pickling", fallback=False)

            if enable_pickling:
                return stdout_buffer
            return b64encode(b"".join(stdout_buffer)).decode(self.output_encoding)

        stderr_output = b"".join(stderr_buffer).decode(self.output_encoding)
        error_msg = f"Error running cmd: {self.command}, return code: {return_code}, error: {stderr_output}"
        raise AirflowException(error_msg)

    def execute_complete(
        self,
        context: Context,
        event: dict[Any, Any] | None = None,
    ) -> Any:
        """
        Execute callback when WinRMCommandOutputTrigger finishes execution.

        This method gets executed automatically when WinRMCommandOutputTrigger completes its execution.
        """
        if event:
            status = event.get("status")

            if status == "error":
                raise AirflowException(f"Trigger failed: {event.get('message')}")

            return_code = event.get("return_code")

            self.log.info("%s completed with %s", self.task_id, status)

            stdout = base64.standard_b64decode(event.get("stdout", ""))
            stderr = base64.standard_b64decode(event.get("stderr", ""))

            self.hook.log_output(stdout, output_encoding=self.output_encoding)
            self.hook.log_output(stderr, level=logging.WARNING, output_encoding=self.output_encoding)

            return self.evaluate_result(return_code, [stdout], [stderr])
        return None
