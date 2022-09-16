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

import logging
from base64 import b64encode
from typing import TYPE_CHECKING, Sequence

from winrm.exceptions import WinRMOperationTimeoutError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

# Hide the following error message in urllib3 when making WinRM connections:
# requests.packages.urllib3.exceptions.HeaderParsingError: [StartBoundaryNotFoundDefect(),
#   MultipartInvariantViolationDefect()], unparsed data: ''

logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)


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
    :param timeout: timeout for executing the command.
    """

    template_fields: Sequence[str] = ('command',)
    template_fields_renderers = {"command": "powershell"}

    def __init__(
        self,
        *,
        winrm_hook: WinRMHook | None = None,
        ssh_conn_id: str | None = None,
        remote_host: str | None = None,
        command: str | None = None,
        ps_path: str | None = None,
        output_encoding: str = 'utf-8',
        timeout: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.winrm_hook = winrm_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.ps_path = ps_path
        self.output_encoding = output_encoding
        self.timeout = timeout

    def execute(self, context: Context) -> list | str:
        if self.ssh_conn_id and not self.winrm_hook:
            self.log.info("Hook not found, creating...")
            self.winrm_hook = WinRMHook(ssh_conn_id=self.ssh_conn_id)

        if not self.winrm_hook:
            raise AirflowException("Cannot operate without winrm_hook or ssh_conn_id.")

        if self.remote_host is not None:
            self.winrm_hook.remote_host = self.remote_host

        if not self.command:
            raise AirflowException("No command specified so nothing to execute here.")

        winrm_client = self.winrm_hook.get_conn()

        try:
            if self.ps_path is not None:
                self.log.info("Running command as powershell script: '%s'...", self.command)
                encoded_ps = b64encode(self.command.encode('utf_16_le')).decode('ascii')
                command_id = self.winrm_hook.winrm_protocol.run_command(  # type: ignore[attr-defined]
                    winrm_client, f'{self.ps_path} -encodedcommand {encoded_ps}'
                )
            else:
                self.log.info("Running command: '%s'...", self.command)
                command_id = self.winrm_hook.winrm_protocol.run_command(  # type: ignore[attr-defined]
                    winrm_client, self.command
                )

            # See: https://github.com/diyan/pywinrm/blob/master/winrm/protocol.py
            stdout_buffer = []
            stderr_buffer = []
            command_done = False
            while not command_done:
                try:

                    (
                        stdout,
                        stderr,
                        return_code,
                        command_done,
                    ) = self.winrm_hook.winrm_protocol._raw_get_command_output(  # type: ignore[attr-defined]
                        winrm_client, command_id
                    )

                    # Only buffer stdout if we need to so that we minimize memory usage.
                    if self.do_xcom_push:
                        stdout_buffer.append(stdout)
                    stderr_buffer.append(stderr)

                    for line in stdout.decode(self.output_encoding).splitlines():
                        self.log.info(line)
                    for line in stderr.decode(self.output_encoding).splitlines():
                        self.log.warning(line)
                except WinRMOperationTimeoutError:
                    # this is an expected error when waiting for a
                    # long-running process, just silently retry
                    pass

            self.winrm_hook.winrm_protocol.cleanup_command(  # type: ignore[attr-defined]
                winrm_client, command_id
            )
            self.winrm_hook.winrm_protocol.close_shell(winrm_client)  # type: ignore[attr-defined]

        except Exception as e:
            raise AirflowException(f"WinRM operator error: {str(e)}")

        if return_code == 0:
            # returning output if do_xcom_push is set
            enable_pickling = conf.getboolean('core', 'enable_xcom_pickling')
            if enable_pickling:
                return stdout_buffer
            else:
                return b64encode(b''.join(stdout_buffer)).decode(self.output_encoding)
        else:
            stderr_output = b''.join(stderr_buffer).decode(self.output_encoding)
            error_msg = (
                f"Error running cmd: {self.command}, return code: {return_code}, error: {stderr_output}"
            )
            raise AirflowException(error_msg)
