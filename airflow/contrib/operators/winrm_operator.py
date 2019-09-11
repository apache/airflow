# -*- coding: utf-8 -*-
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

from base64 import b64encode
import logging

from winrm.exceptions import WinRMOperationTimeoutError

from airflow import configuration
from airflow.contrib.hooks.winrm_hook import WinRMHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Hide the following error message in urllib3 when making WinRM connections:
# requests.packages.urllib3.exceptions.HeaderParsingError: [StartBoundaryNotFoundDefect(),
#   MultipartInvariantViolationDefect()], unparsed data: ''
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)


class WinRMOperator(BaseOperator):
    """
    WinRMOperator to execute commands on given remote host using the winrm_hook.

    :param winrm_hook: predefined ssh_hook to use for remote execution
    :type winrm_hook: airflow.contrib.hooks.winrm_hook.WinRMHook
    :param ssh_conn_id: connection id from airflow Connections
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param command: command to execute on remote host. (templated)
    :type command: str
    :param timeout: timeout for executing the command.
    :type timeout: int
    :param do_xcom_push: return the stdout which also get set in xcom by airflow platform
    :type do_xcom_push: bool
    """
    template_fields = ('command',)

    @apply_defaults
    def __init__(self,
                 winrm_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 command=None,
                 timeout=10,
                 do_xcom_push=False,
                 *args,
                 **kwargs):
        super(WinRMOperator, self).__init__(*args, **kwargs)
        self.winrm_hook = winrm_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.timeout = timeout
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
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
            self.log.info("Running command: '%s'...", self.command)
            command_id = self.winrm_hook.winrm_protocol.run_command(
                winrm_client,
                self.command
            )

            # See: https://github.com/diyan/pywinrm/blob/master/winrm/protocol.py
            stdout_buffer = []
            stderr_buffer = []
            command_done = False
            while not command_done:
                try:
                    stdout, stderr, return_code, command_done = \
                        self.winrm_hook.winrm_protocol._raw_get_command_output(
                            winrm_client,
                            command_id
                        )

                    # Only buffer stdout if we need to so that we minimize memory usage.
                    if self.do_xcom_push:
                        stdout_buffer.append(stdout)
                    stderr_buffer.append(stderr)

                    for line in stdout.decode('utf-8').splitlines():
                        self.log.info(line)
                    for line in stderr.decode('utf-8').splitlines():
                        self.log.warning(line)
                except WinRMOperationTimeoutError:
                    # this is an expected error when waiting for a
                    # long-running process, just silently retry
                    pass

            self.winrm_hook.winrm_protocol.cleanup_command(winrm_client, command_id)
            self.winrm_hook.winrm_protocol.close_shell(winrm_client)

        except Exception as e:
            raise AirflowException("WinRM operator error: {0}".format(str(e)))

        if return_code == 0:
            # returning output if do_xcom_push is set
            if self.do_xcom_push:
                enable_pickling = configuration.conf.getboolean(
                    'core', 'enable_xcom_pickling'
                )
                if enable_pickling:
                    return stdout_buffer
                else:
                    return b64encode(b''.join(stdout_buffer)).decode('utf-8')
        else:
            error_msg = "Error running cmd: {0}, return code: {1}, error: {2}".format(
                self.command,
                return_code,
                b''.join(stderr_buffer).decode('utf-8')
            )
            raise AirflowException(error_msg)

        self.log.info("Finished!")

        return True
