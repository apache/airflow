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

from airflow.contrib.hooks.winrm_hook import WinRMHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class WinRMOperator(BaseOperator):

    """
    WinRMOperator to execute commands on given remote host using the winrm_hook.

    :param winrm_hook: predefined ssh_hook to use for remote execution
    :type winrm_hook: :class:`WinRMHook`
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
        try:
            if self.ssh_conn_id and not self.winrm_hook:
                self.log.info("hook not found, creating")
                self.winrm_hook = WinRMHook(ssh_conn_id=self.ssh_conn_id)

            if not self.winrm_hook:
                raise AirflowException("can not operate without ssh_hook or ssh_conn_id")

            if self.remote_host is not None:
                self.winrm_hook.remote_host = self.remote_host

            winrm_client = self.winrm_hook.get_conn()
            self.log.info("Established WinRM connection")

            if not self.command:
                raise AirflowException("no command specified so nothing to execute here.")

            self.log.info(
                "Starting command: '{command}' on remote host: {remotehost}".
                format(command=self.command, remotehost=self.winrm_hook.remote_host)
            )
            command_id = self.winrm_hook.winrm_protocol. \
                run_command(winrm_client, self.command)
            std_out, std_err, status_code = self.winrm_hook.winrm_protocol. \
                get_command_output(winrm_client, command_id)

            self.log.info("std out: " + std_out.decode())
            self.log.info("std err: " + std_err.decode())
            self.log.info("exit code: " + str(status_code))
            self.log.info("Cleaning up WinRM command")
            self.winrm_hook.winrm_protocol.cleanup_command(winrm_client, command_id)
            self.log.info("Cleaning up WinRM protocol shell")
            self.winrm_hook.winrm_protocol.close_shell(winrm_client)
            if status_code is 0:
                return std_out.decode()

            else:
                error_msg = std_err.decode()
                raise AirflowException("error running cmd: {0}, error: {1}"
                                       .format(self.command, error_msg))

        except Exception as e:
            raise AirflowException("WinRM operator error: {0}".format(str(e)))

        return True
