# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from base64 import b64encode
import logging

from airflow import configuration
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SSHOperator(BaseOperator):

    """
    SSHOperator to execute commands on given remote host using the ssh_hook.

    :param ssh_hook: predefined ssh_hook to use for remote execution
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param command: command to execute on remote host
    :type command: str
    :param timeout: timeout for executing the command.
    :type timeout: int
    :param do_xcom_push: return the stdout which also get set in xcom by airflow platform
    :type do_xcom_push: bool
    """

    template_fields = ('command',)

    @apply_defaults
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 command=None,
                 timeout=10,
                 do_xcom_push=False,
                 *args,
                 **kwargs):
        super(SSHOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.timeout = timeout
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
        try:
            if self.ssh_conn_id and not self.ssh_hook:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("can not operate without ssh_hook or ssh_conn_id")

            if self.remote_host is not None:
                self.ssh_hook.remote_host = self.remote_host

            ssh_client = self.ssh_hook.get_conn()

            if not self.command:
                raise AirflowException("no command specified so nothing to execute here.")

            # Auto apply tty when its required in case of sudo
            get_pty = False
            if self.command.startswith('sudo'):
                get_pty = True

            # set timeout taken as params
            stdin, stdout, stderr = ssh_client.exec_command(command=self.command,
                                                            get_pty=get_pty,
                                                            timeout=self.timeout
                                                            )
            exit_status = stdout.channel.recv_exit_status()
            if exit_status is 0:
                # only returning on output if do_xcom_push is set
                # otherwise its not suppose to be disclosed
                if self.do_xcom_push:
                    enable_pickling = configuration.getboolean('core',
                                                               'enable_xcom_pickling')
                    if enable_pickling:
                        return stdout.read()
                    else:
                        return b64encode(stdout.read()).decode('utf-8')

            else:
                error_msg = stderr.read()
                raise AirflowException("error running cmd: {0}, error: {1}"
                                        .format(self.command, error_msg))

        except Exception as e:
            raise AirflowException("SSH operator error: {0}".format(str(e)))

        return True

    def tunnel(self):
        ssh_client = self.ssh_hook.get_conn()
        ssh_client.get_transport()
