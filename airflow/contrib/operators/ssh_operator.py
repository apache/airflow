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
from select import select

from airflow.configuration import conf
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SSHOperator(BaseOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: airflow.contrib.hooks.ssh_hook.SSHHook
    :param ssh_conn_id: connection id from airflow Connections.
        `ssh_conn_id` will be ignored if `ssh_hook` is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param command: command to execute on remote host. (templated)
    :type command: str
    :param timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
    :type timeout: int
    :param environment: a dict of shell environment variables. Note that the
        server will reject them silently if `AcceptEnv` is not set in SSH config.
    :type environment: dict
    :param do_xcom_push: return the stdout which also get set in xcom by airflow platform
    :type do_xcom_push: bool
    :param get_pty: request a pseudo-terminal from the server. Set to ``True``
        to have the remote process killed upon task timeout.
        The default is ``False`` but note that `get_pty` is forced to ``True``
        when the `command` starts with ``sudo``.
    :type get_pty: bool
    """

    template_fields = ('command', 'remote_host')
    template_ext = ('.sh',)

    @apply_defaults
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 command=None,
                 timeout=10,
                 do_xcom_push=False,
                 environment=None,
                 get_pty=False,
                 *args,
                 **kwargs):
        super(SSHOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.timeout = timeout
        self.environment = environment
        self.do_xcom_push = do_xcom_push
        self.get_pty = self.command.startswith('sudo') or get_pty

    def execute(self, context):
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info("ssh_hook is not provided or invalid. " +
                                  "Trying ssh_conn_id to create SSHHook.")
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id,
                                            timeout=self.timeout)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info("remote_host is provided explicitly. " +
                              "It will replace the remote_host which was defined " +
                              "in ssh_hook or predefined in connection of ssh_conn_id.")
                self.ssh_hook.remote_host = self.remote_host

            if not self.command:
                raise AirflowException("SSH command not specified. Aborting.")

            with self.ssh_hook.get_conn() as ssh_client:
                self.log.info("Running command: %s", self.command)

                # set timeout taken as params
                stdin, stdout, stderr = ssh_client.exec_command(command=self.command,
                                                                get_pty=self.get_pty,
                                                                timeout=self.timeout,
                                                                environment=self.environment
                                                                )
                # get channels
                channel = stdout.channel

                # closing stdin
                stdin.close()
                channel.shutdown_write()

                agg_stdout = b''
                agg_stderr = b''

                # capture any initial output in case channel is closed already
                stdout_buffer_length = len(stdout.channel.in_buffer)

                if stdout_buffer_length > 0:
                    agg_stdout += stdout.channel.recv(stdout_buffer_length)

                # read from both stdout and stderr
                while not channel.closed or \
                        channel.recv_ready() or \
                        channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], self.timeout)
                    for c in readq:
                        if c.recv_ready():
                            line = stdout.channel.recv(len(c.in_buffer))
                            line = line
                            agg_stdout += line
                            self.log.info(line.decode('utf-8').strip('\n'))
                        if c.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                            line = line
                            agg_stderr += line
                            self.log.warning(line.decode('utf-8').strip('\n'))
                    if stdout.channel.exit_status_ready()\
                            and not stderr.channel.recv_stderr_ready()\
                            and not stdout.channel.recv_ready():
                        stdout.channel.shutdown_read()
                        stdout.channel.close()
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    # returning output if do_xcom_push is set
                    if self.do_xcom_push:
                        enable_pickling = conf.getboolean(
                            'core', 'enable_xcom_pickling'
                        )
                        if enable_pickling:
                            return agg_stdout
                        else:
                            return b64encode(agg_stdout).decode('utf-8')

                else:
                    error_msg = agg_stderr.decode('utf-8')
                    raise AirflowException("error running cmd: {0}, error: {1}"
                                           .format(self.command, error_msg))

        except Exception as e:
            raise AirflowException("SSH operator error: {0}".format(str(e)))

        return True

    def tunnel(self):
        ssh_client = self.ssh_hook.get_conn()
        ssh_client.get_transport()
