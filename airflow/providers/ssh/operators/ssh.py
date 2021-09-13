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

import warnings
from base64 import b64encode
from select import select
from typing import Optional, Union

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

CMD_TIMEOUT = 10


class SSHOperator(BaseOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: airflow.providers.ssh.hooks.ssh.SSHHook
    :param ssh_conn_id: :ref:`ssh connection id<howto/connection:ssh>`
        from airflow Connections. `ssh_conn_id` will be ignored if
        `ssh_hook` is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param command: command to execute on remote host. (templated)
    :type command: str
    :param conn_timeout: timeout (in seconds) for maintaining the connection. The default is 10 seconds.
        Nullable. If provided, it will replace the `conn_timeout` which was
        predefined in the connection of `ssh_conn_id`.
    :type conn_timeout: int
    :param cmd_timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
    :type cmd_timeout: int
    :param timeout: (deprecated) timeout (in seconds) for executing the command. The default is 10 seconds.
        Use conn_timeout and cmd_timeout parameters instead.
    :type timeout: int
    :param environment: a dict of shell environment variables. Note that the
        server will reject them silently if `AcceptEnv` is not set in SSH config.
    :type environment: dict
    :param get_pty: request a pseudo-terminal from the server. Set to ``True``
        to have the remote process killed upon task timeout.
        The default is ``False`` but note that `get_pty` is forced to ``True``
        when the `command` starts with ``sudo``.
    :type get_pty: bool
    """

    template_fields = ('command', 'remote_host')
    template_ext = ('.sh',)
    template_fields_renderers = {"command": "bash"}

    def __init__(
        self,
        *,
        ssh_hook: Optional[SSHHook] = None,
        ssh_conn_id: Optional[str] = None,
        remote_host: Optional[str] = None,
        command: Optional[str] = None,
        timeout: Optional[int] = None,
        conn_timeout: Optional[int] = None,
        cmd_timeout: Optional[int] = None,
        environment: Optional[dict] = None,
        get_pty: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.timeout = timeout
        self.conn_timeout = conn_timeout
        self.cmd_timeout = cmd_timeout
        if self.conn_timeout is None and self.timeout:
            self.conn_timeout = self.timeout
        if self.cmd_timeout is None:
            self.cmd_timeout = self.timeout if self.timeout else CMD_TIMEOUT
        self.environment = environment
        self.get_pty = (self.command.startswith('sudo') or get_pty) if self.command else get_pty

        if self.timeout:
            warnings.warn(
                'Parameter `timeout` is deprecated.'
                'Please use `conn_timeout` and `cmd_timeout` instead.'
                'The old option `timeout` will be removed in a future version.',
                DeprecationWarning,
                stacklevel=1,
            )

    def execute(self, context) -> Union[bytes, str, bool]:
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id, conn_timeout=self.conn_timeout)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            if not self.command:
                raise AirflowException("SSH command not specified. Aborting.")

            with self.ssh_hook.get_conn() as ssh_client:
                self.log.info("Running command: %s", self.command)

                # set timeout taken as params
                stdin, stdout, stderr = ssh_client.exec_command(
                    command=self.command,
                    get_pty=self.get_pty,
                    timeout=self.cmd_timeout,
                    environment=self.environment,
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
                while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], self.cmd_timeout)
                    for recv in readq:
                        if recv.recv_ready():
                            line = stdout.channel.recv(len(recv.in_buffer))
                            agg_stdout += line
                            self.log.info(line.decode('utf-8', 'replace').strip('\n'))
                        if recv.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                            agg_stderr += line
                            self.log.warning(line.decode('utf-8', 'replace').strip('\n'))
                    if (
                        stdout.channel.exit_status_ready()
                        and not stderr.channel.recv_stderr_ready()
                        and not stdout.channel.recv_ready()
                    ):
                        stdout.channel.shutdown_read()
                        try:
                            stdout.channel.close()
                        except Exception:
                            # there is a race that when shutdown_read has been called and when
                            # you try to close the connection, the socket is already closed
                            # We should ignore such errors (but we should log them with warning)
                            self.log.warning("Ignoring exception on close", exc_info=True)
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    enable_pickling = conf.getboolean('core', 'enable_xcom_pickling')
                    if enable_pickling:
                        return agg_stdout
                    else:
                        return b64encode(agg_stdout).decode('utf-8')

                else:
                    error_msg = agg_stderr.decode('utf-8')
                    raise AirflowException(f"error running cmd: {self.command}, error: {error_msg}")

        except Exception as e:
            raise AirflowException(f"SSH operator error: {str(e)}")

        return True

    def tunnel(self) -> None:
        """Get ssh tunnel"""
        ssh_client = self.ssh_hook.get_conn()  # type: ignore[union-attr]
        ssh_client.get_transport()
