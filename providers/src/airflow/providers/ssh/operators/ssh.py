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

from base64 import b64encode
from functools import cached_property
from typing import TYPE_CHECKING, Container, Sequence

from deprecated.classic import deprecated

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowProviderDeprecationWarning,
    AirflowSkipException,
)
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from paramiko.client import SSHClient


class SSHOperator(BaseOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :param ssh_conn_id: :ref:`ssh connection id<howto/connection:ssh>`
        from airflow Connections. `ssh_conn_id` will be ignored if
        `ssh_hook` is provided.
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :param command: command to execute on remote host. (templated)
    :param conn_timeout: timeout (in seconds) for maintaining the connection. The default is 10 seconds.
        Nullable. If provided, it will replace the `conn_timeout` which was
        predefined in the connection of `ssh_conn_id`.
    :param cmd_timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
        Nullable, `None` means no timeout. If provided, it will replace the `cmd_timeout`
        which was predefined in the connection of `ssh_conn_id`.
    :param environment: a dict of shell environment variables. Note that the
        server will reject them silently if `AcceptEnv` is not set in SSH config. (templated)
    :param get_pty: request a pseudo-terminal from the server. Set to ``True``
        to have the remote process killed upon task timeout.
        The default is ``False`` but note that `get_pty` is forced to ``True``
        when the `command` starts with ``sudo``.
    :param banner_timeout: timeout to wait for banner from the server in seconds
    :param skip_on_exit_code: If command exits with this exit code, leave the task
        in ``skipped`` state (default: None). If set to ``None``, any non-zero
        exit code will be treated as a failure.

    If *do_xcom_push* is *True*, the numeric exit code emitted by
    the ssh session is pushed to XCom under key ``ssh_exit``.
    """

    template_fields: Sequence[str] = ("command", "environment", "remote_host")
    template_ext: Sequence[str] = (
        ".sh",
        ".bash",
        ".csh",
        ".zsh",
        ".dash",
        ".ksh",
    )
    template_fields_renderers = {
        "command": "bash",
        "environment": "python",
    }

    def __init__(
        self,
        *,
        ssh_hook: SSHHook | None = None,
        ssh_conn_id: str | None = None,
        remote_host: str | None = None,
        command: str | None = None,
        conn_timeout: int | None = None,
        cmd_timeout: int | ArgNotSet | None = NOTSET,
        environment: dict | None = None,
        get_pty: bool = False,
        banner_timeout: float = 30.0,
        skip_on_exit_code: int | Container[int] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if ssh_hook and isinstance(ssh_hook, SSHHook):
            self.ssh_hook = ssh_hook
            if remote_host is not None:
                self.ssh_hook.remote_host = remote_host
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.conn_timeout = conn_timeout
        self.cmd_timeout = cmd_timeout
        self.environment = environment
        self.get_pty = get_pty
        self.banner_timeout = banner_timeout
        self.skip_on_exit_code = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code is not None
            else []
        )

    @cached_property
    def ssh_hook(self) -> SSHHook:
        """Create SSHHook to run commands on remote host."""
        if self.ssh_conn_id:
            self.log.info(
                "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
            )
            hook = SSHHook(
                ssh_conn_id=self.ssh_conn_id,
                conn_timeout=self.conn_timeout,
                cmd_timeout=self.cmd_timeout,
                banner_timeout=self.banner_timeout,
            )
            if self.remote_host is not None:
                self.log.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                hook.remote_host = self.remote_host
            return hook
        raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

    @property
    def hook(self) -> SSHHook:
        return self.ssh_hook

    @deprecated(
        reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning
    )
    def get_hook(self) -> SSHHook:
        return self.ssh_hook

    def get_ssh_client(self) -> SSHClient:
        # Remember to use context manager or call .close() on this when done
        return self.hook.get_conn()

    @deprecated(
        reason=(
            "exec_ssh_client_command method on SSHOperator is deprecated, call "
            "`ssh_hook.exec_ssh_client_command` instead"
        ),
        category=AirflowProviderDeprecationWarning,
    )
    def exec_ssh_client_command(
        self, ssh_client: SSHClient, command: str
    ) -> tuple[int, bytes, bytes]:
        return self.hook.exec_ssh_client_command(
            ssh_client,
            command,
            timeout=self.cmd_timeout,
            environment=self.environment,
            get_pty=self.get_pty,
        )

    def raise_for_status(self, exit_status: int, stderr: bytes, context=None) -> None:
        if context and self.do_xcom_push:
            ti = context.get("task_instance")
            ti.xcom_push(key="ssh_exit", value=exit_status)
        if exit_status in self.skip_on_exit_code:
            raise AirflowSkipException(
                f"SSH command returned exit code {exit_status}. Skipping."
            )
        if exit_status != 0:
            raise AirflowException(f"SSH operator error: exit status = {exit_status}")

    def run_ssh_client_command(
        self, ssh_client: SSHClient, command: str, context=None
    ) -> bytes:
        exit_status, agg_stdout, agg_stderr = self.hook.exec_ssh_client_command(
            ssh_client,
            command,
            timeout=self.cmd_timeout,
            environment=self.environment,
            get_pty=self.get_pty,
        )
        self.raise_for_status(exit_status, agg_stderr, context=context)
        return agg_stdout

    def execute(self, context=None) -> bytes | str:
        result: bytes | str
        if self.command is None:
            raise AirflowException(
                "SSH operator error: SSH command not specified. Aborting."
            )

        # Forcing get_pty to True if the command begins with "sudo".
        self.get_pty = self.command.startswith("sudo") or self.get_pty

        with self.get_ssh_client() as ssh_client:
            result = self.run_ssh_client_command(
                ssh_client, self.command, context=context
            )
        enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
        if not enable_pickling:
            result = b64encode(result).decode("utf-8")

        return result

    def tunnel(self) -> None:
        """Get ssh tunnel."""
        ssh_client = self.hook.get_conn()  # type: ignore[union-attr]
        ssh_client.get_transport()

    def on_kill(self) -> None:
        """Close the ssh client session."""
        ssh_client = self.hook.client
        if ssh_client:
            ssh_client.close()
            self.log.info("SSH client closed.")
        else:
            self.log.info("No SSH client to close.")
