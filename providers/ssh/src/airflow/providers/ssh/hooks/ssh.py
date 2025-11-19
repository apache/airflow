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
"""Hook for SSH connections."""

from __future__ import annotations

import logging
import os
from base64 import decodebytes
from collections.abc import Sequence
from functools import cached_property
from io import StringIO
from select import select
from typing import Any

import paramiko
from paramiko.config import SSH_PORT
from sshtunnel import SSHTunnelForwarder
from tenacity import Retrying, stop_after_attempt, wait_fixed, wait_random

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook
from airflow.utils.platform import getuser

try:
    from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
except ImportError:
    from airflow.utils.types import NOTSET, ArgNotSet  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk.definitions._internal.types import is_arg_set
except ImportError:

    def is_arg_set(value):  # type: ignore[misc,no-redef]
        return value is not NOTSET


CMD_TIMEOUT = 10


class SSHHook(BaseHook):
    """
    Execute remote commands with Paramiko.

    .. seealso:: https://github.com/paramiko/paramiko

    This hook also lets you create ssh tunnel and serve as basis for SFTP file transfer.

    :param ssh_conn_id: :ref:`ssh connection id<howto/connection:ssh>` from airflow
        Connections from where all the required parameters can be fetched like
        username, password or key_file, though priority is given to the
        params passed during init.
    :param remote_host: remote host to connect
    :param username: username to connect to the remote_host
    :param password: password of the username to connect to the remote_host
    :param key_file: path to key file to use to connect to the remote_host
    :param port: port of remote host to connect (Default is paramiko SSH_PORT)
    :param conn_timeout: timeout (in seconds) for the attempt to connect to the remote_host.
        The default is 10 seconds. If provided, it will replace the `conn_timeout` which was
        predefined in the connection of `ssh_conn_id`.
    :param cmd_timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
        Nullable, `None` means no timeout. If provided, it will replace the `cmd_timeout`
        which was predefined in the connection of `ssh_conn_id`.
    :param keepalive_interval: send a keepalive packet to remote host every
        keepalive_interval seconds
    :param banner_timeout: timeout to wait for banner from the server in seconds
    :param disabled_algorithms: dictionary mapping algorithm type to an
        iterable of algorithm identifiers, which will be disabled for the
        lifetime of the transport
    :param ciphers: list of ciphers to use in order of preference
    :param auth_timeout: timeout (in seconds) for the attempt to authenticate with the remote_host
    """

    # List of classes to try loading private keys as, ordered (roughly) by most common to least common
    _pkey_loaders: Sequence[type[paramiko.PKey]] = (
        paramiko.RSAKey,
        paramiko.ECDSAKey,
        paramiko.Ed25519Key,
        paramiko.DSSKey,
    )

    _host_key_mappings = {
        "rsa": paramiko.RSAKey,
        "dss": paramiko.DSSKey,
        "ecdsa": paramiko.ECDSAKey,
        "ed25519": paramiko.Ed25519Key,
    }

    conn_name_attr = "ssh_conn_id"
    default_conn_name = "ssh_default"
    conn_type = "ssh"
    hook_name = "SSH"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for SSH connection."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "login": "Username",
            },
        }

    def __init__(
        self,
        ssh_conn_id: str | None = None,
        remote_host: str = "",
        username: str | None = None,
        password: str | None = None,
        key_file: str | None = None,
        port: int | None = None,
        conn_timeout: int | None = None,
        cmd_timeout: float | ArgNotSet | None = NOTSET,
        keepalive_interval: int = 30,
        banner_timeout: float = 30.0,
        disabled_algorithms: dict | None = None,
        ciphers: list[str] | None = None,
        auth_timeout: int | None = None,
        host_proxy_cmd: str | None = None,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.username = username
        self.password = password
        self.key_file = key_file
        self.pkey = None
        self.port = port
        self.conn_timeout = conn_timeout
        self.cmd_timeout = cmd_timeout
        self.keepalive_interval = keepalive_interval
        self.banner_timeout = banner_timeout
        self.disabled_algorithms = disabled_algorithms
        self.ciphers = ciphers
        self.host_proxy_cmd = host_proxy_cmd
        self.auth_timeout = auth_timeout

        # Default values, overridable from Connection
        self.compress = True
        self.no_host_key_check = True
        self.allow_host_key_change = False
        self.host_key = None
        self.look_for_keys = True

        # Placeholder for future cached connection
        self.client: paramiko.SSHClient | None = None

        # Use connection to override defaults
        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)
            if self.username is None:
                self.username = conn.login
            if self.password is None:
                self.password = conn.password
            if not self.remote_host:
                if conn.host:
                    self.remote_host = conn.host
            if self.port is None:
                self.port = conn.port

            if conn.extra is not None:
                extra_options = conn.extra_dejson
                if "key_file" in extra_options and self.key_file is None:
                    self.key_file = extra_options.get("key_file")

                private_key = extra_options.get("private_key")
                private_key_passphrase = extra_options.get("private_key_passphrase")
                if private_key:
                    self.pkey = self._pkey_from_private_key(private_key, passphrase=private_key_passphrase)

                if "conn_timeout" in extra_options and self.conn_timeout is None:
                    self.conn_timeout = int(extra_options["conn_timeout"])

                if "cmd_timeout" in extra_options and self.cmd_timeout is NOTSET:
                    if extra_options["cmd_timeout"]:
                        self.cmd_timeout = float(extra_options["cmd_timeout"])
                    else:
                        self.cmd_timeout = None

                if "compress" in extra_options and str(extra_options["compress"]).lower() == "false":
                    self.compress = False

                host_key = extra_options.get("host_key")
                no_host_key_check = extra_options.get("no_host_key_check")

                if no_host_key_check is not None:
                    no_host_key_check = str(no_host_key_check).lower() == "true"
                    if host_key is not None and no_host_key_check:
                        raise ValueError("Must check host key when provided")

                    self.no_host_key_check = no_host_key_check

                if (
                    "allow_host_key_change" in extra_options
                    and str(extra_options["allow_host_key_change"]).lower() == "true"
                ):
                    self.allow_host_key_change = True

                if (
                    "look_for_keys" in extra_options
                    and str(extra_options["look_for_keys"]).lower() == "false"
                ):
                    self.look_for_keys = False

                if "disabled_algorithms" in extra_options:
                    self.disabled_algorithms = extra_options.get("disabled_algorithms")

                if "ciphers" in extra_options:
                    self.ciphers = extra_options.get("ciphers")

                if host_key is not None:
                    if host_key.startswith("ssh-"):
                        key_type, host_key = host_key.split(None)[:2]
                        key_constructor = self._host_key_mappings[key_type[4:]]
                    else:
                        key_constructor = paramiko.RSAKey
                    decoded_host_key = decodebytes(host_key.encode("utf-8"))
                    self.host_key = key_constructor(data=decoded_host_key)
                    self.no_host_key_check = False

        if self.cmd_timeout is NOTSET:
            self.cmd_timeout = CMD_TIMEOUT

        if self.pkey and self.key_file:
            raise AirflowException(
                "Params key_file and private_key both provided.  Must provide no more than one."
            )

        if not self.remote_host:
            raise AirflowException("Missing required param: remote_host")

        # Auto detecting username values from system
        if not self.username:
            self.log.debug(
                "username to ssh to host: %s is not specified for connection id"
                " %s. Using system's default provided by getpass.getuser()",
                self.remote_host,
                self.ssh_conn_id,
            )
            self.username = getuser()

        user_ssh_config_filename = os.path.expanduser("~/.ssh/config")
        if os.path.isfile(user_ssh_config_filename):
            ssh_conf = paramiko.SSHConfig()
            with open(user_ssh_config_filename) as config_fd:
                ssh_conf.parse(config_fd)
            host_info = ssh_conf.lookup(self.remote_host)
            if host_info and host_info.get("proxycommand") and not self.host_proxy_cmd:
                self.host_proxy_cmd = host_info["proxycommand"]

            if not (self.password or self.key_file):
                if host_info and host_info.get("identityfile"):
                    self.key_file = host_info["identityfile"][0]

        self.port = self.port or SSH_PORT

    @cached_property
    def host_proxy(self) -> paramiko.ProxyCommand | None:
        cmd = self.host_proxy_cmd
        return paramiko.ProxyCommand(cmd) if cmd else None

    def get_conn(self) -> paramiko.SSHClient:
        """Establish an SSH connection to the remote host."""
        if self.client:
            transport = self.client.get_transport()
            if transport and transport.is_active():
                # Return the existing connection
                return self.client

        self.log.debug("Creating SSH client for conn_id: %s", self.ssh_conn_id)
        client = paramiko.SSHClient()

        if self.allow_host_key_change:
            self.log.warning(
                "Remote Identification Change is not verified. "
                "This won't protect against Man-In-The-Middle attacks"
            )
            # to avoid BadHostKeyException, skip loading host keys
            client.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy)
        else:
            client.load_system_host_keys()

        if self.no_host_key_check:
            self.log.warning("No Host Key Verification. This won't protect against Man-In-The-Middle attacks")
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # nosec B507
            # to avoid BadHostKeyException, skip loading and saving host keys
            known_hosts = os.path.expanduser("~/.ssh/known_hosts")
            if not self.allow_host_key_change and os.path.isfile(known_hosts):
                client.load_host_keys(known_hosts)

        elif self.host_key is not None:
            # Get host key from connection extra if it not set or None then we fallback to system host keys
            client_host_keys = client.get_host_keys()
            if self.port == SSH_PORT:
                client_host_keys.add(self.remote_host, self.host_key.get_name(), self.host_key)
            else:
                client_host_keys.add(
                    f"[{self.remote_host}]:{self.port}", self.host_key.get_name(), self.host_key
                )

        connect_kwargs: dict[str, Any] = {
            "hostname": self.remote_host,
            "username": self.username,
            "timeout": self.conn_timeout,
            "compress": self.compress,
            "port": self.port,
            "sock": self.host_proxy,
            "look_for_keys": self.look_for_keys,
            "banner_timeout": self.banner_timeout,
            "auth_timeout": self.auth_timeout,
        }

        if self.password:
            password = self.password.strip()
            connect_kwargs.update(password=password)

        if self.pkey:
            connect_kwargs.update(pkey=self.pkey)

        if self.key_file:
            connect_kwargs.update(key_filename=self.key_file)

        if self.disabled_algorithms:
            connect_kwargs.update(disabled_algorithms=self.disabled_algorithms)

        def log_before_sleep(retry_state):
            return self.log.info(
                "Failed to connect. Sleeping before retry attempt %d", retry_state.attempt_number
            )

        for attempt in Retrying(
            reraise=True,
            wait=wait_fixed(3) + wait_random(0, 2),
            stop=stop_after_attempt(3),
            before_sleep=log_before_sleep,
        ):
            with attempt:
                client.connect(**connect_kwargs)

        if self.keepalive_interval:
            # MyPy check ignored because "paramiko" isn't well-typed. The `client.get_transport()` returns
            # type "Transport | None" and item "None" has no attribute "set_keepalive".
            client.get_transport().set_keepalive(self.keepalive_interval)  # type: ignore[union-attr]

        if self.ciphers:
            # MyPy check ignored because "paramiko" isn't well-typed. The `client.get_transport()` returns
            # type "Transport | None" and item "None" has no method `get_security_options`".
            client.get_transport().get_security_options().ciphers = self.ciphers  # type: ignore[union-attr]

        self.client = client
        return client

    def get_tunnel(
        self, remote_port: int, remote_host: str = "localhost", local_port: int | None = None
    ) -> SSHTunnelForwarder:
        """
        Create a tunnel between two hosts.

        This is conceptually similar to ``ssh -L <LOCAL_PORT>:host:<REMOTE_PORT>``.

        :param remote_port: The remote port to create a tunnel to
        :param remote_host: The remote host to create a tunnel to (default localhost)
        :param local_port:  The local port to attach the tunnel to

        :return: sshtunnel.SSHTunnelForwarder object
        """
        if local_port:
            local_bind_address: tuple[str, int] | tuple[str] = ("localhost", local_port)
        else:
            local_bind_address = ("localhost",)

        tunnel_kwargs = {
            "ssh_port": self.port,
            "ssh_username": self.username,
            "ssh_pkey": self.key_file or self.pkey,
            "ssh_proxy": self.host_proxy,
            "local_bind_address": local_bind_address,
            "remote_bind_address": (remote_host, remote_port),
            "logger": self.log,
        }

        if self.password:
            password = self.password.strip()
            tunnel_kwargs.update(
                ssh_password=password,
            )
        else:
            tunnel_kwargs.update(
                host_pkey_directories=None,
            )

        if not hasattr(self.log, "handlers"):
            # We need to not hit this https://github.com/pahaz/sshtunnel/blob/dc0732884379a19a21bf7a49650d0708519ec54f/sshtunnel.py#L238-L239
            paramkio_log = logging.getLogger("paramiko.transport")
            paramkio_log.addHandler(logging.NullHandler())
            paramkio_log.propagate = True
        client = SSHTunnelForwarder(self.remote_host, **tunnel_kwargs)

        return client

    def _pkey_from_private_key(self, private_key: str, passphrase: str | None = None) -> paramiko.PKey:
        """
        Create an appropriate Paramiko key for a given private key.

        :param private_key: string containing private key
        :return: ``paramiko.PKey`` appropriate for given key
        :raises AirflowException: if key cannot be read
        """
        if len(private_key.splitlines()) < 2:
            raise AirflowException("Key must have BEGIN and END header/footer on separate lines.")

        for pkey_class in self._pkey_loaders:
            try:
                key = pkey_class.from_private_key(StringIO(private_key), password=passphrase)
                # Test it actually works. If Paramiko loads an openssh generated key, sometimes it will
                # happily load it as the wrong type, only to fail when actually used.
                key.sign_ssh_data(b"")
                return key
            except (paramiko.ssh_exception.SSHException, ValueError):
                continue
        raise AirflowException(
            "Private key provided cannot be read by paramiko."
            "Ensure key provided is valid for one of the following"
            "key formats: RSA, DSS, ECDSA, or Ed25519"
        )

    def exec_ssh_client_command(
        self,
        ssh_client: paramiko.SSHClient,
        command: str,
        get_pty: bool,
        environment: dict | None,
        timeout: float | ArgNotSet | None = NOTSET,
    ) -> tuple[int, bytes, bytes]:
        self.log.info("Running command: %s", command)

        cmd_timeout: float | None
        if is_arg_set(timeout):
            cmd_timeout = timeout
        elif is_arg_set(self.cmd_timeout):
            cmd_timeout = self.cmd_timeout
        else:
            cmd_timeout = CMD_TIMEOUT
        del timeout  # Too easy to confuse with "timedout" below.

        # set timeout taken as params
        stdin, stdout, stderr = ssh_client.exec_command(
            command=command,
            get_pty=get_pty,
            timeout=cmd_timeout,
            environment=environment,
        )
        # get channels
        channel = stdout.channel

        # closing stdin
        stdin.close()
        channel.shutdown_write()

        agg_stdout = b""
        agg_stderr = b""

        # capture any initial output in case channel is closed already
        stdout_buffer_length = len(stdout.channel.in_buffer)

        if stdout_buffer_length > 0:
            agg_stdout += stdout.channel.recv(stdout_buffer_length)

        timedout = False

        # read from both stdout and stderr
        while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
            readq, _, _ = select([channel], [], [], cmd_timeout)
            if cmd_timeout is not None:
                timedout = not readq
            for recv in readq:
                if recv.recv_ready():
                    output = stdout.channel.recv(len(recv.in_buffer))
                    agg_stdout += output
                    for line in output.decode("utf-8", "replace").strip("\n").splitlines():
                        self.log.info(line)
                if recv.recv_stderr_ready():
                    output = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                    agg_stderr += output
                    for line in output.decode("utf-8", "replace").strip("\n").splitlines():
                        self.log.warning(line)
            if (
                stdout.channel.exit_status_ready()
                and not stderr.channel.recv_stderr_ready()
                and not stdout.channel.recv_ready()
            ) or timedout:
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

        if timedout:
            raise AirflowException("SSH command timed out")

        exit_status = stdout.channel.recv_exit_status()

        return exit_status, agg_stdout, agg_stderr

    def test_connection(self) -> tuple[bool, str]:
        """Test the ssh connection by execute remote bash commands."""
        try:
            with self.get_conn() as conn:
                conn.exec_command("pwd")
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
