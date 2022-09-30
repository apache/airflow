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

import os
import warnings
from base64 import decodebytes
from io import StringIO
from select import select
from typing import Any, Sequence

import paramiko
from paramiko.config import SSH_PORT
from sshtunnel import SSHTunnelForwarder
from tenacity import Retrying, stop_after_attempt, wait_fixed, wait_random

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

try:
    from airflow.utils.platform import getuser
except ImportError:
    from getpass import getuser  # type: ignore[misc]

TIMEOUT_DEFAULT = 10


class SSHHook(BaseHook):
    """
    Hook for ssh remote execution using Paramiko.
    ref: https://github.com/paramiko/paramiko
    This hook also lets you create ssh tunnel and serve as basis for SFTP file transfer

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
    :param timeout: (Deprecated). timeout for the attempt to connect to the remote_host.
        Use conn_timeout instead.
    :param keepalive_interval: send a keepalive packet to remote host every
        keepalive_interval seconds
    :param banner_timeout: timeout to wait for banner from the server in seconds
    :param disabled_algorithms: dictionary mapping algorithm type to an
        iterable of algorithm identifiers, which will be disabled for the
        lifetime of the transport
    :param ciphers: list of ciphers to use in order of preference
    """

    # List of classes to try loading private keys as, ordered (roughly) by most common to least common
    _pkey_loaders: Sequence[type[paramiko.PKey]] = (
        paramiko.RSAKey,
        paramiko.ECDSAKey,
        paramiko.Ed25519Key,
        paramiko.DSSKey,
    )

    _host_key_mappings = {
        'rsa': paramiko.RSAKey,
        'dss': paramiko.DSSKey,
        'ecdsa': paramiko.ECDSAKey,
        'ed25519': paramiko.Ed25519Key,
    }

    conn_name_attr = 'ssh_conn_id'
    default_conn_name = 'ssh_default'
    conn_type = 'ssh'
    hook_name = 'SSH'

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema'],
            "relabeling": {
                'login': 'Username',
            },
        }

    def __init__(
        self,
        ssh_conn_id: str | None = None,
        remote_host: str = '',
        username: str | None = None,
        password: str | None = None,
        key_file: str | None = None,
        port: int | None = None,
        timeout: int | None = None,
        conn_timeout: int | None = None,
        keepalive_interval: int = 30,
        banner_timeout: float = 30.0,
        disabled_algorithms: dict | None = None,
        ciphers: list[str] | None = None,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.username = username
        self.password = password
        self.key_file = key_file
        self.pkey = None
        self.port = port
        self.timeout = timeout
        self.conn_timeout = conn_timeout
        self.keepalive_interval = keepalive_interval
        self.banner_timeout = banner_timeout
        self.disabled_algorithms = disabled_algorithms
        self.ciphers = ciphers
        self.host_proxy_cmd = None

        # Default values, overridable from Connection
        self.compress = True
        self.no_host_key_check = True
        self.allow_host_key_change = False
        self.host_key = None
        self.look_for_keys = True

        # Placeholder for deprecated __enter__
        self.client: paramiko.SSHClient | None = None

        # Use connection to override defaults
        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)
            if self.username is None:
                self.username = conn.login
            if self.password is None:
                self.password = conn.password
            if not self.remote_host:
                self.remote_host = conn.host
            if self.port is None:
                self.port = conn.port

            if conn.extra is not None:
                extra_options = conn.extra_dejson
                if "key_file" in extra_options and self.key_file is None:
                    self.key_file = extra_options.get("key_file")

                private_key = extra_options.get('private_key')
                private_key_passphrase = extra_options.get('private_key_passphrase')
                if private_key:
                    self.pkey = self._pkey_from_private_key(private_key, passphrase=private_key_passphrase)

                if "timeout" in extra_options:
                    warnings.warn(
                        'Extra option `timeout` is deprecated.'
                        'Please use `conn_timeout` instead.'
                        'The old option `timeout` will be removed in a future version.',
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    self.timeout = int(extra_options['timeout'])

                if "conn_timeout" in extra_options and self.conn_timeout is None:
                    self.conn_timeout = int(extra_options['conn_timeout'])

                if "compress" in extra_options and str(extra_options["compress"]).lower() == 'false':
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
                    and str(extra_options["allow_host_key_change"]).lower() == 'true'
                ):
                    self.allow_host_key_change = True

                if (
                    "look_for_keys" in extra_options
                    and str(extra_options["look_for_keys"]).lower() == 'false'
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
                    decoded_host_key = decodebytes(host_key.encode('utf-8'))
                    self.host_key = key_constructor(data=decoded_host_key)
                    self.no_host_key_check = False

        if self.timeout:
            warnings.warn(
                'Parameter `timeout` is deprecated.'
                'Please use `conn_timeout` instead.'
                'The old option `timeout` will be removed in a future version.',
                DeprecationWarning,
                stacklevel=1,
            )

        if self.conn_timeout is None:
            self.conn_timeout = self.timeout if self.timeout else TIMEOUT_DEFAULT

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

        user_ssh_config_filename = os.path.expanduser('~/.ssh/config')
        if os.path.isfile(user_ssh_config_filename):
            ssh_conf = paramiko.SSHConfig()
            with open(user_ssh_config_filename) as config_fd:
                ssh_conf.parse(config_fd)
            host_info = ssh_conf.lookup(self.remote_host)
            if host_info and host_info.get('proxycommand'):
                self.host_proxy_cmd = host_info['proxycommand']

            if not (self.password or self.key_file):
                if host_info and host_info.get('identityfile'):
                    self.key_file = host_info['identityfile'][0]

        self.port = self.port or SSH_PORT

    @cached_property
    def host_proxy(self) -> paramiko.ProxyCommand | None:
        cmd = self.host_proxy_cmd
        return paramiko.ProxyCommand(cmd) if cmd else None

    def get_conn(self) -> paramiko.SSHClient:
        """
        Opens a ssh connection to the remote host.

        :rtype: paramiko.client.SSHClient
        """
        self.log.debug('Creating SSH client for conn_id: %s', self.ssh_conn_id)
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
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # to avoid BadHostKeyException, skip loading and saving host keys
            known_hosts = os.path.expanduser("~/.ssh/known_hosts")
            if not self.allow_host_key_change and os.path.isfile(known_hosts):
                client.load_host_keys(known_hosts)
        else:
            if self.host_key is not None:
                client_host_keys = client.get_host_keys()
                if self.port == SSH_PORT:
                    client_host_keys.add(self.remote_host, self.host_key.get_name(), self.host_key)
                else:
                    client_host_keys.add(
                        f"[{self.remote_host}]:{self.port}", self.host_key.get_name(), self.host_key
                    )
            else:
                pass  # will fallback to system host keys if none explicitly specified in conn extra

        connect_kwargs: dict[str, Any] = dict(
            hostname=self.remote_host,
            username=self.username,
            timeout=self.conn_timeout,
            compress=self.compress,
            port=self.port,
            sock=self.host_proxy,
            look_for_keys=self.look_for_keys,
            banner_timeout=self.banner_timeout,
        )

        if self.password:
            password = self.password.strip()
            connect_kwargs.update(password=password)

        if self.pkey:
            connect_kwargs.update(pkey=self.pkey)

        if self.key_file:
            connect_kwargs.update(key_filename=self.key_file)

        if self.disabled_algorithms:
            connect_kwargs.update(disabled_algorithms=self.disabled_algorithms)

        log_before_sleep = lambda retry_state: self.log.info(
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
            # type "Optional[Transport]" and item "None" has no attribute "set_keepalive".
            client.get_transport().set_keepalive(self.keepalive_interval)  # type: ignore[union-attr]

        if self.ciphers:
            # MyPy check ignored because "paramiko" isn't well-typed. The `client.get_transport()` returns
            # type "Optional[Transport]" and item "None" has no method `get_security_options`".
            client.get_transport().get_security_options().ciphers = self.ciphers  # type: ignore[union-attr]

        self.client = client
        return client

    def __enter__(self) -> SSHHook:
        warnings.warn(
            'The contextmanager of SSHHook is deprecated.'
            'Please use get_conn() as a contextmanager instead.'
            'This method will be removed in Airflow 2.0',
            category=DeprecationWarning,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    def get_tunnel(
        self, remote_port: int, remote_host: str = "localhost", local_port: int | None = None
    ) -> SSHTunnelForwarder:
        """
        Creates a tunnel between two hosts. Like ssh -L <LOCAL_PORT>:host:<REMOTE_PORT>.

        :param remote_port: The remote port to create a tunnel to
        :param remote_host: The remote host to create a tunnel to (default localhost)
        :param local_port:  The local port to attach the tunnel to

        :return: sshtunnel.SSHTunnelForwarder object
        """
        if local_port:
            local_bind_address: tuple[str, int] | tuple[str] = ('localhost', local_port)
        else:
            local_bind_address = ('localhost',)

        tunnel_kwargs = dict(
            ssh_port=self.port,
            ssh_username=self.username,
            ssh_pkey=self.key_file or self.pkey,
            ssh_proxy=self.host_proxy,
            local_bind_address=local_bind_address,
            remote_bind_address=(remote_host, remote_port),
            logger=self.log,
        )

        if self.password:
            password = self.password.strip()
            tunnel_kwargs.update(
                ssh_password=password,
            )
        else:
            tunnel_kwargs.update(
                host_pkey_directories=None,
            )

        client = SSHTunnelForwarder(self.remote_host, **tunnel_kwargs)

        return client

    def create_tunnel(
        self, local_port: int, remote_port: int, remote_host: str = "localhost"
    ) -> SSHTunnelForwarder:
        """
        Creates tunnel for SSH connection [Deprecated].

        :param local_port: local port number
        :param remote_port: remote port number
        :param remote_host: remote host
        :return:
        """
        warnings.warn(
            'SSHHook.create_tunnel is deprecated, Please'
            'use get_tunnel() instead. But please note that the'
            'order of the parameters have changed'
            'This method will be removed in Airflow 2.0',
            category=DeprecationWarning,
        )

        return self.get_tunnel(remote_port, remote_host, local_port)

    def _pkey_from_private_key(self, private_key: str, passphrase: str | None = None) -> paramiko.PKey:
        """
        Creates appropriate paramiko key for given private key

        :param private_key: string containing private key
        :return: ``paramiko.PKey`` appropriate for given key
        :raises AirflowException: if key cannot be read
        """
        if len(private_key.split("\n", 2)) < 2:
            raise AirflowException('Key must have BEGIN and END header/footer on separate lines.')

        for pkey_class in self._pkey_loaders:
            try:
                key = pkey_class.from_private_key(StringIO(private_key), password=passphrase)
                # Test it actually works. If Paramiko loads an openssh generated key, sometimes it will
                # happily load it as the wrong type, only to fail when actually used.
                key.sign_ssh_data(b'')
                return key
            except (paramiko.ssh_exception.SSHException, ValueError):
                continue
        raise AirflowException(
            'Private key provided cannot be read by paramiko.'
            'Ensure key provided is valid for one of the following'
            'key formats: RSA, DSS, ECDSA, or Ed25519'
        )

    def exec_ssh_client_command(
        self,
        ssh_client: paramiko.SSHClient,
        command: str,
        get_pty: bool,
        environment: dict | None,
        timeout: int | None,
    ) -> tuple[int, bytes, bytes]:
        self.log.info("Running command: %s", command)

        # set timeout taken as params
        stdin, stdout, stderr = ssh_client.exec_command(
            command=command,
            get_pty=get_pty,
            timeout=timeout,
            environment=environment,
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
            readq, _, _ = select([channel], [], [], timeout)
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

        return exit_status, agg_stdout, agg_stderr
