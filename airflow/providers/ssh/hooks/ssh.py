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
import os
import warnings
from base64 import decodebytes
from io import StringIO
from typing import Dict, Optional, Tuple, Union

import paramiko
from paramiko.config import SSH_PORT
from sshtunnel import SSHTunnelForwarder

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

try:
    from airflow.utils.platform import getuser
except ImportError:
    from getpass import getuser

TIMEOUT_DEFAULT = 10


class SSHHook(BaseHook):
    """
    Hook for ssh remote execution using Paramiko.
    ref: https://github.com/paramiko/paramiko
    This hook also lets you create ssh tunnel and serve as basis for SFTP file transfer

    :param ssh_conn_id: :ref:`ssh connection id<howto/connection:ssh>` from airflow
        Connections from where all the required parameters can be fetched like
        username, password or key_file. Thought the priority is given to the
        param passed during init
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param username: username to connect to the remote_host
    :type username: str
    :param password: password of the username to connect to the remote_host
    :type password: str
    :param key_file: path to key file to use to connect to the remote_host
    :type key_file: str
    :param port: port of remote host to connect (Default is paramiko SSH_PORT)
    :type port: int
    :param conn_timeout: timeout (in seconds) for the attempt to connect to the remote_host.
        The default is 10 seconds. If provided, it will replace the `conn_timeout` which was
        predefined in the connection of `ssh_conn_id`.
    :type conn_timeout: int
    :param timeout: (Deprecated). timeout for the attempt to connect to the remote_host.
        Use conn_timeout instead.
    :type timeout: int
    :param keepalive_interval: send a keepalive packet to remote host every
        keepalive_interval seconds
    :type keepalive_interval: int
    """

    # List of classes to try loading private keys as, ordered (roughly) by most common to least common
    _pkey_loaders = (
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
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema'],
            "relabeling": {
                'login': 'Username',
            },
        }

    def __init__(
        self,
        ssh_conn_id: Optional[str] = None,
        remote_host: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        key_file: Optional[str] = None,
        port: Optional[int] = None,
        timeout: Optional[int] = None,
        conn_timeout: Optional[int] = None,
        keepalive_interval: int = 30,
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

        # Default values, overridable from Connection
        self.compress = True
        self.no_host_key_check = True
        self.allow_host_key_change = False
        self.host_proxy = None
        self.host_key = None
        self.look_for_keys = True

        # Placeholder for deprecated __enter__
        self.client = None

        # Use connection to override defaults
        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)
            if self.username is None:
                self.username = conn.login
            if self.password is None:
                self.password = conn.password
            if self.remote_host is None:
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
                self.host_proxy = paramiko.ProxyCommand(host_info.get('proxycommand'))

            if not (self.password or self.key_file):
                if host_info and host_info.get('identityfile'):
                    self.key_file = host_info.get('identityfile')[0]

        self.port = self.port or SSH_PORT

    def get_conn(self) -> paramiko.SSHClient:
        """
        Opens a ssh connection to the remote host.

        :rtype: paramiko.client.SSHClient
        """
        self.log.debug('Creating SSH client for conn_id: %s', self.ssh_conn_id)
        client = paramiko.SSHClient()

        if not self.allow_host_key_change:
            self.log.warning(
                'Remote Identification Change is not verified. '
                'This wont protect against Man-In-The-Middle attacks'
            )
            client.load_system_host_keys()

        if self.no_host_key_check:
            self.log.warning('No Host Key Verification. This wont protect against Man-In-The-Middle attacks')
            # Default is RejectPolicy
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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

        connect_kwargs = dict(
            hostname=self.remote_host,
            username=self.username,
            timeout=self.conn_timeout,
            compress=self.compress,
            port=self.port,
            sock=self.host_proxy,
            look_for_keys=self.look_for_keys,
        )

        if self.password:
            password = self.password.strip()
            connect_kwargs.update(password=password)

        if self.pkey:
            connect_kwargs.update(pkey=self.pkey)

        if self.key_file:
            connect_kwargs.update(key_filename=self.key_file)

        client.connect(**connect_kwargs)

        if self.keepalive_interval:
            client.get_transport().set_keepalive(self.keepalive_interval)

        self.client = client
        return client

    def __enter__(self) -> 'SSHHook':
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
        self, remote_port: int, remote_host: str = "localhost", local_port: Optional[int] = None
    ) -> SSHTunnelForwarder:
        """
        Creates a tunnel between two hosts. Like ssh -L <LOCAL_PORT>:host:<REMOTE_PORT>.

        :param remote_port: The remote port to create a tunnel to
        :type remote_port: int
        :param remote_host: The remote host to create a tunnel to (default localhost)
        :type remote_host: str
        :param local_port:  The local port to attach the tunnel to
        :type local_port: int

        :return: sshtunnel.SSHTunnelForwarder object
        """
        if local_port:
            local_bind_address: Union[Tuple[str, int], Tuple[str]] = ('localhost', local_port)
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
                host_pkey_directories=[],
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

    def _pkey_from_private_key(self, private_key: str, passphrase: Optional[str] = None) -> paramiko.PKey:
        """
        Creates appropriate paramiko key for given private key

        :param private_key: string containing private key
        :return: ``paramiko.PKey`` appropriate for given key
        :raises AirflowException: if key cannot be read
        """
        for pkey_class in self._pkey_loaders:
            try:
                key = pkey_class.from_private_key(StringIO(private_key), password=passphrase)
                # Test it acutally works. If Paramiko loads an openssh generated key, sometimes it will
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
