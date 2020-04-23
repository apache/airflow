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
import getpass
import os
import warnings
from io import StringIO
from typing import Optional, TextIO

import paramiko
from paramiko.config import SSH_PORT
from sshtunnel import SSHTunnelForwarder

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class SSHHook(BaseHook):
    """
    Hook for ssh remote execution using Paramiko.
    ref: https://github.com/paramiko/paramiko
    This hook also lets you create ssh tunnel and serve as basis for SFTP file transfer

    :param ssh_conn_id: connection id from airflow Connections from where all the required
        parameters can be fetched like username, password or key_file.
        Thought the priority is given to the param passed during init
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
    :param timeout: timeout for the attempt to connect to the remote_host.
    :type timeout: int
    :param keepalive_interval: send a keepalive packet to remote host every
        keepalive_interval seconds
    :type keepalive_interval: int
    """

    def __init__(self,
                 ssh_conn_id=None,
                 remote_host=None,
                 username=None,
                 password=None,
                 key_file=None,
                 port=None,
                 timeout=10,
                 keepalive_interval=30
                 ):
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.username = username
        self.password = password
        self.key_file = key_file
        self.pkey = None
        self.port = port
        self.timeout = timeout
        self.keepalive_interval = keepalive_interval

        # Default values, overridable from Connection
        self.compress = True
        self.no_host_key_check = True
        self.allow_host_key_change = False
        self.host_proxy = None
        self.host_key = None

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
                if private_key:
                    self.pkey = paramiko.RSAKey.from_private_key(StringIO(private_key))

                if "timeout" in extra_options:
                    self.timeout = int(extra_options["timeout"], 10)

                if "compress" in extra_options\
                        and str(extra_options["compress"]).lower() == 'false':
                    self.compress = False
                if "no_host_key_check" in extra_options\
                        and\
                        str(extra_options["no_host_key_check"]).lower() == 'false':
                    self.no_host_key_check = False
                if "allow_host_key_change" in extra_options\
                        and\
                        str(extra_options["allow_host_key_change"]).lower() == 'true':
                    self.allow_host_key_change = True
                if "host_key" in extra_options and self.no_host_key_check is False:
                    # self.update_host_in_known_hosts(
                    #     self.remote_host,
                    #     'ssh-rsa',
                    #     extra_options["host_key"]
                    # )
                    self.host_key = extra_options["host_key"]

        if self.pkey and self.key_file:
            raise AirflowException(
                "Params key_file and private_key both provided.  Must provide no more than one.")

        if not self.remote_host:
            raise AirflowException("Missing required param: remote_host")

        # Auto detecting username values from system
        if not self.username:
            self.log.debug(
                "username to ssh to host: %s is not specified for connection id"
                " %s. Using system's default provided by getpass.getuser()",
                self.remote_host, self.ssh_conn_id
            )
            self.username = getpass.getuser()

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
            self.log.warning('Remote Identification Change is not verified. '
                             'This wont protect against Man-In-The-Middle attacks')
            client.load_system_host_keys()
        if self.no_host_key_check:
            self.log.warning('No Host Key Verification. This wont protect '
                             'against Man-In-The-Middle attacks')
            # Default is RejectPolicy
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        else:
            if self.host_key is not None:
                client_host_keys = client.get_host_keys()
                client_host_keys.add(self.remote_host, 'ssh-rsa', self.host_key)
                if not client_host_keys.check(self.remote_host, self.host_key):
                    self.log.warning('host_key key was not added - could be invalid.')
            else:
                self.log.warning('No public key supplied for remote_host. '
                                 'Please set a value for "host_key" in SSH Connection extras.')
        connect_kwargs = dict(
            hostname=self.remote_host,
            username=self.username,
            timeout=self.timeout,
            compress=self.compress,
            port=self.port,
            sock=self.host_proxy
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

    def __enter__(self):
        warnings.warn('The contextmanager of SSHHook is deprecated.'
                      'Please use get_conn() as a contextmanager instead.'
                      'This method will be removed in Airflow 2.0',
                      category=DeprecationWarning)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()
            self.client = None

    def get_tunnel(self, remote_port, remote_host="localhost", local_port=None):
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
            local_bind_address = ('localhost', local_port)
        else:
            local_bind_address = ('localhost',)

        tunnel_kwargs = dict(
            ssh_port=self.port,
            ssh_username=self.username,
            ssh_pkey=self.key_file or self.pkey,
            ssh_proxy=self.host_proxy,
            local_bind_address=local_bind_address,
            remote_bind_address=(remote_host, remote_port),
            logger=self.log
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
        self,
        local_port: int,
        remote_port: Optional[int] = None,
        remote_host: str = "localhost"
    ) -> SSHTunnelForwarder:
        """
        Creates tunnel for SSH connection [Deprecated].

        :param local_port: local port number
        :param remote_port: remote port number
        :param remote_host: remote host
        :return:
        """
        warnings.warn('SSHHook.create_tunnel is deprecated, Please'
                      'use get_tunnel() instead. But please note that the'
                      'order of the parameters have changed'
                      'This method will be removed in Airflow 2.0',
                      category=DeprecationWarning)

        return self.get_tunnel(remote_port, remote_host, local_port)

    @staticmethod
    def update_host_in_known_hosts(host: str, key_type: str, host_key: str) -> None:
        """
        Adds a specified remote_host public key to the known_hosts file
            in order to prevent man-in-the-middle attacks.

        If the host doesn't exist in the file then a new record is added.
            If a record exists with a matching host but a different key,
            then the existing record will be deleted and replaced with the new key.

        The format of the new line in known_hosts will be:
        {host} {key_type} {host_key}\n
        So, for these inputs:
        SSHHook.add_host_to_known_hosts('example.com', 'ssh-rsa', 'mjL4Bb/hFHx8OfTO...')

        We would expect to see a (new) line in ~/.ssh/known_hosts with the following:
        example.com ssh-rsa mjL4Bb/hFHx8OfTO...\n


        :param host: FQDN of the remote host.
        :type host: str
        :param key_type: The algorithm format of the provided public key.
        :type key_type: str
        :param host_key: The base64-ecoded public key of the remote host.
        :type host_key: str
        """
        # The .ssh hidden directory is required and not present on all airflow deployments.
        try:
            known_hosts_file_ref = SSHHook._create_known_hosts()
        except PermissionError:
            raise AirflowException("The user running airflow on this system does not have the necessary "
                                   "permissions to make changes to the ~/.ssh directory and its contents.")

        with open(known_hosts_file_ref, 'r') as f:
            file_content = f.read()

        record = SSHHook._format_known_hosts_record(host, key_type, host_key)

        if record in file_content:
            # record already present in file; no update or append required
            pass
        elif host in file_content:
            # the host may be in the file with a different (possibly old) key
            # in this case, we should replace the existing record
            with open(known_hosts_file_ref, 'w+') as f:
                SSHHook._update_record_in_known_hosts(host, record, file_content, f)
        else:
            # in this case the file is empty or the record doesn't exist in the file so add it
            with open(known_hosts_file_ref, 'a') as f:
                SSHHook._add_new_record_to_known_hosts(record, f)

    @staticmethod
    def _format_known_hosts_record(host: str, key_type: str, public_key: str) -> str:
        return ' '.join([host, key_type, public_key])

    @staticmethod
    def _create_known_hosts() -> str:
        if not os.path.exists(os.path.expanduser('~/.ssh')):
            os.mkdir(os.path.expanduser('~/.ssh'))
        with open(os.path.expanduser('~/.ssh/known_hosts'), 'a') as f:
            f.write(str())
        return os.path.expanduser('~/.ssh/known_hosts')

    @staticmethod
    def _add_new_record_to_known_hosts(record: str, file: TextIO) -> None:
        file.write(''.join([record, '\n']))

    @staticmethod
    def _update_record_in_known_hosts(host: str, record: str, file_contents: str, file: TextIO) -> None:
        current_file_contents = str(file_contents)
        current_records_with_host_removed = [line for line in current_file_contents.split('\n')
                                             if host not in line and len(line) > 0]
        new_file_records = current_records_with_host_removed + [record]
        new_file_contents = '\n'.join(new_file_records) + '\n'
        file.seek(0)
        file.write(new_file_contents)
