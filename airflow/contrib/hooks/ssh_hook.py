# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Ported to Airflow by Bolke de Bruin
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

import getpass
import os

import paramiko

from contextlib import contextmanager
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.LoggingMixin import LoggingMixin


class SSHHook(BaseHook, LoggingMixin):
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
    :param key_file: key file to use to connect to the remote_host.
    :type key_file: str
    :param timeout: timeout for the attempt to connect to the remote_host.
    :type timeout: int
    """

    def __init__(self,
                 ssh_conn_id=None,
                 remote_host=None,
                 username=None,
                 password=None,
                 key_file=None,
                 timeout=10
                 ):
        super(SSHHook, self).__init__(ssh_conn_id)
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.username = username
        self.password = password
        self.key_file = key_file
        self.timeout = timeout
        # Default values, overridable from Connection
        self.compress = True
        self.no_host_key_check = True
        self.client = None

    def get_conn(self):
        if not self.client:
            self.logger.debug('Creating SSH client for conn_id: %s', self.ssh_conn_id)
            if self.ssh_conn_id is not None:
                conn = self.get_connection(self.ssh_conn_id)
                if self.username is None:
                    self.username = conn.login
                if self.password is None:
                    self.password = conn.password
                if self.remote_host is None:
                    self.remote_host = conn.host
                if conn.extra is not None:
                    extra_options = conn.extra_dejson
                    self.key_file = extra_options.get("key_file")

                    if "timeout" in extra_options:
                        self.timeout = int(extra_options["timeout"], 10)

                    if "compress" in extra_options \
                            and extra_options["compress"].lower() == 'false':
                        self.compress = False
                    if "no_host_key_check" in extra_options \
                            and extra_options["no_host_key_check"].lower() == 'false':
                        self.no_host_key_check = False

            if not self.remote_host:
                raise AirflowException("Missing required param: remote_host")

            # Auto detecting username values from system
            if not self.username:
                self.logger.debug(
                    "username to ssh to host: %s is not specified for connection id"
                    " %s. Using system's default provided by getpass.getuser()",
                    self.remote_host, self.ssh_conn_id
                )
                self.username = getpass.getuser()

            host_proxy = None
            user_ssh_config_filename = os.path.expanduser('~/.ssh/config')
            if os.path.isfile(user_ssh_config_filename):
                ssh_conf = paramiko.SSHConfig()
                ssh_conf.parse(open(user_ssh_config_filename))
                host_info = ssh_conf.lookup(self.remote_host)
                if host_info and host_info.get('proxycommand'):
                    host_proxy = paramiko.ProxyCommand(host_info.get('proxycommand'))

                if not (self.password or self.key_file):
                    if host_info and host_info.get('identityfile'):
                        self.key_file = host_info.get('identityfile')[0]

            try:
                client = paramiko.SSHClient()
                client.load_system_host_keys()
                if self.no_host_key_check:
                    # Default is RejectPolicy
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                if self.password and self.password.strip():
                    client.connect(hostname=self.remote_host,
                                   username=self.username,
                                   password=self.password,
                                   timeout=self.timeout,
                                   compress=self.compress,
                                   sock=host_proxy)
                else:
                    client.connect(hostname=self.remote_host,
                                   username=self.username,
                                   key_filename=self.key_file,
                                   timeout=self.timeout,
                                   compress=self.compress,
                                   sock=host_proxy)

                self.client = client
            except paramiko.AuthenticationException as auth_error:
                self.logger.error(
                    "Auth failed while connecting to host: %s, error: %s",
                    self.remote_host, auth_error
                )
            except paramiko.SSHException as ssh_error:
                self.logger.error(
                    "Failed connecting to host: %s, error: %s",
                    self.remote_host, ssh_error
                )
            except Exception as error:
                self.logger.error(
                    "Error connecting to host: %s, error: %s",
                    self.remote_host, error
                )
        return self.client

    @contextmanager
    def create_tunnel(self, local_port, remote_port=None, remote_host="localhost"):
        """
        Creates a tunnel between two hosts. Like ssh -L <LOCAL_PORT>:host:<REMOTE_PORT>.
        Remember to close() the returned "tunnel" object in order to clean up
        after yourself when you are done with the tunnel.

        :param local_port:
        :type local_port: int
        :param remote_port:
        :type remote_port: int
        :param remote_host:
        :type remote_host: str
        :return:
        """

        import subprocess
        # this will ensure the connection to the ssh.remote_host from where the tunnel
        # is getting created
        self.get_conn()

        tunnel_host = "{0}:{1}:{2}".format(local_port, remote_host, remote_port)

        ssh_cmd = ["ssh", "{0}@{1}".format(self.username, self.remote_host),
                   "-o", "ControlMaster=no",
                   "-o", "UserKnownHostsFile=/dev/null",
                   "-o", "StrictHostKeyChecking=no"]

        ssh_tunnel_cmd = ["-L", tunnel_host,
                          "echo -n ready && cat"
                          ]

        ssh_cmd += ssh_tunnel_cmd
        self.logger.debug("Creating tunnel with cmd: %s", ssh_cmd)

        proc = subprocess.Popen(ssh_cmd,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        ready = proc.stdout.read(5)
        assert ready == b"ready", \
            "Did not get 'ready' from remote, got '{0}' instead".format(ready)
        yield
        proc.communicate()
        assert proc.returncode == 0, \
            "Tunnel process did unclean exit (returncode {}".format(proc.returncode)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()
