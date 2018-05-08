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
#
import getpass
from winrm.protocol import Protocol
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class WinRMHook(BaseHook, LoggingMixin):

    """
    Hook for winrm remote execution using pywinrm.

    :param ssh_conn_id: connection id from airflow Connections from where all
        the required parameters can be fetched like username, password or key_file.
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
    :param keepalive_interval: send a keepalive packet to remote host
        every keepalive_interval seconds
    :type keepalive_interval: int
    """

    def __init__(self,
                 ssh_conn_id=None,
                 remote_host=None,
                 username=None,
                 password=None,
                 key_file=None,
                 timeout=10,
                 keepalive_interval=30
                 ):
        super(WinRMHook, self).__init__(ssh_conn_id)
        # TODO make new win rm connection class
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.username = username
        self.password = password
        self.key_file = key_file
        self.timeout = timeout
        self.keepalive_interval = keepalive_interval
        # Default values, overridable from Connection
        self.compress = True
        self.no_host_key_check = True
        self.client = None
        self.winrm_protocol = None

    def get_conn(self):
        if not self.client:
            self.log.debug('Creating WinRM client for conn_id: %s', self.ssh_conn_id)
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
                self.log.debug(
                    "username to ssh to host: %s is not specified for connection id"
                    " %s. Using system's default provided by getpass.getuser()",
                    self.remote_host, self.ssh_conn_id
                )
                self.username = getpass.getuser()

            try:

                if self.password and self.password.strip():
                    self.winrm_protocol = Protocol(
                        # TODO pass in port from ssh conn
                        endpoint='http://' + self.remote_host + ':5985/wsman',
                        # TODO get cert transport working
                        # transport='certificate',
                        transport='plaintext',
                        # cert_pem=r'publickey.pem',
                        # cert_key_pem=r'dev.pem',
                        read_timeout_sec=70,
                        operation_timeout_sec=60,
                        username=self.username,
                        password=self.password,
                        server_cert_validation='ignore')

                self.log.info("Opening WinRM shell")
                self.client = self.winrm_protocol.open_shell()

            except Exception as error:
                self.log.error(
                    "Error connecting to host: %s, error: %s",
                    self.remote_host, error
                )
        return self.client
