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
#
# This is a port of Luigi's ssh implementation. All credits go there.

import paramiko

from airflow.hooks.base_hook import BaseHook


class SshHook(BaseHook):
    """
    General hook for SFTP access to file system. This class is a wrapper around the paramiko library.
    If a connection id is specified, host, port, username, password will be taken from the predefined connection.
    Raises an airflow error if the given connection id doesn't exist.
    :param ssh_conn_id: reference to predefined connection
    :type ssh_conn_id: string
    """

    def __init__(self, ssh_conn_id='ssh_default'):
        self.ssh_conn_id = ssh_conn_id

    def get_conn(self):
        """
        Returns ssh session
        """
        conn = self.get_connection(self.ssh_conn_id)
        host = conn.host
        port = conn.port if conn.port else 22
        username = conn.login
        password = conn.password

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=host, port=port, username=username, password=password)
        console = ssh_client.invoke_shell()
        console.keep_this = ssh_client

        return ssh_client

    def get(self, local_filepath, remote_filepath):
        """
        Copies remote_filepath from remote host to local_filepath on local host
        :param local_filepath: Full path to file on local filesystem
        :type local_filepath: string
        :param remote_filepath: Full path to file on remote filesystem
        :type remote_filepath: string
        """
        ssh_client = self.get_conn()
        stfp_client = ssh_client.open_sftp()
        stfp_client.get(remote_filepath, local_filepath)
        stfp_client.close()
        ssh_client.close()

    def put(self, local_filepath, remote_filepath):
        """
        Copies local_filepath from local host to remote_filepath on remote host
        :param local_filepath: Full path to file on local filesystem
        :type local_filepath: string
        :param remote_filepath: Full path to file on remote filesystem
        :type remote_filepath: string
        """
        ssh_client = self.get_conn()
        stfp_client = ssh_client.open_sftp()
        stfp_client.put(local_filepath, remote_filepath)
        stfp_client.close()
        ssh_client.close()

    def exec_command(self, cmd):
        """
        Execute a command at a remote host
        :param cmd: The command to be executed
        :type cmd: string
        """
        ssh_client = self.get_conn()
        ssh_client.exec_command(cmd)
        ssh_client.close()
