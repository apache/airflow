# -*- coding: utf-8 -*-
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

import paramiko

from airflow.hooks.base_hook import BaseHook


class SftpHook(BaseHook):
    """
    General hook for SFTP access to file system. This class is a wrapper around the paramiko library.

    If a connection id is specified, host, port, username, password will be taken from the predefined connection.
    Raises an airflow error if the given connection id doesn't exist.
    Otherwise host, port, username and password can be specified on the fly.

    :param sftp_conn_id: reference to predefined connection
    :type sftp_conn_id: string
    """

    def __init__(self, sftp_conn_id='ssh_default'):
        self.sftp_conn_id = sftp_conn_id

    def get_conn(self):
        """
        Returns http session for use with requests
        """
        conn = self.get_connection(self.sftp_conn_id)
        host = conn.host
        port = conn.port if conn.port else 22
        username = conn.port
        password = conn.password

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=host, port=port, username=username, password=password)

        return ssh_client.open_sftp()

    def get(self, local_filepath, remote_filepath):

        sftp_client = self.get_conn()
        sftp_client.get(local_filepath, remote_filepath)

    def put(self, local_filepath, remote_filepath):

        sftp_client = self.get_conn()
        sftp_client.put(local_filepath, remote_filepath)
