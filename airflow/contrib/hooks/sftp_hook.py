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

import datetime
import stat
from typing import Dict, List

import pysftp

from airflow.contrib.hooks.ssh_hook import SSHHook


class SFTPHook(SSHHook):
    """
    This hook is inherited from SSH hook. Please refer to SSH hook for the input
    arguments.

    Interact with SFTP. Aims to be interchangeable with FTPHook.

    :Pitfalls::

        - In contrast with FTPHook describe_directory only returns size, type and
          modify. It doesn't return unix.owner, unix.mode, perm, unix.group and
          unique.
        - retrieve_file and store_file only take a local full path and not a
           buffer.
        - If no mode is passed to create_directory it will be created with 777
          permissions.

    Errors that may occur throughout but should be handled downstream.
    """

    def __init__(self, ftp_conn_id: str = 'sftp_default', *args, **kwargs) -> None:
        kwargs['ssh_conn_id'] = ftp_conn_id
        super().__init__(*args, **kwargs)

        self.conn = None
        self.private_key_pass = None

        # Fail for unverified hosts, unless this is explicitly allowed
        self.no_host_key_check = False

        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)
            if conn.extra is not None:
                extra_options = conn.extra_dejson
                if 'private_key_pass' in extra_options:
                    self.private_key_pass = extra_options.get('private_key_pass', None)

                # For backward compatibility
                # TODO: remove in Airflow 2.1
                import warnings
                if 'ignore_hostkey_verification' in extra_options:
                    warnings.warn(
                        'Extra option `ignore_hostkey_verification` is deprecated.'
                        'Please use `no_host_key_check` instead.'
                        'This option will be removed in Airflow 2.1',
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    self.no_host_key_check = str(
                        extra_options['ignore_hostkey_verification']
                    ).lower() == 'true'

                if 'no_host_key_check' in extra_options:
                    self.no_host_key_check = str(
                        extra_options['no_host_key_check']).lower() == 'true'

                if 'private_key' in extra_options:
                    warnings.warn(
                        'Extra option `private_key` is deprecated.'
                        'Please use `key_file` instead.'
                        'This option will be removed in Airflow 2.1',
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    self.key_file = extra_options.get('private_key')

    def get_conn(self) -> pysftp.Connection:
        """
        Returns an SFTP connection object
        """
        if self.conn is None:
            cnopts = pysftp.CnOpts()
            if self.no_host_key_check:
                cnopts.hostkeys = None
            cnopts.compression = self.compress
            conn_params = {
                'host': self.remote_host,
                'port': self.port,
                'username': self.username,
                'cnopts': cnopts
            }
            if self.password and self.password.strip():
                conn_params['password'] = self.password
            if self.key_file:
                conn_params['private_key'] = self.key_file
            if self.private_key_pass:
                conn_params['private_key_pass'] = self.private_key_pass

            self.conn = pysftp.Connection(**conn_params)
        return self.conn

    def close_conn(self) -> None:
        """
        Closes the connection. An error will occur if the
        connection wasnt ever opened.
        """
        conn = self.conn
        conn.close()  # type: ignore
        self.conn = None

    def describe_directory(self, path: str) -> Dict[str, Dict[str, str]]:
        """
        Returns a dictionary of {filename: {attributes}} for all files
        on the remote system (where the MLSD command is supported).

        :param path: full path to the remote directory
        :type path: str
        """
        conn = self.get_conn()
        flist = conn.listdir_attr(path)
        files = {}
        for f in flist:
            modify = datetime.datetime.fromtimestamp(
                f.st_mtime).strftime('%Y%m%d%H%M%S')
            files[f.filename] = {
                'size': f.st_size,
                'type': 'dir' if stat.S_ISDIR(f.st_mode) else 'file',
                'modify': modify}
        return files

    def list_directory(self, path: str) -> List[str]:
        """
        Returns a list of files on the remote system.

        :param path: full path to the remote directory to list
        :type path: str
        """
        conn = self.get_conn()
        files = conn.listdir(path)
        return files

    def create_directory(self, path: str, mode: int = 777) -> None:
        """
        Creates a directory on the remote system.

        :param path: full path to the remote directory to create
        :type path: str
        :param mode: int representation of octal mode for directory
        """
        conn = self.get_conn()
        conn.makedirs(path, mode)

    def delete_directory(self, path: str) -> None:
        """
        Deletes a directory on the remote system.

        :param path: full path to the remote directory to delete
        :type path: str
        """
        conn = self.get_conn()
        conn.rmdir(path)

    def retrieve_file(self, remote_full_path: str, local_full_path: str) -> None:
        """
        Transfers the remote file to a local location.
        If local_full_path is a string path, the file will be put
        at that location

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()
        self.log.info('Retrieving file from FTP: %s', remote_full_path)
        conn.get(remote_full_path, local_full_path)
        self.log.info('Finished retrieving file from FTP: %s', remote_full_path)

    def store_file(self, remote_full_path: str, local_full_path: str) -> None:
        """
        Transfers a local file to the remote location.
        If local_full_path_or_buffer is a string path, the file will be read
        from that location

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()
        conn.put(local_full_path, remote_full_path)

    def delete_file(self, path: str) -> None:
        """
        Removes a file on the FTP Server

        :param path: full path to the remote file
        :type path: str
        """
        conn = self.get_conn()
        conn.remove(path)

    def get_mod_time(self, path: str) -> str:
        conn = self.get_conn()
        ftp_mdtm = conn.stat(path).st_mtime
        return datetime.datetime.fromtimestamp(ftp_mdtm).strftime('%Y%m%d%H%M%S')

    def path_exists(self, path):
        """
        Returns True if a remote entity exists

        :param path: full path to the remote file or directory
        :type path: str
        """
        conn = self.get_conn()
        return conn.exists(path)
