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

import stat
import pysftp
import logging
import datetime
from airflow.hooks.base_hook import BaseHook


class SFTPHook(BaseHook):
    """
    Interact with SFTP. Aims to be interchangeable with FTPHook.

    Pitfalls: - In contrast with FTPHook describe_directory only returns size, type and
                modify. It doesn't return unix.owner, unix.mode, perm, unix.group and
                unique.
              - retrieve_file and store_file only take a local full path and not a
                buffer.
              - If no mode is passed to create_directory it will be created with 777
                permissions.

    Errors that may occur throughout but should be handled downstream.
    """

    def __init__(self, ftp_conn_id='sftp_default'):
        self.ftp_conn_id = ftp_conn_id
        self.conn = None

    def get_conn(self):
        """
        Returns an SFTP connection object
        """
        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)
            cnopts = pysftp.CnOpts()
            if ('ignore_hostkey_verification' in params.extra_dejson and
                    params.extra_dejson['ignore_hostkey_verification']):
                cnopts.hostkeys = None
            conn_params = {
                'host': params.host,
                'port': params.port,
                'username': params.login,
                'cnopts': cnopts
            }
            if params.password is not None:
                conn_params['password'] = params.password
            if 'private_key' in params.extra_dejson:
                conn_params['private_key'] = params.extra_dejson['private_key']
            if 'private_key_pass' in params.extra_dejson:
                conn_params['private_key_pass'] = params.extra_dejson['private_key_pass']
            self.conn = pysftp.Connection(**conn_params)
        return self.conn

    def close_conn(self):
        """
        Closes the connection. An error will occur if the
        connection wasnt ever opened.
        """
        conn = self.conn
        conn.close()
        self.conn = None

    def describe_directory(self, path):
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

    def list_directory(self, path):
        """
        Returns a list of files on the remote system.
        :param path: full path to the remote directory to list
        :type path: str
        """
        conn = self.get_conn()
        files = conn.listdir(path)
        return files

    def create_directory(self, path, mode=777):
        """
        Creates a directory on the remote system.
        :param path: full path to the remote directory to create
        :type path: str
        :param mode: int representation of octal mode for directory
        """
        conn = self.get_conn()
        conn.mkdir(path, mode)

    def delete_directory(self, path):
        """
        Deletes a directory on the remote system.
        :param path: full path to the remote directory to delete
        :type path: str
        """
        conn = self.get_conn()
        conn.rmdir(path)

    def retrieve_file(self, remote_full_path, local_full_path):
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
        logging.info('Retrieving file from FTP: {}'.format(remote_full_path))
        conn.get(remote_full_path, local_full_path)
        logging.info('Finished retrieving file from FTP: {}'.format(
            remote_full_path))

    def store_file(self, remote_full_path, local_full_path):
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

    def delete_file(self, path):
        """
        Removes a file on the FTP Server
        :param path: full path to the remote file
        :type path: str
        """
        conn = self.get_conn()
        conn.remove(path)

    def get_mod_time(self, path):
        conn = self.get_conn()
        ftp_mdtm = conn.stat(path).st_mtime
        return datetime.datetime.fromtimestamp(ftp_mdtm).strftime('%Y%m%d%H%M%S')
