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

import datetime
import ftplib
import os.path
from airflow.hooks.base_hook import BaseHook
from past.builtins import basestring

from airflow.utils.log.logging_mixin import LoggingMixin


def mlsd(conn, path="", facts=None):
    """
    BACKPORT FROM PYTHON3 FTPLIB.

    List a directory in a standardized format by using MLSD
    command (RFC-3659). If path is omitted the current directory
    is assumed. "facts" is a list of strings representing the type
    of information desired (e.g. ["type", "size", "perm"]).

    Return a generator object yielding a tuple of two elements
    for every file found in path.
    First element is the file name, the second one is a dictionary
    including a variable number of "facts" depending on the server
    and whether "facts" argument has been provided.
    """
    facts = facts or []
    if facts:
        conn.sendcmd("OPTS MLST " + ";".join(facts) + ";")
    if path:
        cmd = "MLSD %s" % path
    else:
        cmd = "MLSD"
    lines = []
    conn.retrlines(cmd, lines.append)
    for line in lines:
        facts_found, _, name = line.rstrip(ftplib.CRLF).partition(' ')
        entry = {}
        for fact in facts_found[:-1].split(";"):
            key, _, value = fact.partition("=")
            entry[key.lower()] = value
        yield (name, entry)


class FTPHook(BaseHook, LoggingMixin):
    """
    Interact with FTP.

    Errors that may occur throughout but should be handled
    downstream.
    """

    def __init__(self, ftp_conn_id='ftp_default'):
        self.ftp_conn_id = ftp_conn_id
        self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            self.close_conn()

    def get_conn(self):
        """
        Returns a FTP connection object
        """
        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)
            self.conn = ftplib.FTP(params.host, params.login, params.password)

        return self.conn

    def close_conn(self):
        """
        Closes the connection. An error will occur if the
        connection wasn't ever opened.
        """
        conn = self.conn
        conn.quit()
        self.conn = None

    def describe_directory(self, path):
        """
        Returns a dictionary of {filename: {attributes}} for all files
        on the remote system (where the MLSD command is supported).

        :param path: full path to the remote directory
        :type path: str
        """
        conn = self.get_conn()
        conn.cwd(path)
        try:
            # only works in Python 3
            files = dict(conn.mlsd())
        except AttributeError:
            files = dict(mlsd(conn))
        return files

    def list_directory(self, path, nlst=False):
        """
        Returns a list of files on the remote system.

        :param path: full path to the remote directory to list
        :type path: str
        """
        conn = self.get_conn()
        conn.cwd(path)

        files = conn.nlst()
        return files

    def create_directory(self, path):
        """
        Creates a directory on the remote system.

        :param path: full path to the remote directory to create
        :type path: str
        """
        conn = self.get_conn()
        conn.mkd(path)

    def delete_directory(self, path):
        """
        Deletes a directory on the remote system.

        :param path: full path to the remote directory to delete
        :type path: str
        """
        conn = self.get_conn()
        conn.rmd(path)

    def retrieve_file(
            self,
            remote_full_path,
            local_full_path_or_buffer,
            callback=None):
        """
        Transfers the remote file to a local location.

        If local_full_path_or_buffer is a string path, the file will be put
        at that location; if it is a file-like buffer, the file will
        be written to the buffer but not closed.

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path_or_buffer: full path to the local file or a
            file-like buffer
        :type local_full_path_or_buffer: str or file-like buffer
        :param callback: callback which is called each time a block of data
            is read. if you do not use a callback, these blocks will be written
            to the file or buffer passed in. if you do pass in a callback, note
            that writing to a file or buffer will need to be handled inside the
            callback.
            [default: output_handle.write()]
        :type callback: callable

        :Example::

            hook = FTPHook(ftp_conn_id='my_conn')

            remote_path = '/path/to/remote/file'
            local_path = '/path/to/local/file'

            # with a custom callback (in this case displaying progress on each read)
            def print_progress(percent_progress):
                self.log.info('Percent Downloaded: %s%%' % percent_progress)

            total_downloaded = 0
            total_file_size = hook.get_size(remote_path)
            output_handle = open(local_path, 'wb')
            def write_to_file_with_progress(data):
                total_downloaded += len(data)
                output_handle.write(data)
                percent_progress = (total_downloaded / total_file_size) * 100
                print_progress(percent_progress)
            hook.retrieve_file(remote_path, None, callback=write_to_file_with_progress)

            # without a custom callback data is written to the local_path
            hook.retrieve_file(remote_path, local_path)
        """
        conn = self.get_conn()

        is_path = isinstance(local_full_path_or_buffer, basestring)

        # without a callback, default to writing to a user-provided file or
        # file-like buffer
        if not callback:
            if is_path:
                output_handle = open(local_full_path_or_buffer, 'wb')
            else:
                output_handle = local_full_path_or_buffer
            callback = output_handle.write
        else:
            output_handle = None

        remote_path, remote_file_name = os.path.split(remote_full_path)
        conn.cwd(remote_path)
        self.log.info('Retrieving file from FTP: %s', remote_full_path)
        conn.retrbinary('RETR %s' % remote_file_name, callback)
        self.log.info('Finished retrieving file from FTP: %s', remote_full_path)

        if is_path and output_handle:
            output_handle.close()

    def store_file(self, remote_full_path, local_full_path_or_buffer):
        """
        Transfers a local file to the remote location.

        If local_full_path_or_buffer is a string path, the file will be read
        from that location; if it is a file-like buffer, the file will
        be read from the buffer but not closed.

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path_or_buffer: full path to the local file or a
            file-like buffer
        :type local_full_path_or_buffer: str or file-like buffer
        """
        conn = self.get_conn()

        is_path = isinstance(local_full_path_or_buffer, basestring)

        if is_path:
            input_handle = open(local_full_path_or_buffer, 'rb')
        else:
            input_handle = local_full_path_or_buffer
        remote_path, remote_file_name = os.path.split(remote_full_path)
        conn.cwd(remote_path)
        conn.storbinary('STOR %s' % remote_file_name, input_handle)

        if is_path:
            input_handle.close()

    def delete_file(self, path):
        """
        Removes a file on the FTP Server.

        :param path: full path to the remote file
        :type path: str
        """
        conn = self.get_conn()
        conn.delete(path)

    def rename(self, from_name, to_name):
        """
        Rename a file.

        :param from_name: rename file from name
        :param to_name: rename file to name
        """
        conn = self.get_conn()
        return conn.rename(from_name, to_name)

    def get_mod_time(self, path):
        """
        Returns a datetime object representing the last time the file was modified

        :param path: remote file path
        :type path: string
        """
        conn = self.get_conn()
        ftp_mdtm = conn.sendcmd('MDTM ' + path)
        time_val = ftp_mdtm[4:]
        # time_val optionally has microseconds
        try:
            return datetime.datetime.strptime(time_val, "%Y%m%d%H%M%S.%f")
        except ValueError:
            return datetime.datetime.strptime(time_val, '%Y%m%d%H%M%S')

    def get_size(self, path):
        """
        Returns the size of a file (in bytes)

        :param path: remote file path
        :type path: string
        """
        conn = self.get_conn()
        return conn.size(path)


class FTPSHook(FTPHook):

    def get_conn(self):
        """
        Returns a FTPS connection object.
        """
        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)

            if params.port:
                ftplib.FTP_TLS.port = params.port

            self.conn = ftplib.FTP_TLS(
                params.host, params.login, params.password
            )

        return self.conn
