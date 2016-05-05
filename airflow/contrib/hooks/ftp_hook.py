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
#

import datetime
import ftplib
import logging
import os.path
from airflow.hooks.base_hook import BaseHook
from past.builtins import basestring


def mlsd(conn, path="", facts=None):
    '''
    BACKPORT FROM PYTHON3 FTPLIB

    List a directory in a standardized format by using MLSD
    command (RFC-3659). If path is omitted the current directory
    is assumed. "facts" is a list of strings representing the type
    of information desired (e.g. ["type", "size", "perm"]).

    Return a generator object yielding a tuple of two elements
    for every file found in path.
    First element is the file name, the second one is a dictionary
    including a variable number of "facts" depending on the server
    and whether "facts" argument has been provided.
    '''
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


class FTPHook(BaseHook):

    """
    Interact with FTP.

    Errors that may occur throughout but should be handled
    downstream.
    """

    def __init__(self, ftp_conn_id='ftp_default'):
        self.ftp_conn_id = ftp_conn_id
        self.conn = None

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
        connection wasnt ever opened.
        """
        conn = self.conn
        conn.quit()

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

    def retrieve_file(self, remote_full_path, local_full_path_or_buffer):
        """
        Transfers the remote file to a local location.

        If local_full_path_or_buffer is a string path, the file will be put
        at that location; if it is a file-like buffer, the file will
        be written to the buffer but not closed.

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path_or_buffer: full path to the local file or a
            file-like buffer
        :type local_full_path: str or file-like buffer
        """
        conn = self.get_conn()

        is_path = isinstance(local_full_path_or_buffer, basestring)

        if is_path:
            output_handle = open(local_full_path_or_buffer, 'wb')
        else:
            output_handle = local_full_path_or_buffer

        remote_path, remote_file_name = os.path.split(remote_full_path)
        conn.cwd(remote_path)
        logging.info('Retrieving file from FTP: {}'.format(remote_full_path))
        conn.retrbinary('RETR %s' % remote_file_name, output_handle.write)
        logging.info('Finished etrieving file from FTP: {}'.format(
            remote_full_path))

        if is_path:
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
        Removes a file on the FTP Server

        :param path: full path to the remote file
        :type path: str
        """
        conn = self.get_conn()
        conn.delete(path)

    def get_mod_time(self, path):
        conn = self.get_conn()
        ftp_mdtm = conn.sendcmd('MDTM ' + path)
        return datetime.datetime.strptime(ftp_mdtm[4:], '%Y%m%d%H%M%S')


class FTPSHook(FTPHook):

    def get_conn(self):
        """
        Returns a FTPS connection object
        """
        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)
            self.conn = ftplib.FTP_TLS(
                params.host, params.login, params.password
            )
        return self.conn
