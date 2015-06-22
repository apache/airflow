import logging

import os.path
import ftplib
from airflow.hooks.base_hook import BaseHook


class FTPHook(BaseHook):

    """
    Interact with FTP.
    """

    def __init__(
            self, ftp_conn_id='ftp_default'):
        self.ftp_conn_id = ftp_conn_id

    def get_conn(self):
        """
        Returns a FTP connection object
        """
        conn = self.get_connection(self.ftp_conn_id)
        conn = ftplib.FTP(conn.host, conn.login, conn.password)
        
        return conn

    def list_directory(self, path):
        """
        Returns a list of files on the remote system.

        :param path: full path to the remote directory to list
        :type path: str
        """
        conn = self.get_conn()
        conn.cwd(path)

        files = []

        try:
            files = conn.nlst()
        finally:
            conn.quit()

        return files

    def create_directory(self, path):
        """
        Creates a directory on the remote system.

        :param path: full path to the remote directory to create
        :type path: str
        """
        conn = self.get_conn()
        try:
            conn.mkd(path)
        finally:
            conn.quit()

    def delete_directory(self, path):
        """
        Deletes a directory on the remote system.

        :param path: full path to the remote directory to delete
        :type path: str
        """
        conn = self.get_conn()
        try:
            conn.rmd(path)
        finally:     
            conn.quit()

    def retrieve_file(self, remote_full_path, local_full_path):
        """
        Transfers the remote file to a local location.

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()
        try:
            output_handle = open(local_full_path, 'wb')

            remote_path, remote_file_name = os.path.split(remote_full_path)

            conn.cwd(remote_path)
            conn.retrbinary('RETR %s' % remote_file_name, output_handle.write)

        finally:
            output_handle.close()
            conn.quit()

    def store_file(self, remote_full_path, local_full_path):
        """
        Transfers a local file to the remote location.

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()
        try:
            input_handle = open(local_full_path, 'rb')

            remote_path, remote_file_name = os.path.split(remote_full_path)

            conn.cwd(remote_path)
            conn.storbinary('STOR %s' % remote_file_name, input_handle)
        finally:
            input_handle.close()
            conn.quit()

    def delete_file(self, path):
        """
        Removes a file on the FTP Server

        :param path: full path to the remote file
        :type path: str
        """
        conn = self.get_conn()
        try:
            conn.delete(path)
        finally:
            conn.quit()


