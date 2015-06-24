import os.path
import ftplib
from airflow.hooks.base_hook import BaseHook


class FTPHook(BaseHook):

    """
    Interact with FTP.

    Errors that may occur throughout but should be handled
    downstream.
    """

    def __init__(
            self, ftp_conn_id='ftp_default'):
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

    def list_directory(self, path):
        """
        Returns a list of files on the remote system.

        :param path: full path to the remote directory to list
        :type path: str
        """
        conn = self.get_conn()
        conn.cwd(path)

        files = []
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

    def retrieve_file(self, remote_full_path, local_full_path):
        """
        Transfers the remote file to a local location.

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()

        output_handle = open(local_full_path, 'wb')
        remote_path, remote_file_name = os.path.split(remote_full_path)
        conn.cwd(remote_path)
        conn.retrbinary('RETR %s' % remote_file_name, output_handle.write)

        output_handle.close()

    def store_file(self, remote_full_path, local_full_path):
        """
        Transfers a local file to the remote location.

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()

        input_handle = open(local_full_path, 'rb')
        remote_path, remote_file_name = os.path.split(remote_full_path)
        conn.cwd(remote_path)
        conn.storbinary('STOR %s' % remote_file_name, input_handle)

        input_handle.close()

    def delete_file(self, path):
        """
        Removes a file on the FTP Server

        :param path: full path to the remote file
        :type path: str
        """
        conn = self.get_conn()
        conn.delete(path)
