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
from __future__ import annotations

import datetime
import ftplib  # nosec: B402
import logging
from typing import Any, Callable

from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class FTPHook(BaseHook):
    """
    Interact with FTP.

    Errors that may occur throughout but should be handled downstream.
    You can specify mode for data transfers in the extra field of your
    connection as ``{"passive": "true"}``.

    :param ftp_conn_id: The :ref:`ftp connection id <howto/connection:ftp>`
        reference.
    """

    conn_name_attr = "ftp_conn_id"
    default_conn_name = "ftp_default"
    conn_type = "ftp"
    hook_name = "FTP"

    def __init__(self, ftp_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.ftp_conn_id = ftp_conn_id
        self.conn: ftplib.FTP | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.conn is not None:
            self.close_conn()

    def get_conn(self) -> ftplib.FTP:
        """Return an FTP connection object."""
        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)
            pasv = params.extra_dejson.get("passive", True)
            self.conn = ftplib.FTP()  # nosec: B321
            if params.host:
                port = ftplib.FTP_PORT
                if params.port is not None:
                    port = params.port
                logger.info("Connecting via FTP to %s:%d", params.host, port)
                self.conn.connect(params.host, port)
                if params.login:
                    self.conn.login(params.login, params.password)
            self.conn.set_pasv(pasv)

        return self.conn

    def close_conn(self):
        """Close the connection; an error will occur if the connection was never opened."""
        conn = self.conn
        conn.quit()
        self.conn = None

    def describe_directory(self, path: str) -> dict:
        """
        Return a dictionary of {filename: {attributes}} for all files on a remote system which supports MLSD.

        :param path: full path to the remote directory
        """
        conn = self.get_conn()
        return dict(conn.mlsd(path))

    def list_directory(self, path: str) -> list[str]:
        """
        Return a list of files on the remote system.

        :param path: full path to the remote directory to list
        """
        conn = self.get_conn()
        return conn.nlst(path)

    def create_directory(self, path: str) -> None:
        """
        Create a directory on the remote system.

        :param path: full path to the remote directory to create
        """
        conn = self.get_conn()
        conn.mkd(path)

    def delete_directory(self, path: str) -> None:
        """
        Delete a directory on the remote system.

        :param path: full path to the remote directory to delete
        """
        conn = self.get_conn()
        conn.rmd(path)

    def retrieve_file(
        self,
        remote_full_path: str,
        local_full_path_or_buffer: Any,
        callback: Callable | None = None,
        block_size: int = 8192,
    ) -> None:
        """
        Transfer the remote file to a local location.

        If local_full_path_or_buffer is a string path, the file will be put
        at that location; if it is a file-like buffer, the file will
        be written to the buffer but not closed.

        :param remote_full_path: full path to the remote file
        :param local_full_path_or_buffer: full path to the local file or a
            file-like buffer
        :param callback: callback which is called each time a block of data
            is read. if you do not use a callback, these blocks will be written
            to the file or buffer passed in. if you do pass in a callback, note
            that writing to a file or buffer will need to be handled inside the
            callback.
            [default: output_handle.write()]
        :param block_size: file is transferred in chunks of default size 8192
            or as set by user

        .. code-block:: python

            hook = FTPHook(ftp_conn_id="my_conn")

            remote_path = "/path/to/remote/file"
            local_path = "/path/to/local/file"


            # with a custom callback (in this case displaying progress on each read)
            def print_progress(percent_progress):
                self.log.info("Percent Downloaded: %s%%" % percent_progress)


            total_downloaded = 0
            total_file_size = hook.get_size(remote_path)
            output_handle = open(local_path, "wb")


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
        is_path = isinstance(local_full_path_or_buffer, str)

        # without a callback, default to writing to a user-provided file or
        # file-like buffer
        if not callback:
            if is_path:
                output_handle = open(local_full_path_or_buffer, "wb")
            else:
                output_handle = local_full_path_or_buffer

            callback = output_handle.write

        self.log.info("Retrieving file from FTP: %s", remote_full_path)
        conn.retrbinary(f"RETR {remote_full_path}", callback, block_size)
        self.log.info("Finished retrieving file from FTP: %s", remote_full_path)

        if is_path and output_handle:
            output_handle.close()

    def store_file(
        self, remote_full_path: str, local_full_path_or_buffer: Any, block_size: int = 8192
    ) -> None:
        """
        Transfers a local file to the remote location.

        If local_full_path_or_buffer is a string path, the file will be read
        from that location; if it is a file-like buffer, the file will
        be read from the buffer but not closed.

        :param remote_full_path: full path to the remote file
        :param local_full_path_or_buffer: full path to the local file or a
            file-like buffer
        :param block_size: file is transferred in chunks of default size 8192
            or as set by user
        """
        conn = self.get_conn()
        is_path = isinstance(local_full_path_or_buffer, str)

        if is_path:
            input_handle = open(local_full_path_or_buffer, "rb")
        else:
            input_handle = local_full_path_or_buffer

        conn.storbinary(f"STOR {remote_full_path}", input_handle, block_size)

        if is_path:
            input_handle.close()

    def delete_file(self, path: str) -> None:
        """
        Remove a file on the FTP Server.

        :param path: full path to the remote file
        """
        conn = self.get_conn()
        conn.delete(path)

    def rename(self, from_name: str, to_name: str) -> str:
        """
        Rename a file.

        :param from_name: rename file from name
        :param to_name: rename file to name
        """
        conn = self.get_conn()
        return conn.rename(from_name, to_name)

    def get_mod_time(self, path: str) -> datetime.datetime:
        """
        Return a datetime object representing the last time the file was modified.

        :param path: remote file path
        """
        conn = self.get_conn()
        ftp_mdtm = conn.sendcmd("MDTM " + path)
        time_val = ftp_mdtm[4:]
        # time_val optionally has microseconds
        try:
            return datetime.datetime.strptime(time_val, "%Y%m%d%H%M%S.%f")
        except ValueError:
            return datetime.datetime.strptime(time_val, "%Y%m%d%H%M%S")

    def get_size(self, path: str) -> int | None:
        """
        Return the size of a file (in bytes).

        :param path: remote file path
        """
        conn = self.get_conn()
        size = conn.size(path)
        return int(size) if size else None

    def test_connection(self) -> tuple[bool, str]:
        """Test the FTP connection by calling path with directory."""
        try:
            conn = self.get_conn()
            conn.pwd
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)


class FTPSHook(FTPHook):
    """Interact with FTPS."""

    def get_conn(self) -> ftplib.FTP:
        """Return an FTPS connection object."""
        import ssl

        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)
            pasv = params.extra_dejson.get("passive", True)

            if params.port:
                ftplib.FTP_TLS.port = params.port

            # Construct FTP_TLS instance with SSL context to allow certificates to be validated by default
            context = ssl.create_default_context()
            self.conn = ftplib.FTP_TLS(params.host, params.login, params.password, context=context)  # nosec: B321
            self.conn.set_pasv(pasv)

        return self.conn
