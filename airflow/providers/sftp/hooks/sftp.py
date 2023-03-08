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
"""This module contains SFTP hook."""
from __future__ import annotations

import datetime
import os
import stat
import warnings
from fnmatch import fnmatch
from typing import Any, Callable

import paramiko

from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook


class SFTPHook(SSHHook):
    """
    This hook is inherited from SSH hook. Please refer to SSH hook for the input
    arguments.

    Interact with SFTP.

    :Pitfalls::

        - In contrast with FTPHook describe_directory only returns size, type and
          modify. It doesn't return unix.owner, unix.mode, perm, unix.group and
          unique.
        - retrieve_file and store_file only take a local full path and not a
           buffer.
        - If no mode is passed to create_directory it will be created with 777
          permissions.

    Errors that may occur throughout but should be handled downstream.

    For consistency reasons with SSHHook, the preferred parameter is "ssh_conn_id".

    :param ssh_conn_id: The :ref:`sftp connection id<howto/connection:sftp>`
    :param ssh_hook: Optional SSH hook (included to support passing of an SSH hook to the SFTP operator)
    """

    conn_name_attr = "ssh_conn_id"
    default_conn_name = "sftp_default"
    conn_type = "sftp"
    hook_name = "SFTP"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "login": "Username",
            },
        }

    def __init__(
        self,
        ssh_conn_id: str | None = "sftp_default",
        ssh_hook: SSHHook | None = None,
        *args,
        **kwargs,
    ) -> None:
        self.conn: paramiko.SFTPClient | None = None

        # TODO: remove support for ssh_hook when it is removed from SFTPOperator
        self.ssh_hook = ssh_hook

        if self.ssh_hook is not None:
            warnings.warn(
                "Parameter `ssh_hook` is deprecated and will be removed in a future version.",
                DeprecationWarning,
                stacklevel=2,
            )
            if not isinstance(self.ssh_hook, SSHHook):
                raise AirflowException(
                    f"ssh_hook must be an instance of SSHHook, but got {type(self.ssh_hook)}"
                )
            self.log.info("ssh_hook is provided. It will be used to generate SFTP connection.")
            self.ssh_conn_id = self.ssh_hook.ssh_conn_id
            return

        ftp_conn_id = kwargs.pop("ftp_conn_id", None)
        if ftp_conn_id:
            warnings.warn(
                "Parameter `ftp_conn_id` is deprecated. Please use `ssh_conn_id` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            ssh_conn_id = ftp_conn_id

        kwargs["ssh_conn_id"] = ssh_conn_id
        self.ssh_conn_id = ssh_conn_id

        super().__init__(*args, **kwargs)

    def get_conn(self) -> paramiko.SFTPClient:  # type: ignore[override]
        """Opens an SFTP connection to the remote host"""
        if self.conn is None:
            # TODO: remove support for ssh_hook when it is removed from SFTPOperator
            if self.ssh_hook is not None:
                self.conn = self.ssh_hook.get_conn().open_sftp()
            else:
                self.conn = super().get_conn().open_sftp()
        return self.conn

    def close_conn(self) -> None:
        """Closes the SFTP connection"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def describe_directory(self, path: str) -> dict[str, dict[str, str | int | None]]:
        """
        Returns a dictionary of {filename: {attributes}} for all files
        on the remote system (where the MLSD command is supported).

        :param path: full path to the remote directory
        """
        conn = self.get_conn()
        flist = sorted(conn.listdir_attr(path), key=lambda x: x.filename)
        files = {}
        for f in flist:
            modify = datetime.datetime.fromtimestamp(f.st_mtime).strftime("%Y%m%d%H%M%S")  # type: ignore
            files[f.filename] = {
                "size": f.st_size,
                "type": "dir" if stat.S_ISDIR(f.st_mode) else "file",  # type: ignore
                "modify": modify,
            }
        return files

    def list_directory(self, path: str) -> list[str]:
        """
        Returns a list of files on the remote system.

        :param path: full path to the remote directory to list
        """
        conn = self.get_conn()
        files = sorted(conn.listdir(path))
        return files

    def mkdir(self, path: str, mode: int = 0o777) -> None:
        """
        Creates a directory on the remote system.
        The default mode is 0777, but on some systems, the current umask value is first masked out.

        :param path: full path to the remote directory to create
        :param mode: int permissions of octal mode for directory
        """
        conn = self.get_conn()
        conn.mkdir(path, mode=mode)

    def isdir(self, path: str) -> bool:
        """
        Checks if the path provided is a directory or not.

        :param path: full path to the remote directory to check
        """
        conn = self.get_conn()
        try:
            result = stat.S_ISDIR(conn.stat(path).st_mode)  # type: ignore
        except OSError:
            result = False
        return result

    def isfile(self, path: str) -> bool:
        """
        Checks if the path provided is a file or not.

        :param path: full path to the remote file to check
        """
        conn = self.get_conn()
        try:
            result = stat.S_ISREG(conn.stat(path).st_mode)  # type: ignore
        except OSError:
            result = False
        return result

    def create_directory(self, path: str, mode: int = 0o777) -> None:
        """
        Creates a directory on the remote system.
        The default mode is 0777, but on some systems, the current umask value is first masked out.

        :param path: full path to the remote directory to create
        :param mode: int permissions of octal mode for directory
        """
        conn = self.get_conn()
        if self.isdir(path):
            self.log.info("%s already exists", path)
            return
        elif self.isfile(path):
            raise AirflowException(f"{path} already exists and is a file")
        else:
            dirname, basename = os.path.split(path)
            if dirname and not self.isdir(dirname):
                self.create_directory(dirname, mode)
            if basename:
                self.log.info("Creating %s", path)
                conn.mkdir(path, mode=mode)

    def delete_directory(self, path: str) -> None:
        """
        Deletes a directory on the remote system.

        :param path: full path to the remote directory to delete
        """
        conn = self.get_conn()
        conn.rmdir(path)

    def retrieve_file(self, remote_full_path: str, local_full_path: str) -> None:
        """
        Transfers the remote file to a local location.
        If local_full_path is a string path, the file will be put
        at that location

        :param remote_full_path: full path to the remote file
        :param local_full_path: full path to the local file
        """
        conn = self.get_conn()
        conn.get(remote_full_path, local_full_path)

    def store_file(self, remote_full_path: str, local_full_path: str, confirm: bool = True) -> None:
        """
        Transfers a local file to the remote location.
        If local_full_path_or_buffer is a string path, the file will be read
        from that location

        :param remote_full_path: full path to the remote file
        :param local_full_path: full path to the local file
        """
        conn = self.get_conn()
        conn.put(local_full_path, remote_full_path, confirm=confirm)

    def delete_file(self, path: str) -> None:
        """
        Removes a file on the FTP Server

        :param path: full path to the remote file
        """
        conn = self.get_conn()
        conn.remove(path)

    def get_mod_time(self, path: str) -> str:
        """
        Returns modification time.

        :param path: full path to the remote file
        """
        conn = self.get_conn()
        ftp_mdtm = conn.stat(path).st_mtime
        return datetime.datetime.fromtimestamp(ftp_mdtm).strftime("%Y%m%d%H%M%S")  # type: ignore

    def path_exists(self, path: str) -> bool:
        """
        Returns True if a remote entity exists

        :param path: full path to the remote file or directory
        """
        conn = self.get_conn()
        try:
            conn.stat(path)
        except OSError:
            return False
        return True

    @staticmethod
    def _is_path_match(path: str, prefix: str | None = None, delimiter: str | None = None) -> bool:
        """
        Return True if given path starts with prefix (if set) and ends with delimiter (if set).

        :param path: path to be checked
        :param prefix: if set path will be checked is starting with prefix
        :param delimiter: if set path will be checked is ending with suffix
        :return: bool
        """
        if prefix is not None and not path.startswith(prefix):
            return False
        if delimiter is not None and not path.endswith(delimiter):
            return False
        return True

    def walktree(
        self,
        path: str,
        fcallback: Callable[[str], Any | None],
        dcallback: Callable[[str], Any | None],
        ucallback: Callable[[str], Any | None],
        recurse: bool = True,
    ) -> None:
        """
        Recursively descend, depth first, the directory tree rooted at
        path, calling discrete callback functions for each regular file,
        directory and unknown file type.

        :param str path:
            root of remote directory to descend, use '.' to start at
            :attr:`.pwd`
        :param callable fcallback:
            callback function to invoke for a regular file.
            (form: ``func(str)``)
        :param callable dcallback:
            callback function to invoke for a directory. (form: ``func(str)``)
        :param callable ucallback:
            callback function to invoke for an unknown file type.
            (form: ``func(str)``)
        :param bool recurse: *Default: True* - should it recurse

        :returns: None
        """
        conn = self.get_conn()
        for entry in self.list_directory(path):
            pathname = os.path.join(path, entry)
            mode = conn.stat(pathname).st_mode
            if stat.S_ISDIR(mode):  # type: ignore
                # It's a directory, call the dcallback function
                dcallback(pathname)
                if recurse:
                    # now, recurse into it
                    self.walktree(pathname, fcallback, dcallback, ucallback)
            elif stat.S_ISREG(mode):  # type: ignore
                # It's a file, call the fcallback function
                fcallback(pathname)
            else:
                # Unknown file type
                ucallback(pathname)

    def get_tree_map(
        self, path: str, prefix: str | None = None, delimiter: str | None = None
    ) -> tuple[list[str], list[str], list[str]]:
        """
        Return tuple with recursive lists of files, directories and unknown paths from given path.
        It is possible to filter results by giving prefix and/or delimiter parameters.

        :param path: path from which tree will be built
        :param prefix: if set paths will be added if start with prefix
        :param delimiter: if set paths will be added if end with delimiter
        :return: tuple with list of files, dirs and unknown items
        """
        files: list[str] = []
        dirs: list[str] = []
        unknowns: list[str] = []

        def append_matching_path_callback(list_: list[str]) -> Callable:
            return lambda item: list_.append(item) if self._is_path_match(item, prefix, delimiter) else None

        self.walktree(
            path=path,
            fcallback=append_matching_path_callback(files),
            dcallback=append_matching_path_callback(dirs),
            ucallback=append_matching_path_callback(unknowns),
            recurse=True,
        )

        return files, dirs, unknowns

    def test_connection(self) -> tuple[bool, str]:
        """Test the SFTP connection by calling path with directory"""
        try:
            conn = self.get_conn()
            conn.normalize(".")
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)

    def get_file_by_pattern(self, path, fnmatch_pattern) -> str:
        """
        Returning the first matching file based on the given fnmatch type pattern

        :param path: path to be checked
        :param fnmatch_pattern: The pattern that will be matched with `fnmatch`
        :return: string containing the first found file, or an empty string if none matched
        """
        for file in self.list_directory(path):
            if fnmatch(file, fnmatch_pattern):
                return file

        return ""

    def get_files_by_pattern(self, path, fnmatch_pattern) -> list[str]:
        """
        Returning the list of matching files based on the given fnmatch type pattern

        :param path: path to be checked
        :param fnmatch_pattern: The pattern that will be matched with `fnmatch`
        :return: list of string containing the found files, or an empty list if none matched
        """
        matched_files = []
        for file in self.list_directory(path):
            if fnmatch(file, fnmatch_pattern):
                matched_files.append(file)

        return matched_files
