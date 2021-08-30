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
"""This module contains SFTP operator."""
import os
import re
from pathlib import Path
from typing import Any, List

from paramiko.sftp_client import SFTPClient

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook


class SFTPOperation:
    """Operation that can be used with SFTP/"""

    PUT = 'put'
    GET = 'get'


class SFTPOperator(BaseOperator):
    """
    SFTPOperator for transferring files from remote host to local or vice a versa.
    This operator uses ssh_hook to open sftp transport channel that serve as basis
    for file transfer.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: airflow.providers.ssh.hooks.ssh.SSHHook
    :param ssh_conn_id: :ref:`ssh connection id<howto/connection:ssh>`
        from airflow Connections. `ssh_conn_id` will be ignored if `ssh_hook`
        is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param local_filepath: local file path to get or put. (templated)
    :type local_filepath: str or List
    :param local_folder: local folder path to get or put. (templated)
    :type local_folder: str
    :param remote_filepath: remote file path to get or put. (templated)
    :type remote_filepath: str or List
    :param remote_folder: remote folder path to get or put. (templated)
    :type remote_folder: str
    :param regexp_mask: regexp mask for file match in local_folder or remote_folder to get or put. (templated)
    :type regexp_mask: str
    :param operation: specify operation 'get' or 'put', defaults to put
    :type operation: str
    :param confirm: specify if the SFTP operation should be confirmed, defaults to True
    :type confirm: bool
    :param create_intermediate_dirs: create missing intermediate directories when
        copying from remote to local and vice-versa. Default is False.

        Example: The following task would copy ``file.txt`` to the remote host
        at ``/tmp/tmp1/tmp2/`` while creating ``tmp``,``tmp1`` and ``tmp2`` if they
        don't exist. If the parameter is not passed it would error as the directory
        does not exist. ::
            # for transfer only one file
            put_file = SFTPOperator(
                task_id="put_file",
                ssh_conn_id="ssh_default",
                local_filepath="/tmp/file.txt",
                remote_filepath="/tmp/tmp1/tmp2/file.txt",
                operation="put",
                create_intermediate_dirs=True,
                dag=dag
            )
            # for transfer more files
            put_files = SFTPOperator(
                task_id="put_files",
                ssh_conn_id="ssh_default",
                local_filepath=["/tmp/file1.txt", "/tmp/file2.txt"],
                remote_filepath="/tmp/tmp1/tmp2/",
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=True
            )
            # for transfer all files in dir with pattern
            put_dir_txt_files = SFTPOperator(
                task_id="put_dir_txt_files",
                ssh_conn_id="ssh_default",
                local_folder="/tmp/dir_for_remote_transfer/",
                remote_folder="/tmp/dir_for_remote_transfer/txt",
                regexp_mask=".*.txt",
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=True
            )

    :type create_intermediate_dirs: bool
    """

    template_fields = (
        'local_filepath',
        'remote_filepath',
        'remote_host',
        'local_folder',
        'remote_folder',
        'regexp_mask',
    )

    def __init__(
        self,
        *,
        ssh_hook=None,
        ssh_conn_id=None,
        remote_host=None,
        local_filepath=None,
        remote_filepath=None,
        local_folder=None,
        remote_folder=None,
        regexp_mask=None,
        operation=SFTPOperation.PUT,
        confirm=True,
        create_intermediate_dirs=False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.local_folder = local_folder
        self.remote_folder = remote_folder
        self.regexp_mask = regexp_mask
        self.operation = operation
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs
        if not (self.operation.lower() == SFTPOperation.GET or self.operation.lower() == SFTPOperation.PUT):
            raise TypeError(
                "unsupported operation value {}, expected {} or {}".format(
                    self.operation, SFTPOperation.GET, SFTPOperation.PUT
                )
            )

    def execute(self, context: Any) -> str:
        file_msg = None
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                if self.local_filepath and self.remote_filepath:
                    if isinstance(self.local_filepath, list) and isinstance(self.remote_filepath, str):
                        for file_path in self.local_filepath:
                            local_folder = os.path.dirname(file_path)
                            local_file = os.path.basename(file_path)
                            file_msg = file_path
                            self._transfer(sftp_client, local_folder, local_file, self.remote_filepath)
                    elif isinstance(self.remote_filepath, list) and isinstance(self.local_filepath, str):
                        for file_path in self.remote_filepath:
                            remote_folder = os.path.dirname(file_path)
                            remote_file = os.path.basename(file_path)
                            file_msg = file_path
                            self._transfer(sftp_client, self.local_filepath, remote_file, remote_folder)
                    elif isinstance(self.remote_filepath, str) and isinstance(self.local_filepath, str):
                        local_folder = os.path.dirname(self.local_filepath)
                        file_msg = self.local_filepath
                        self._transfer(
                            sftp_client,
                            local_folder,
                            self.local_filepath,
                            self.remote_filepath,
                            only_file=True,
                        )
                elif self.local_folder and self.remote_folder:
                    if self.operation.lower() == SFTPOperation.PUT:
                        files_list = self._search_files(os.listdir(self.local_folder))
                        for file in files_list:
                            local_file = os.path.basename(file)
                            file_msg = file
                            self._transfer(sftp_client, self.local_folder, local_file, self.remote_folder)
                    elif self.operation.lower() == SFTPOperation.GET:
                        files_list = self._search_files(sftp_client.listdir(self.remote_folder))
                        for file in files_list:
                            remote_file = os.path.basename(file)
                            file_msg = file
                            self._transfer(sftp_client, self.local_folder, remote_file, self.remote_folder)
                else:
                    raise AirflowException(f"Argument mismatch, please read docs \n {SFTPOperator.__doc__}")

        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.local_filepath

    def _search_files(self, files) -> List:
        if self.regexp_mask:
            files = list(filter(re.compile(self.regexp_mask).match, files))
        self.log.info("File for transfer: \n %s", files)
        return files

    def _transfer(self, sftp_client: SFTPClient, local_folder, file, remote_path, only_file=False) -> None:
        local_full_path = os.path.join(local_folder, file)
        remote_full_path = os.path.join(remote_path, file)
        if self.operation.lower() == SFTPOperation.GET:
            if self.create_intermediate_dirs:
                Path(local_folder).mkdir(parents=True, exist_ok=True)
            file_msg = f"from {remote_full_path} to {local_full_path}"
            self.log.info("Starting to transfer %s", file_msg)
            if only_file:
                sftp_client.get(remote_path, file)
            else:
                sftp_client.get(remote_full_path, local_full_path)
        else:
            if self.create_intermediate_dirs:
                _make_intermediate_dirs(
                    sftp_client=sftp_client,
                    remote_directory=os.path.dirname(remote_path) if only_file else remote_path,
                )
            file_msg = f"from {local_full_path} to {remote_full_path}"
            self.log.info("Starting to transfer file %s", file_msg)
            if only_file:
                sftp_client.put(file, remote_path, confirm=self.confirm)
            else:
                sftp_client.put(local_full_path, remote_full_path, confirm=self.confirm)


def _make_intermediate_dirs(sftp_client, remote_directory) -> None:
    """
    Create all the intermediate directories in a remote host

    :param sftp_client: A Paramiko SFTP client.
    :param remote_directory: Absolute Path of the directory containing the file
    :return:
    """
    if remote_directory == '/':
        sftp_client.chdir('/')
        return
    if remote_directory == '':
        return
    try:
        sftp_client.chdir(remote_directory)
    except OSError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        _make_intermediate_dirs(sftp_client, dirname)
        sftp_client.mkdir(basename)
        sftp_client.chdir(basename)
        return
