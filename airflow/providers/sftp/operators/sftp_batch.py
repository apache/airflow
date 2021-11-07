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
"""This module contains SFTP Batch operator."""
import os
import re
from pathlib import Path
from typing import Any, List, Union

from paramiko.sftp_client import SFTPClient

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.sftp.operators.sftp import SFTPOperation, _check_conn, _make_intermediate_dirs


class SFTPBatchOperator(BaseOperator):
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
    :param local_path: local folder path to get or put. (templated)
    :type local_path: str or list
    :param remote_path: remote folder path to get or put. (templated)
    :type remote_path: str or list
    :param regexp_mask: regexp mask for file match in local_folder or remote_folder to get or put. (templated)
    :type regexp_mask: str
    :param operation: specify operation 'get' or 'put', defaults to put
    :type operation: str
    :param confirm: specify if the SFTP operation should be confirmed, defaults to True
    :type confirm: bool
    :param create_intermediate_dirs: create missing intermediate directories when
    :type create_intermediate_dirs: bool
        copying from remote to local and vice-versa. Default is False.
        Example: The following task would copy ``file.txt`` to the remote host
        at ``/tmp/tmp1/tmp2/`` while creating ``tmp``,``tmp1`` and ``tmp2`` if they
        don't exist. If the parameter is not passed it would error as the directory
        does not exist. ::
            put_dir_txt_files = SFTPOperator(
                task_id="put_dir_txt_files",
                ssh_conn_id="ssh_default",
                local_folder="/tmp/dir_for_remote_transfer/",
                remote_folder="/tmp/dir_for_remote_transfer/txt",
                regexp_mask=".*[.]txt",
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=True
            )

    """

    template_fields = (
        'remote_host',
        'local_path',
        'remote_path',
        'regexp_mask',
    )

    def __init__(
        self,
        *,
        ssh_hook=None,
        ssh_conn_id=None,
        remote_host=None,
        local_path: Union[str, list] = None,
        remote_path: Union[str, list] = None,
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
        self.local_path = local_path
        self.remote_path = remote_path
        self.regexp_mask = regexp_mask
        self.operation = operation
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs
        if not (self.operation.lower() == SFTPOperation.GET or self.operation.lower() == SFTPOperation.PUT):
            raise TypeError(
                f"""Unsupported operation value {self.operation},
                expected {SFTPOperation.GET} or {SFTPOperation.PUT}"""
            )
        if not (
            (isinstance(self.local_path, str) and isinstance(self.remote_path, str))
            or (isinstance(self.local_path, list) and isinstance(self.remote_path, str))
            or (isinstance(self.remote_path, list) and isinstance(self.local_path, str))
        ):
            raise TypeError(
                """Unsupported path argument value local_path and remote_path
                Possible options: \n local_path is str and remote_path is str\n
                local_path is list and remote_path is str\n
                local_path is str and remote_path is list"""
            )

    def execute(self, context: Any) -> str:
        file_msg = None
        try:
            _check_conn(self)

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                if self.operation.lower() == SFTPOperation.PUT:
                    if isinstance(self.local_path, str):
                        files_list = self._search_files(os.listdir(self.local_path))
                        for file in files_list:
                            local_file = os.path.basename(file)
                            file_msg = file
                            self._transfer(sftp_client, self.local_path, local_file, self.remote_path)
                    if isinstance(self.local_path, list) and isinstance(self.remote_path, str):
                        for file in self.local_path:
                            local_file = os.path.basename(file)
                            file_msg = file
                            self._transfer(sftp_client, os.path.dirname(file), local_file, self.remote_path)
                elif self.operation.lower() == SFTPOperation.GET:
                    if isinstance(self.remote_path, str):
                        files_list = self._search_files(sftp_client.listdir(self.remote_path))
                        for file in files_list:
                            remote_file = os.path.basename(file)
                            file_msg = file
                            self._transfer(sftp_client, self.local_path, remote_file, self.remote_path)
                    if isinstance(self.remote_path, list) and isinstance(self.local_path, str):
                        for file in self.remote_path:
                            remote_file = os.path.basename(file)
                            file_msg = file
                            self._transfer(sftp_client, self.local_path, remote_file, os.path.dirname(file))

        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.local_path

    def _search_files(self, files) -> List:
        if self.regexp_mask:
            files = list(filter(re.compile(self.regexp_mask).match, files))
        self.log.info("File for transfer: \n %s", files)
        return files

    def _transfer(self, sftp_client: SFTPClient, local_folder, file, remote_path) -> None:
        local_full_path = os.path.join(local_folder, file)
        remote_full_path = os.path.join(remote_path, file)
        if self.operation.lower() == SFTPOperation.GET:
            if self.create_intermediate_dirs:
                Path(local_folder).mkdir(parents=True, exist_ok=True)
            file_msg = f"from {remote_full_path} to {local_full_path}"
            self.log.info("Starting to transfer %s", file_msg)
            sftp_client.get(remote_full_path, local_full_path)
        else:
            if self.create_intermediate_dirs:
                _make_intermediate_dirs(
                    sftp_client=sftp_client,
                    remote_directory=remote_path,
                )
            file_msg = f"from {local_full_path} to {remote_full_path}"
            self.log.info("Starting to transfer file %s", file_msg)
            sftp_client.put(local_full_path, remote_full_path, confirm=self.confirm)
