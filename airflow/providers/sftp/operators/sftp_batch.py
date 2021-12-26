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
from typing import Any, List

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
    :param local_files_path: local files path to get or put. (templated)
    :type local_files_path: list
    :param local_folder: local folder path to get or put. (templated)
    :type local_folder: str
    :param remote_folder: remote folder path to get or put. (templated)
    :type remote_folder: str
    :param remote_files_path: remote folder path to get or put. (templated)
    :type remote_files_path: list
    :param regexp_mask: regexp mask for file match in local_folder for PUT operational
        or match filenames in remote_folder for GET operational. (templated)
    :type regexp_mask: str
    :param operation: specify operation 'get' or 'put', defaults to put
    :type operation: str
    :param confirm: specify if the SFTP operation should be confirmed, defaults to True
    :type confirm: bool
    :param create_intermediate_dirs: create missing intermediate directories when
    :type create_intermediate_dirs: bool
    :param force: if the file already exists, it will be overwritten
    :type force: bool
        copying from remote to local and vice-versa. Default is False.
    Summary, support arguments:
        Possible options for PUT:
            1.optional(regexp_mask:str) + local_folder:str + remote_folder:str
            2.local_files_path:list + remote_folder:str
        Possible options for GET:
            1.local_folder:str + remote_folder:str + optional(regexp_mask:str)
            2.local_folder:str + remote_files_path:list
    Example:
    Move all txt files
        from local `/tmp/dir_for_local_transfer/` to remote folder `/tmp/dir_for_remote_transfer/`
            put_dir_txt_files = SFTPOperator(
                task_id="put_dir_txt_files",
                ssh_conn_id="ssh_default",
                local_folder="/tmp/dir_for_local_transfer/",
                remote_folder="/tmp/dir_for_remote_transfer/",
                regexp_mask=".*[.]txt",
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=True
            )
    Move `/tmp/file1.txt` file
        from local to remote folder `/tmp/dir_for_remote_transfer/`
            put_files = SFTPOperator(
                task_id="put_dir_txt_files",
                ssh_conn_id="ssh_default",
                local_files_path=["/tmp/file1.txt",],
                remote_folder="/tmp/dir_for_remote_transfer/",
                operation=SFTPOperation.PUT,
                create_intermediate_dirs=True
            )
    Move all files
        from remote folder `/tmp/dir_for_remote_transfer/` to local folder `/tmp/dir_for_local_transfer/`
            get_dir = SFTPOperator(
                task_id="put_dir_txt_files",
                ssh_conn_id="ssh_default",
                local_folder="/tmp/dir_for_local_transfer/",
                remote_folder="/tmp/dir_for_remote_transfer/",
                operation=SFTPOperation.GET,
                create_intermediate_dirs=True
            )
    Move `/tmp/file1.txt` file
        from remote to local folder `/tmp/dir_for_local_transfer/`
            get_files = SFTPOperator(
                task_id="put_dir_txt_files",
                ssh_conn_id="ssh_default",
                local_folder="/tmp/dir_for_local_transfer/",
                remote_files_path=["/tmp/file1.txt",],
                operation=SFTPOperation.GET,
                create_intermediate_dirs=True
            )
    """

    template_fields = (
        'remote_host',
        'local_files_path',
        'remote_files_path',
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
        local_files_path: list = None,
        remote_files_path: list = None,
        local_folder: str = None,
        remote_folder: str = None,
        regexp_mask=None,
        operation=SFTPOperation.PUT,
        confirm=True,
        create_intermediate_dirs=False,
        force=False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_files_path = local_files_path
        self.remote_files_path = remote_files_path
        self.local_folder = local_folder
        self.remote_folder = remote_folder
        self.regexp_mask = regexp_mask
        self.operation = operation
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs
        self.force = force
        if not (self.operation.lower() == SFTPOperation.GET or self.operation.lower() == SFTPOperation.PUT):
            raise TypeError(
                f"""Unsupported operation value {self.operation},
                expected {SFTPOperation.GET} or {SFTPOperation.PUT}"""
            )
        if not (
            (
                self.operation == SFTPOperation.PUT
                and (
                    (
                        isinstance(self.local_folder, str)
                        and isinstance(self.remote_folder, str)
                        and local_files_path is None
                        and remote_files_path is None
                    )
                    or (
                        isinstance(self.local_files_path, list)
                        and isinstance(self.remote_folder, str)
                        and self.local_folder is None
                        and remote_files_path is None
                        and self.regexp_mask is None
                    )
                )
            )
            or (
                self.operation == SFTPOperation.GET
                and (
                    (
                        isinstance(self.local_folder, str)
                        and isinstance(self.remote_folder, str)
                        and local_files_path is None
                        and remote_files_path is None
                    )
                    or (
                        isinstance(self.local_folder, str)
                        and isinstance(self.remote_files_path, list)
                        and self.local_files_path is None
                        and remote_folder is None
                        and self.regexp_mask is None
                    )
                )
            )
        ):
            raise TypeError(
                """
                Unsupported argument pool,
                Possible options for PUT:
                    1.optional(regexp_mask:str) + local_folder:str + remote_folder:str
                    2.local_files_path:list + remote_folder:str
                Possible options for GET:
                    1.local_folder:str + remote_folder:str + optional(regexp_mask:str)
                    2.local_folder:str + remote_files_path:list
                """
            )

    def execute(self, context: Any) -> str:
        dump_file_name_for_log = None
        try:
            _check_conn(self)

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                if self.operation.lower() == SFTPOperation.PUT:
                    if self.local_folder and self.remote_folder:
                        files_list = self._search_files(os.listdir(self.local_folder))
                        for file in files_list:
                            local_file = os.path.basename(file)
                            dump_file_name_for_log = file
                            self._check_remote_file(f"{self.remote_folder}/{local_file}", sftp_client)
                            self._transfer(sftp_client, self.local_folder, local_file, self.remote_folder)
                    if self.local_files_path and self.remote_folder:
                        for file in self.local_files_path:
                            local_file = os.path.basename(file)
                            dump_file_name_for_log = file
                            self._check_remote_file(f"{self.remote_folder}/{local_file}", sftp_client)
                            self._transfer(sftp_client, os.path.dirname(file), local_file, self.remote_folder)
                elif self.operation.lower() == SFTPOperation.GET:
                    if self.remote_folder and self.local_folder:
                        files_list = self._search_files(sftp_client.listdir(self.remote_folder))
                        for file in files_list:
                            remote_file = os.path.basename(file)
                            dump_file_name_for_log = file
                            self._check_local_file(f"{self.local_folder}/{remote_file}")
                            self._transfer(sftp_client, self.local_folder, remote_file, self.remote_folder)
                    if self.remote_files_path and self.local_folder:
                        for file in self.remote_files_path:
                            remote_file = os.path.basename(file)
                            dump_file_name_for_log = file
                            self._check_local_file(f"{self.local_folder}/{remote_file}")
                            self._transfer(sftp_client, self.local_folder, remote_file, os.path.dirname(file))

        except Exception as e:
            raise AirflowException(f"Error while transferring {dump_file_name_for_log}, error: {str(e)}")

        return self.local_folder

    def _search_files(self, files) -> List:
        if self.regexp_mask:
            files = list(filter(re.compile(self.regexp_mask).match, files))
        self.log.info("File for transfer: \n %s", files)
        return files

    def _transfer(self, sftp_client: SFTPClient, local_folder, file, remote_foolder) -> None:
        local_full_path = os.path.join(local_folder, file)
        remote_full_path = os.path.join(remote_foolder, file)
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
                    remote_directory=remote_foolder,
                )
            file_msg = f"from {local_full_path} to {remote_full_path}"
            self.log.info("Starting to transfer file %s", file_msg)
            sftp_client.put(local_full_path, remote_full_path, confirm=self.confirm)

    def _check_local_file(self, file_path):
        if self.force:
            return False
        if Path(file_path).exists():
            raise FileExistsError(f"File {file_path} is already exist! Turn 'force=True' for overwrite it")
        return False

    def _check_remote_file(self, file_path, sftp_client: SFTPClient):
        try:
            if self.force:
                return False
            if sftp_client.stat(file_path):
                raise FileExistsError(
                    f"File {file_path} is already exist! Turn 'force=True' for overwrite it"
                )
        except FileNotFoundError:
            return False
