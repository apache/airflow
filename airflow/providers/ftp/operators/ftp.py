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
"""This module contains FTP operator."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ftp.hooks.ftp import FTPHook


class FTPOperation:
    """Operation that can be used with FTP"""

    PUT = 'put'
    GET = 'get'


class FTPOperator(BaseOperator):
    """
    FTPOperator for transferring files from remote host to local or vice a versa.
    This operator uses ftp_hook to open ftp transport channel that serve as basis
    for file transfer.

    :param ftp_conn_id: :ref:`ftp connection id<howto/connection:ftp>`
        from airflow Connections.
    :param ftp_hook: predefined FTPHook to use
        Either `ftp_hook` or `ftp_conn_id` needs to be provided.
    :param local_filepath: local file path to get or put. (templated)
    :param remote_filepath: remote file path to get or put. (templated)
    :param operation: specify operation 'get' or 'put', defaults to put
    :param create_intermediate_dirs: create missing intermediate directories when
        copying from remote to local and vice-versa. Default is False.

        Example: The following task would copy ``file.txt`` to the remote host
        at ``/tmp/tmp1/tmp2/`` while creating ``tmp``,``tmp1`` and ``tmp2`` if they
        don't exist. If the parameter is not passed it would error as the directory
        does not exist. ::

            put_file = FTPOperator(
                task_id="test_ftp",
                ftp_conn_id="ftp_default",
                local_filepath="/tmp/file.txt",
                remote_filepath="/tmp/tmp1/tmp2/file.txt",
                operation="put",
                create_intermediate_dirs=True,
                dag=dag
            )

    """

    template_fields: Sequence[str] = ('local_filepath', 'remote_filepath')

    def __init__(
        self,
        *,
        ftp_hook: FTPHook | None = None,
        ftp_conn_id: str | None = None,
        local_filepath: str | list[str],
        remote_filepath: str | list[str],
        operation: str = FTPOperation.PUT,
        create_intermediate_dirs: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ftp_hook = ftp_hook
        self.ftp_conn_id = ftp_conn_id
        self.operation = operation
        self.create_intermediate_dirs = create_intermediate_dirs

        self.local_filepath_was_str = False
        if isinstance(local_filepath, str):
            self.local_filepath = [local_filepath]
            self.local_filepath_was_str = True
        else:
            self.local_filepath = local_filepath

        if isinstance(remote_filepath, str):
            self.remote_filepath = [remote_filepath]
        else:
            self.remote_filepath = remote_filepath

        if len(self.local_filepath) != len(self.remote_filepath):
            raise ValueError(
                f'{len(self.local_filepath)} paths in local_filepath '
                f'!= {len(self.remote_filepath)} paths in remote_filepath'
            )

        if not (self.operation.lower() == FTPOperation.GET or self.operation.lower() == FTPOperation.PUT):
            raise TypeError(
                f"Unsupported operation value {self.operation}, "
                f"expected {FTPOperation.GET} or {FTPOperation.PUT}."
            )

    def execute(self, context: Any) -> str | list[str] | None:
        file_msg = None
        try:
            if self.ftp_conn_id and not self.ftp_hook:
                self.log.info('ftp_hook not provided or invalid. Trying ftp_conn_id to create FTPHook.')
                self.ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

            if self.ftp_hook and not isinstance(self.ftp_hook, FTPHook):
                self.log.info('ftp_hook not provided or invalid. Trying ftp_conn_id to create FTPHook.')
                self.ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

            if not self.ftp_hook:
                raise AirflowException("Cannot operate without ftp_hook or ftp_conn_id.")

            for local_filepath, remote_filepath in zip(self.local_filepath, self.remote_filepath):
                if self.operation.lower() == FTPOperation.GET:
                    local_folder = os.path.dirname(local_filepath)
                    if self.create_intermediate_dirs:
                        Path(local_folder).mkdir(parents=True, exist_ok=True)
                    file_msg = f"from {remote_filepath} to {local_filepath}"
                    self.log.info("Starting to transfer %s", file_msg)
                    self.ftp_hook.retrieve_file(remote_filepath, local_filepath)
                else:
                    remote_folder = os.path.dirname(remote_filepath)
                    if self.create_intermediate_dirs:
                        self.ftp_hook.create_directory(remote_folder)
                    file_msg = f"from {local_filepath} to {remote_filepath}"
                    self.log.info("Starting to transfer file %s", file_msg)
                    self.ftp_hook.store_file(remote_filepath, local_filepath)

        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.local_filepath[0] if self.local_filepath_was_str else self.local_filepath
