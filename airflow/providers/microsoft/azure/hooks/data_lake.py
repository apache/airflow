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
"""
This module contains integration with Azure Data Lake.

AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
Airflow connection of type `azure_data_lake` exists. Authorization can be done by supplying a
login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
(see connection `azure_data_lake_default` for an example).
"""
from __future__ import annotations

from typing import Any

from azure.datalake.store import core, lib, multithread

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AzureDataLakeHook(BaseHook):
    """
    Interacts with Azure Data Lake.

    Client ID and client secret should be in user and password parameters.
    Tenant and account name should be extra field as
    {"tenant": "<TENANT>", "account_name": "ACCOUNT_NAME"}.

    :param azure_data_lake_conn_id: Reference to the :ref:`Azure Data Lake connection<howto/connection:adl>`.
    """

    conn_name_attr = 'azure_data_lake_conn_id'
    default_conn_name = 'azure_data_lake_default'
    conn_type = 'azure_data_lake'
    hook_name = 'Azure Data Lake'

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure_data_lake__tenant": StringField(
                lazy_gettext('Azure Tenant ID'), widget=BS3TextFieldWidget()
            ),
            "extra__azure_data_lake__account_name": StringField(
                lazy_gettext('Azure DataLake Store Name'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'host', 'extra'],
            "relabeling": {
                'login': 'Azure Client ID',
                'password': 'Azure Client Secret',
            },
            "placeholders": {
                'login': 'client id',
                'password': 'secret',
                'extra__azure_data_lake__tenant': 'tenant id',
                'extra__azure_data_lake__account_name': 'datalake store',
            },
        }

    def __init__(self, azure_data_lake_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_data_lake_conn_id
        self._conn: core.AzureDLFileSystem | None = None
        self.account_name: str | None = None

    def get_conn(self) -> core.AzureDLFileSystem:
        """Return a AzureDLFileSystem object."""
        if not self._conn:
            conn = self.get_connection(self.conn_id)
            service_options = conn.extra_dejson
            self.account_name = service_options.get('account_name') or service_options.get(
                'extra__azure_data_lake__account_name'
            )
            tenant = service_options.get('tenant') or service_options.get('extra__azure_data_lake__tenant')

            adl_creds = lib.auth(tenant_id=tenant, client_secret=conn.password, client_id=conn.login)
            self._conn = core.AzureDLFileSystem(adl_creds, store_name=self.account_name)
            self._conn.connect()
        return self._conn

    def check_for_file(self, file_path: str) -> bool:
        """
        Check if a file exists on Azure Data Lake.

        :param file_path: Path and name of the file.
        :return: True if the file exists, False otherwise.
        :rtype: bool
        """
        try:
            files = self.get_conn().glob(file_path, details=False, invalidate_cache=True)
            return len(files) == 1
        except FileNotFoundError:
            return False

    def upload_file(
        self,
        local_path: str,
        remote_path: str,
        nthreads: int = 64,
        overwrite: bool = True,
        buffersize: int = 4194304,
        blocksize: int = 4194304,
        **kwargs,
    ) -> None:
        """
        Upload a file to Azure Data Lake.

        :param local_path: local path. Can be single file, directory (in which case,
            upload recursively) or glob pattern. Recursive glob patterns using `**`
            are not supported.
        :param remote_path: Remote path to upload to; if multiple files, this is the
            directory root to write within.
        :param nthreads: Number of threads to use. If None, uses the number of cores.
        :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten.
        :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block.
        :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk.
        """
        multithread.ADLUploader(
            self.get_conn(),
            lpath=local_path,
            rpath=remote_path,
            nthreads=nthreads,
            overwrite=overwrite,
            buffersize=buffersize,
            blocksize=blocksize,
            **kwargs,
        )

    def download_file(
        self,
        local_path: str,
        remote_path: str,
        nthreads: int = 64,
        overwrite: bool = True,
        buffersize: int = 4194304,
        blocksize: int = 4194304,
        **kwargs,
    ) -> None:
        """
        Download a file from Azure Blob Storage.

        :param local_path: local path. If downloading a single file, will write to this
            specific file, unless it is an existing directory, in which case a file is
            created within it. If downloading multiple files, this is the root
            directory to write within. Will create directories as required.
        :param remote_path: remote path/globstring to use to find remote files.
            Recursive glob patterns using `**` are not supported.
        :param nthreads: Number of threads to use. If None, uses the number of cores.
        :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten.
        :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block.
        :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk.
        """
        multithread.ADLDownloader(
            self.get_conn(),
            lpath=local_path,
            rpath=remote_path,
            nthreads=nthreads,
            overwrite=overwrite,
            buffersize=buffersize,
            blocksize=blocksize,
            **kwargs,
        )

    def list(self, path: str) -> list:
        """
        List files in Azure Data Lake Storage

        :param path: full path/globstring to use to list files in ADLS
        """
        if "*" in path:
            return self.get_conn().glob(path)
        else:
            return self.get_conn().walk(path)

    def remove(self, path: str, recursive: bool = False, ignore_not_found: bool = True) -> None:
        """
        Remove files in Azure Data Lake Storage

        :param path: A directory or file to remove in ADLS
        :param recursive: Whether to loop into directories in the location and remove the files
        :param ignore_not_found: Whether to raise error if file to delete is not found
        """
        try:
            self.get_conn().remove(path=path, recursive=recursive)
        except FileNotFoundError:
            if ignore_not_found:
                self.log.info("File %s not found", path)
            else:
                raise AirflowException(f"File {path} not found")
