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
#
"""
This module contains integration with Azure Data Lake.

AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
Airflow connection of type `azure_data_lake` exists. Authorization can be done by supplying a
login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
(see connection `azure_data_lake_default` for an example).
"""
from typing import Optional

from azure.datalake.store import core, lib, multithread

from airflow.hooks.base_hook import BaseHook


class AzureDataLakeHook(BaseHook):
    """
    Interacts with Azure Data Lake.

    Client ID and client secret should be in user and password parameters.
    Tenant and account name should be extra field as
    {"tenant": "<TENANT>", "account_name": "ACCOUNT_NAME"}.

    :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection.
    :type azure_data_lake_conn_id: str
    """

    conn_name_attr = 'azure_data_lake_conn_id'
    default_conn_name = 'azure_data_lake_default'
    conn_type = 'azure_data_lake'

    def __init__(self, azure_data_lake_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_data_lake_conn_id
        self._conn: Optional[core.AzureDLFileSystem] = None
        self.account_name: Optional[str] = None

    def get_conn(self) -> core.AzureDLFileSystem:
        """Return a AzureDLFileSystem object."""
        if not self._conn:
            conn = self.get_connection(self.conn_id)
            service_options = conn.extra_dejson
            self.account_name = service_options.get('account_name')

            adl_creds = lib.auth(
                tenant_id=service_options.get('tenant'), client_secret=conn.password, client_id=conn.login
            )
            self._conn = core.AzureDLFileSystem(adl_creds, store_name=self.account_name)
            self._conn.connect()
        return self._conn

    def check_for_file(self, file_path: str) -> bool:
        """
        Check if a file exists on Azure Data Lake.

        :param file_path: Path and name of the file.
        :type file_path: str
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
        :type local_path: str
        :param remote_path: Remote path to upload to; if multiple files, this is the
            directory root to write within.
        :type remote_path: str
        :param nthreads: Number of threads to use. If None, uses the number of cores.
        :type nthreads: int
        :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten.
        :type overwrite: bool
        :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block.
        :type buffersize: int
        :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk.
        :type blocksize: int
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
        :type local_path: str
        :param remote_path: remote path/globstring to use to find remote files.
            Recursive glob patterns using `**` are not supported.
        :type remote_path: str
        :param nthreads: Number of threads to use. If None, uses the number of cores.
        :type nthreads: int
        :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten.
        :type overwrite: bool
        :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block.
        :type buffersize: int
        :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk.
        :type blocksize: int
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
        :type path: str
        """
        if "*" in path:
            return self.get_conn().glob(path)
        else:
            return self.get_conn().walk(path)
