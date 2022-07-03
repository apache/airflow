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

import warnings
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToADLSOperator(BaseOperator):
    """
    Upload file(s) to Azure Data Lake

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LocalFilesystemToADLSOperator`

    :param local_path: local path. Can be single file, directory (in which case,
            upload recursively) or glob pattern. Recursive glob patterns using `**`
            are not supported
    :param remote_path: Remote path to upload to; if multiple files, this is the
            directory root to write within
    :param nthreads: Number of threads to use. If None, uses the number of cores.
    :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten
    :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block
    :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk
    :param extra_upload_options: Extra upload options to add to the hook upload method
    :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection
    """

    template_fields: Sequence[str] = ("local_path", "remote_path")
    ui_color = '#e4f0e8'

    def __init__(
        self,
        *,
        local_path: str,
        remote_path: str,
        overwrite: bool = True,
        nthreads: int = 64,
        buffersize: int = 4194304,
        blocksize: int = 4194304,
        extra_upload_options: dict[str, Any] | None = None,
        azure_data_lake_conn_id: str = 'azure_data_lake_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.local_path = local_path
        self.remote_path = remote_path
        self.overwrite = overwrite
        self.nthreads = nthreads
        self.buffersize = buffersize
        self.blocksize = blocksize
        self.extra_upload_options = extra_upload_options
        self.azure_data_lake_conn_id = azure_data_lake_conn_id

    def execute(self, context: Context) -> None:
        if '**' in self.local_path:
            raise AirflowException("Recursive glob patterns using `**` are not supported")
        if not self.extra_upload_options:
            self.extra_upload_options = {}
        hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)
        self.log.info('Uploading %s to %s', self.local_path, self.remote_path)
        return hook.upload_file(
            local_path=self.local_path,
            remote_path=self.remote_path,
            nthreads=self.nthreads,
            overwrite=self.overwrite,
            buffersize=self.buffersize,
            blocksize=self.blocksize,
            **self.extra_upload_options,
        )


class LocalToAzureDataLakeStorageOperator(LocalFilesystemToADLSOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.microsoft.azure.transfers.local_to_adls.LocalFilesystemToADLSOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.microsoft.azure.transfers.local_to_adls.LocalFilesystemToADLSOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
