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

from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_data_lake import AzureDataLakeHook
from airflow.utils.decorators import apply_defaults


class LocalToAzureDataLakeStorageOperator(BaseOperator):
    """
    Upload file(s) to Azure Data Lake

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LocalToAzureDataLakeStorageOperator`

    :param local_path: local path. Can be single file, directory (in which case,
            upload recursively) or glob pattern. Recursive glob patterns using `**`
            are not supported
    :type local_path: str
    :param remote_path: Remote path to upload to; if multiple files, this is the
            directory root to write within
    :type remote_path: str
    :param nthreads: Number of threads to use. If None, uses the number of cores.
    :type nthreads: int
    :param overwrite: Whether to forcibly overwrite existing files/directories.
            If False and remote path is a directory, will quit regardless if any files
            would be overwritten or not. If True, only matching filenames are actually
            overwritten
    :type overwrite: bool
    :param buffersize: int [2**22]
            Number of bytes for internal buffer. This block cannot be bigger than
            a chunk and cannot be smaller than a block
    :type buffersize: int
    :param blocksize: int [2**22]
            Number of bytes for a block. Within each chunk, we write a smaller
            block for each API call. This block cannot be bigger than a chunk
    :type blocksize: int
    :param extra_upload_options: Extra upload options to add to the hook upload method
    :type extra_upload_options: dict
    :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection
    :type azure_data_lake_conn_id: str
    """

    template_fields = ("local_path", "remote_path")
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(
        self,
        *,
        local_path: str,
        remote_path: str,
        overwrite: bool = True,
        nthreads: int = 64,
        buffersize: int = 4194304,
        blocksize: int = 4194304,
        extra_upload_options: Optional[Dict[str, Any]] = None,
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

    def execute(self, context: dict) -> None:
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
