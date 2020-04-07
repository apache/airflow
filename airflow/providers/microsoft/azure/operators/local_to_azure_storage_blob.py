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
import os
from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_storage_blob import AzureStorageBlobHook
from airflow.utils.decorators import apply_defaults


class LocalFileSystemToAzureStorageBlobOperator(BaseOperator):
    """
    Uploads a file to Azure Blob Storage.

    :param container_name: Name of the container. (templated)
    :type container_name: str
    :param source_path: Path to the file to load. Can be a directory (templated)
    :type source_path: str
    :param destination_path: The destination to save the file in container (templated)
    :type destination_path: Optional[str]
    :type blob_name: Optional[str]
    :param azure_blob_conn_id: Reference to the wasb connection.
    :type azure_blob_conn_id: str
    :param blob_type: The type of the blob. This can be either BlockBlob,
        PageBlob or AppendBlob. The default value is BlockBlob.
    :type blob_type: storage.BlobType
    :param length: Number of bytes to read from the stream. This is optional,
        but should be supplied for optimal performance.
    :type length: int
    """
    template_fields = ('container_name', 'source_path', 'destination_path')

    @apply_defaults
    def __init__(self, container_name, source_path, destination_path: Optional[str] = None,
                 azure_blob_conn_id='azure_blob_default',
                 blob_type: Optional[str] = 'BlockBlob',
                 length: Optional[int] = None, load_options=None, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if load_options is None:
            load_options = {}
        self.container_name = container_name
        self.source_path = source_path
        self.destination_path = destination_path
        self.azure_blob_conn_id = azure_blob_conn_id
        self.blob_type = blob_type
        self.length = length
        self.load_options = load_options

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.azure_blob_conn_id)

        if os.path.isdir(self.source_path):
            # Take blob name from the files
            prefix = '' if self.destination_path == '' else self.destination_path + '/'
            prefix += os.path.basename(self.source_path) + '/'
            for root, dirs, files in os.walk(self.source_path):
                for name in files:
                    dir_part = os.path.relpath(root, self.source_path)
                    dir_part = '' if dir_part == '.' else dir_part + '/'
                    file_path = os.path.join(root, name)
                    blob_path = prefix + dir_part + name
                    self.log.info("Uploading %s to %s", file_path, blob_path)
                    self._upload_single_file(hook, file_path, blob_path)
        else:
            if not self.destination_path:
                self.destination_path = os.path.split(self.source_path)[-1]
            self.log.info("Uploading %s to %s", self.source_path, self.destination_path)
            self._upload_single_file(hook, self.source_path, self.destination_path)
            pass

    def _upload_single_file(self, hook, file_path, blob_name):
        with open(file_path, 'rb') as data:
            hook.upload(self.container_name, blob_name,
                        data, blob_type=self.blob_type, length=self.length,
                        **self.load_options)
