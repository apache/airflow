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

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_storage_blob import AzureStorageBlobHook
from airflow.utils.decorators import apply_defaults


class AzureStorageBlobDownloadOperator(BaseOperator):
    """
    Downloads blob from Azure Blob Storage.

    :param container_name: Name of the container. (templated)
    :type container_name: str

    :param source: Name of the blob or path to the blob (templated)
    :type source: str

    :param destination_path: The destination or name to use to save the file in file system (templated)
    :type destination_path: str

    :param azure_blob_conn_id: Reference to the wasb connection.
    :type azure_blob_conn_id: str

    :param offset: Start of byte range to use for downloading a section of the blob.
            Must be set if length is provided.
    :type offset: int

    :param length: Number of bytes to read from the stream. This is optional,
        but should be supplied for optimal performance.
    :type length: int
    :param download_options: Extra keyword arguments to supply to `AzureStorageBlobHook.download`
        method
    :type download_options: Optional[dict]

    :param search_options: Extra arguments to supply to `AzureStorageBlobHook.list` method
    :type search_options: Optional[dict]
    """
    template_fields = ('container_name', 'source', 'destination_path')

    ui_color = "#d6d6a6"

    @apply_defaults
    def __init__(self,
                 container_name: str,
                 source: str,
                 destination_path: str,
                 azure_blob_conn_id='azure_blob_default',
                 offset: Optional[int] = None,
                 length: Optional[int] = None,
                 download_options: Optional[dict] = None,
                 search_options: Optional[dict] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if download_options is None:
            download_options = {}
        if search_options is None:
            search_options = {}
        self.container_name = container_name
        self.source = source
        self.destination_path = destination_path
        self.azure_blob_conn_id = azure_blob_conn_id
        self.offset = offset
        self.length = length
        self.download_options = download_options
        self.search_options = search_options
        self.hook = self.get_hook()

    def execute(self, context):
        """Downloads a blob from Azure Blob Storage."""
        if self.length and not self.offset:
            raise AirflowException("offset must be set if length is provided.")

        blobs = self._path_to_files(hook=self.hook, path=self.source)
        if blobs:
            self._download_blobs(hook=self.hook,
                                 source=self.source,
                                 dest=self.destination_path,
                                 blobs=blobs)
        else:
            self._download_single_blobs(hook=self.hook,
                                        source=self.source,
                                        blob_dest=self.destination_path,
                                        blob_name=self.source)

    def _download_single_blobs(self, hook, source, blob_dest, blob_name):
        if blob_dest.endswith('.'):
            blob_dest += '/'
        if os.path.isdir(blob_dest) and not blob_dest.endswith('/'):
            blob_dest += '/'
        blob_dest = blob_dest + os.path.basename(source) if blob_dest.endswith('/') else blob_dest
        os.makedirs(os.path.dirname(blob_dest), exist_ok=True)
        self.log.info('Downloading %s to %s', source, blob_dest)
        with open(blob_dest, 'wb') as blob:
            stream = hook.download(self.container_name, blob_name,
                                   offset=self.offset, length=self.length,
                                   **self.download_options)
            blob.write(stream.readall())

    def _download_blobs(self, hook, source, dest, blobs):
        # if source is a directory, dest must also be a directory
        if not source == '' and not source.endswith('/'):
            source += '/'
        if not dest.endswith('/'):
            dest += '/'

        # append the directory name from source to the destination
        dest += os.path.basename(os.path.normpath(source)) + '/'

        blobs = [source + blob for blob in blobs]
        self.log.info("blobs: %s", blobs)
        for blob in blobs:
            self.log.info('destination: %s', dest)
            blob_dest = dest + os.path.relpath(blob, source)
            self.log.info('source: %s', source)
            self.log.info('destination: %s', blob_dest)
            self._download_single_blobs(hook=hook,
                                        source=source,
                                        blob_dest=blob_dest,
                                        blob_name=blob)

    def _path_to_files(self, hook, path):
        if not path == '' and not path.endswith('/'):
            path += '/'
        blob_names = hook.list(container_name=self.container_name,
                               name_starts_with=path,
                               **self.search_options)
        files = []

        for blob in blob_names:
            relative_path = os.path.relpath(blob, path)
            self.log.info('Path: %s', relative_path)
            if '/' not in relative_path:
                files.append(relative_path)
        self.log.info('Files to download %s', files)
        return files

    def get_hook(self):
        """
        Create and return an AzureStorageBlobHook.

        """
        return AzureStorageBlobHook(
            azure_blob_conn_id=self.azure_blob_conn_id
        )
