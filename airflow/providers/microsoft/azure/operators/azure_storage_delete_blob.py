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
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_storage_blob import AzureStorageBlobHook
from airflow.utils.decorators import apply_defaults


class AzureDeleteBlobOperator(BaseOperator):
    """
    Deletes blob(s) on Azure Blob Storage.

    :param container_name: Name of the container. (templated)
    :type container_name: str
    :param blob_name: Name of the blob. (templated)
    :type blob_name: str
    :param azure_blob_conn_id: Reference to the blob connection.
    :type azure_blob_conn_id: str
    :param check_options: Optional keyword arguments that
        `AzureStorageBlobHook.list` takes.
    :type check_options: dict
    :param delete_options: Optional keyword arguments that
        `AzureStorageBlobHook.delete_blob` takes.
    :type delete_options: dict
    :param is_prefix: If blob_name is a prefix, delete all files matching prefix.
    :type is_prefix: bool

    """

    template_fields = ('container_name', 'blob_name')

    @apply_defaults
    def __init__(self, container_name,
                 blob_name,
                 azure_blob_conn_id='azure_blob_default',
                 check_options=None,
                 delete_options=None,
                 is_prefix=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if check_options is None:
            check_options = {}
        if delete_options is None:
            delete_options = {}
        self.azure_blob_conn_id = azure_blob_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.check_options = check_options
        self.delete_options = delete_options
        self.is_prefix = is_prefix

    def execute(self, context):
        """ Delete blobs from Azure Storage Blob container"""
        self.log.info(
            'Deleting blob %s from %s container', self.blob_name, self.container_name
        )
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.azure_blob_conn_id)
        if self.is_prefix:
            blobs = hook.list(self.container_name, name_starts_with=self.blob_name,
                              **self.check_options)
            hook.delete_blobs(self.container_name, *blobs, **self.delete_options)
        else:
            hook.delete_blob(self.container_name, self.blob_name,
                             **self.delete_options)
