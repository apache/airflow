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
from airflow.providers.microsoft.azure.hooks.azure_storage_blob import AzureStorageBlobHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AzureStorageBlobSensor(BaseSensorOperator):
    """
    Waits for a blob to arrive on Azure Blob Storage.

    :param container_name: Name of the container.
    :type container_name: str
    :param blob_name: Name of the blob.
    :type blob_name: str
    :param azure_blob_conn_id: Reference to the wasb connection.
    :type azure_blob_conn_id: str
    :param check_options: Optional keyword arguments that
        `AzureStorageBlobHook.check_copy_status()` takes.
    :type check_options: dict
    """

    template_fields = ('container_name', 'blob_name')

    @apply_defaults
    def __init__(self, container_name, blob_name,
                 azure_blob_conn_id='azure_blob_default',
                 check_options=None, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if check_options is None:
            check_options = {}
        self.azure_blob_conn_id = azure_blob_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.check_options = check_options

    def poke(self, context):
        self.log.info(
            'Poking for blob: %s\nin %s', self.blob_name, self.container_name
        )
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.azure_blob_conn_id)
        return hook.check_for_blob(self.container_name, self.blob_name,
                                   **self.check_options)
