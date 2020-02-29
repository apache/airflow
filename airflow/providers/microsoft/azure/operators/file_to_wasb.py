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
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.decorators import apply_defaults


class FileToWasbOperator(BaseOperator):
    """
    Uploads a file to Azure Blob Storage.

    :param file_path: Path to the file to load. (templated)
    :type file_path: str
    :param container_name: Name of the container. (templated)
    :type container_name: str
    :param blob_name: Name of the blob. (templated)
    :type blob_name: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param load_options: Optional keyword arguments that
        `WasbHook.load_file()` takes.
    :type load_options: dict
    """
    template_fields = ('file_path', 'container_name', 'blob_name')

    @apply_defaults
    def __init__(self, file_path, container_name, blob_name,
                 wasb_conn_id='wasb_default', load_options=None, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if load_options is None:
            load_options = {}
        self.file_path = file_path
        self.container_name = container_name
        self.blob_name = blob_name
        self.wasb_conn_id = wasb_conn_id
        self.load_options = load_options

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        self.log.info(
            'Uploading %s to wasb://%s as %s',
            self.file_path, self.container_name, self.blob_name,
        )
        hook.load_file(self.file_path, self.container_name,
                       self.blob_name, **self.load_options)
