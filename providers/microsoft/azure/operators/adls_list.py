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

from typing import Iterable

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_data_lake import AzureDataLakeHook
from airflow.utils.decorators import apply_defaults


class AzureDataLakeStorageListOperator(BaseOperator):
    """
    List all files from the specified path

    This operator returns a python list with the names of files which can be used by
     `xcom` in the downstream tasks.

    :param path: The Azure Data Lake path to find the objects. Supports glob
        strings (templated)
    :type path: str
    :param azure_data_lake_conn_id: The connection ID to use when
        connecting to Azure Data Lake Storage.
    :type azure_data_lake_conn_id: str

    **Example**:
        The following Operator would list all the Parquet files from ``folder/output/``
        folder in the specified ADLS account ::

            adls_files = AzureDataLakeStorageListOperator(
                task_id='adls_files',
                path='folder/output/*.parquet',
                azure_data_lake_conn_id='azure_data_lake_default'
            )
    """
    template_fields = ('path',)  # type: Iterable[str]
    ui_color = '#901dd2'

    @apply_defaults
    def __init__(self,
                 path,
                 azure_data_lake_conn_id='azure_data_lake_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.azure_data_lake_conn_id = azure_data_lake_conn_id

    def execute(self, context):

        hook = AzureDataLakeHook(
            azure_data_lake_conn_id=self.azure_data_lake_conn_id
        )

        self.log.info('Getting list of ADLS files in path: %s', self.path)

        return hook.list(path=self.path)
