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

from typing import Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_data_lake import AzureDataLakeHook
from airflow.utils.decorators import apply_defaults


class AzureDataLakeStorageDeleteOperator(BaseOperator):
    """
    Delete files in the specified path.

        .. seealso::
            For more information on how to use this operator, take a look at the guide:
            :ref:`howto/operator:AzureDataLakeStorageDeleteOperator`

    :param path: A directory or file to remove
    :type path: str
    :param recursive: Whether to loop into directories in the location and remove the files
    :type recursive: bool
    :param ignore_not_found: Whether to raise error if file to delete is not found
    :type ignore_not_found: bool
    """

    template_fields: Sequence[str] = ('path',)
    ui_color = '#901dd2'

    @apply_defaults
    def __init__(
        self,
        *,
        path: str,
        recursive: bool = False,
        ignore_not_found: bool = True,
        azure_data_lake_conn_id: str = 'azure_data_lake_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.recursive = recursive
        self.ignore_not_found = ignore_not_found
        self.azure_data_lake_conn_id = azure_data_lake_conn_id

    def execute(self, context: dict) -> Any:
        hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)

        return hook.remove(path=self.path, recursive=self.recursive, ignore_not_found=self.ignore_not_found)
