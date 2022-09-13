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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ADLSDeleteOperator(BaseOperator):
    """
    Delete files in the specified path.

        .. seealso::
            For more information on how to use this operator, take a look at the guide:
            :ref:`howto/operator:ADLSDeleteOperator`

    :param path: A directory or file to remove
    :param recursive: Whether to loop into directories in the location and remove the files
    :param ignore_not_found: Whether to raise error if file to delete is not found
    :param azure_data_lake_conn_id: Reference to the :ref:`Azure Data Lake connection<howto/connection:adl>`.
    """

    template_fields: Sequence[str] = ('path',)
    ui_color = '#901dd2'

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

    def execute(self, context: Context) -> Any:
        hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)
        return hook.remove(path=self.path, recursive=self.recursive, ignore_not_found=self.ignore_not_found)


class ADLSListOperator(BaseOperator):
    """
    List all files from the specified path

    This operator returns a python list with the names of files which can be used by
     `xcom` in the downstream tasks.

    :param path: The Azure Data Lake path to find the objects. Supports glob
        strings (templated)
    :param azure_data_lake_conn_id: Reference to the :ref:`Azure Data Lake connection<howto/connection:adl>`.

    **Example**:
        The following Operator would list all the Parquet files from ``folder/output/``
        folder in the specified ADLS account ::

            adls_files = ADLSListOperator(
                task_id='adls_files',
                path='folder/output/*.parquet',
                azure_data_lake_conn_id='azure_data_lake_default'
            )
    """

    template_fields: Sequence[str] = ('path',)
    ui_color = '#901dd2'

    def __init__(
        self, *, path: str, azure_data_lake_conn_id: str = 'azure_data_lake_default', **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.azure_data_lake_conn_id = azure_data_lake_conn_id

    def execute(self, context: Context) -> list:
        hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)
        self.log.info('Getting list of ADLS files in path: %s', self.path)
        return hook.list(path=self.path)
