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
from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator

try:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
except ModuleNotFoundError as e:
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureBlobStorageToTeradataOperator(BaseOperator):
    """

    Loads CSV, JSON and Parquet format data from Azure Blob Storage to Teradata.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureBlobStorageToTeradataOperator`

    :param blob_source_key: The URI format specifying the location of the Azure blob object store.(templated)
        The URI format is `/az/YOUR-STORAGE-ACCOUNT.blob.core.windows.net/YOUR-CONTAINER/YOUR-BLOB-LOCATION`.
        Refer to
        https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US
    :param azure_conn_id: The Airflow WASB connection used for azure blob credentials.
    :param teradata_table: The name of the teradata table to which the data is transferred.(templated)
    :param teradata_conn_id: The connection ID used to connect to Teradata
        :ref:`Teradata connection <howto/connection:Teradata>`

    Note that ``blob_source_key`` and ``teradata_table`` are
    templated, so you can use variables in them if you wish.
    """

    template_fields: Sequence[str] = ("blob_source_key", "teradata_table")
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        blob_source_key: str,
        azure_conn_id: str = "azure_default",
        teradata_table: str,
        teradata_conn_id: str = "teradata_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.blob_source_key = blob_source_key
        self.azure_conn_id = azure_conn_id
        self.teradata_table = teradata_table
        self.teradata_conn_id = teradata_conn_id

    def execute(self, context: Context) -> None:
        self.log.info(
            "transferring data from %s to teradata table %s...", self.blob_source_key, self.teradata_table
        )
        azure_hook = WasbHook(wasb_conn_id=self.azure_conn_id)
        conn = azure_hook.get_connection(self.azure_conn_id)
        # Obtaining the Azure client ID and Azure secret in order to access a specified Blob container
        access_id = conn.login if conn.login is not None else ""
        access_secret = conn.password if conn.password is not None else ""
        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        sql = dedent(f"""
                    CREATE MULTISET TABLE {self.teradata_table}  AS
                    (
                        SELECT * FROM (
                            LOCATION = '{self.blob_source_key}'
                            ACCESS_ID= '{access_id}'
                            ACCESS_KEY= '{access_secret}'
                    ) AS d
                    ) WITH DATA
                """).rstrip()
        try:
            teradata_hook.run(sql, True)
        except Exception as ex:
            self.log.error(str(ex))
            raise
        self.log.info("The transfer of data from Azure Blob to Teradata was successful")
