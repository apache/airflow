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

from collections.abc import Sequence
from textwrap import dedent
from typing import TYPE_CHECKING

try:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
except ModuleNotFoundError as e:
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


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
    :param public_bucket: Specifies whether the provided blob container is public. If the blob container is public,
        it means that anyone can access the objects within it via a URL without requiring authentication.
        If the bucket is private and authentication is not provided, the operator will throw an exception.
    :param azure_conn_id: The Airflow WASB connection used for azure blob credentials.
    :param teradata_table: The name of the teradata table to which the data is transferred.(templated)
    :param teradata_conn_id: The connection ID used to connect to Teradata
        :ref:`Teradata connection <howto/connection:Teradata>`
    :param teradata_authorization_name: The name of Teradata Authorization Database Object,
        is used to control who can access an Azure Blob object store.
        Refer to
        https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Setting-Up-Access/Controlling-Foreign-Table-Access-with-an-AUTHORIZATION-Object

    Note that ``blob_source_key`` and ``teradata_table`` are
    templated, so you can use variables in them if you wish.
    """

    template_fields: Sequence[str] = ("blob_source_key", "teradata_table")
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        blob_source_key: str,
        public_bucket: bool = False,
        azure_conn_id: str = "azure_default",
        teradata_table: str,
        teradata_conn_id: str = "teradata_default",
        teradata_authorization_name: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.blob_source_key = blob_source_key
        self.public_bucket = public_bucket
        self.azure_conn_id = azure_conn_id
        self.teradata_table = teradata_table
        self.teradata_conn_id = teradata_conn_id
        self.teradata_authorization_name = teradata_authorization_name

    def execute(self, context: Context) -> None:
        self.log.info(
            "transferring data from %s to teradata table %s...", self.blob_source_key, self.teradata_table
        )
        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        credentials_part = "ACCESS_ID= '' ACCESS_KEY= ''"
        if not self.public_bucket:
            # Accessing data directly from the Azure Blob Storage and creating permanent table inside the
            # database
            if self.teradata_authorization_name:
                credentials_part = f"AUTHORIZATION={self.teradata_authorization_name}"
            else:
                # Obtaining the Azure client ID and Azure secret in order to access a specified Blob container
                azure_hook = WasbHook(wasb_conn_id=self.azure_conn_id)
                conn = azure_hook.get_connection(self.azure_conn_id)
                access_id = conn.login
                access_secret = conn.password
                credentials_part = f"ACCESS_ID= '{access_id}' ACCESS_KEY= '{access_secret}'"
        sql = dedent(f"""
                        CREATE MULTISET TABLE {self.teradata_table} AS
                        (
                            SELECT * FROM (
                                LOCATION = '{self.blob_source_key}'
                                {credentials_part}
                            ) AS d
                        ) WITH DATA
                        """).rstrip()
        try:
            teradata_hook.run(sql, True)
        except Exception as ex:
            self.log.error(str(ex))
            raise
        self.log.info("The transfer of data from Azure Blob to Teradata was successful")
