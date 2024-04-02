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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.teradata.hooks.teradata import TeradataHook

import os

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureBlobStorageToTeradataOperator(BaseOperator):
    """
    Loads CSV, JSON and Parquet format data from Azure Blob Storage to Teradata.
    .. seealso::
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:AzureBlobStorageToTeradataOperator`
    :param blob_source_key: The path to the file (Azure Blob) that will be loaded into Teradata.
    :param wasb_conn_id: Reference to the wasb connection.
    :param teradata_table: destination table to insert rows.
    :param teradata_conn_id: :ref:`Teradata connection <howto/connection:Teradata>`.
    """

    template_fields: Sequence[str] = ("blob_source_key", "teradata_table")
    template_fields_renderers = {"blob_source_key": "sql", "teradata_table": "py"}
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        blob_source_key: str,
        wasb_conn_id: str = "wasb_default",
        teradata_table: str,
        teradata_conn_id: str = "teradata_default",
        wasb_extra_args: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.blob_source_key = blob_source_key
        self.wasb_conn_id = wasb_conn_id
        self.teradata_table = teradata_table
        self.teradata_conn_id = teradata_conn_id
        self.wasb_extra_args = wasb_extra_args or {}

    def execute(self, context: Context) -> None:
        """
        Executes the transfer operation from Azure Blob Storage to Teradata.
        :param context: The context that is being provided when executing.
        """
        self.log.info("Loading %s to Teradata table %s...", self.blob_source_key, self.teradata_table)

        # list all files in the Azure Blob Storage container
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id, **self.wasb_extra_args)
        access_key = wasb_hook.get_conn().token_credential.client_id
        access_secret = wasb_hook.get_conn().token_credential.client_secret

        if access_key is None or access_secret is None:
            access_key = ""
            access_secret = ""

        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        sql = f"""
                        CREATE MULTISET TABLE {self.teradata_table} AS
                        (
                            SELECT * FROM (
                                LOCATION = '{self.blob_source_key}'
                                ACCESS_ID= '{access_key}'
                                ACCESS_KEY= '{access_secret}'
                            ) AS d
                        ) WITH DATA
                        """
        self.log.info("COPYING using READ_NOS and CREATE TABLE AS feature of teradata....")
        self.log.info("sql : %s", sql)
        teradata_hook.run(sql)
        self.log.info("COPYING is completed")
