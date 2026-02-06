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
"""This module contains Azure Data Explorer operators."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.microsoft.azure.hooks.adx import AzureDataExplorerHook

if TYPE_CHECKING:
    from azure.kusto.data._models import KustoResultTable

    from airflow.sdk import Context


class AzureDataExplorerQueryOperator(BaseOperator):
    """
    Operator for querying Azure Data Explorer (Kusto).

    :param query: KQL query to run (templated).
    :param database: Database to run the query on (templated).
    :param options: Optional query options. See:
      https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
    :param azure_data_explorer_conn_id: Reference to the
        :ref:`Azure Data Explorer connection<howto/connection:adx>`.
    """

    ui_color = "#00a1f2"
    template_fields: Sequence[str] = ("query", "database")
    template_ext: Sequence[str] = (".kql",)

    def __init__(
        self,
        *,
        query: str,
        database: str,
        options: dict | None = None,
        azure_data_explorer_conn_id: str = "azure_data_explorer_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.database = database
        self.options = options
        self.azure_data_explorer_conn_id = azure_data_explorer_conn_id

    @cached_property
    def hook(self) -> AzureDataExplorerHook:
        """Return new instance of AzureDataExplorerHook."""
        return AzureDataExplorerHook(self.azure_data_explorer_conn_id)

    def execute(self, context: Context) -> KustoResultTable | str:
        """
        Run KQL Query on Azure Data Explorer (Kusto).

        Returns `PrimaryResult` of Query v2 HTTP response contents.

        https://docs.microsoft.com/en-us/azure/kusto/api/rest/response2
        """
        response = self.hook.run_query(self.query, self.database, self.options)
        # TODO: Remove this after minimum Airflow version is 3.0
        if conf.getboolean("core", "enable_xcom_pickling", fallback=False):
            return response.primary_results[0]
        return str(response.primary_results[0])
