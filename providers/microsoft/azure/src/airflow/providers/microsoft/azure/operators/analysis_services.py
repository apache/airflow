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

from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.microsoft.azure.hooks.analysis_services import AzureAnalysisServicesHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureAnalysisServicesRefreshOperator(BaseOperator):
    """
    Trigger a refresh operation on an Azure Analysis Services model.

    This operator triggers a refresh operation and returns the refresh ID.
    Use ``AzureAnalysisServicesRefreshSensor`` to wait for the operation to complete.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureAnalysisServicesRefreshOperator`

    :param server_name: Name of the Analysis Services server.
    :param model_name: Name of the model to refresh.
    :param region: Azure region (e.g., 'westus2').
    :param refresh_type: Type of refresh operation. Defaults to 'full'.
        Options: 'full', 'automatic', 'dataOnly', 'calculate', 'clearValues'.
    :param commit_mode: Commit mode for the refresh. Defaults to 'transactional'.
        Options: 'transactional', 'partialBatch'.
    :param max_parallelism: Maximum number of parallel threads for processing.
    :param retry_count: Number of times to retry the operation on failure.
    :param objects: List of objects to refresh. If None or empty, refreshes all objects.
    :param azure_analysis_services_conn_id: Reference to the
        :ref:`Azure Analysis Services connection <howto/connection:azure_analysis_services>`.
    """

    template_fields = ("server_name", "model_name", "region", "refresh_type")
    template_fields_renderers = {"objects": "json"}

    ui_color = "#50C878"

    def __init__(
        self,
        *,
        server_name: str,
        model_name: str,
        region: str,
        refresh_type: str = "full",
        commit_mode: str = "transactional",
        max_parallelism: int | None = None,
        retry_count: int | None = None,
        objects: list[dict[str, Any]] | None = None,
        azure_analysis_services_conn_id: str = AzureAnalysisServicesHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.server_name = server_name
        self.model_name = model_name
        self.region = region
        self.refresh_type = refresh_type
        self.commit_mode = commit_mode
        self.max_parallelism = max_parallelism
        self.retry_count = retry_count
        self.objects = objects
        self.azure_analysis_services_conn_id = azure_analysis_services_conn_id

    @cached_property
    def hook(self) -> AzureAnalysisServicesHook:
        """Create and return an AzureAnalysisServicesHook (cached)."""
        return AzureAnalysisServicesHook(azure_analysis_services_conn_id=self.azure_analysis_services_conn_id)

    def execute(self, context: Context) -> str:
        """
        Execute the refresh operation.

        :param context: The task execution context.
        :return: The refresh operation ID.
        """
        self.log.info(
            "Starting refresh operation for model '%s' on server '%s' in region '%s'",
            self.model_name,
            self.server_name,
            self.region,
        )

        refresh_id = self.hook.refresh_model(
            server_name=self.server_name,
            model_name=self.model_name,
            region=self.region,
            refresh_type=self.refresh_type,
            commit_mode=self.commit_mode,
            max_parallelism=self.max_parallelism,
            retry_count=self.retry_count,
            objects=self.objects,
        )

        self.log.info("Refresh operation started with ID: %s", refresh_id)
        return refresh_id
