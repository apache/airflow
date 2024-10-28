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
"""Operators that interact with Google Cloud Life Sciences service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.life_sciences import LifeSciencesHook
from airflow.providers.google.cloud.links.life_sciences import LifeSciencesLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.utils.context import Context


@deprecated(
    planned_removal_date="July 08, 2025",
    use_instead="Google Cloud Batch Operators",
    reason="The Life Sciences API (beta) will be discontinued "
    "on July 8, 2025 in favor of Google Cloud Batch.",
    category=AirflowProviderDeprecationWarning,
)
class LifeSciencesRunPipelineOperator(GoogleCloudBaseOperator):
    """
    Runs a Life Sciences Pipeline.

    .. warning::
        This operator is deprecated. Consider using Google Cloud Batch Operators instead.
        The Life Sciences API (beta) will be discontinued on July 8, 2025 in favor
        of Google Cloud Batch.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LifeSciencesRunPipelineOperator`

    :param body: The request body
    :param location: The location of the project
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param api_version: API version used (for example v2beta).
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "body",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    operator_extra_links = (LifeSciencesLink(),)

    def __init__(
        self,
        *,
        body: dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v2beta",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.body = body
        self.location = location
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain

    def _validate_inputs(self) -> None:
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")
        if not self.location:
            raise AirflowException("The required parameter 'location' is missing")

    def execute(self, context: Context) -> dict:
        hook = LifeSciencesHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            LifeSciencesLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )
        return hook.run_pipeline(
            body=self.body, location=self.location, project_id=self.project_id
        )
