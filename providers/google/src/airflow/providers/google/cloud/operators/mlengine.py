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
"""This module contains Google Cloud MLEngine operators."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.mlengine import MLEngineHook
from airflow.providers.google.cloud.links.mlengine import MLEngineModelLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.utils.context import Context


log = logging.getLogger(__name__)


@deprecated(
    planned_removal_date="November 01, 2025",
    use_instead="appropriate VertexAI operator",
    reason="All the functionality of legacy MLEngine and new features are available on the Vertex AI.",
    category=AirflowProviderDeprecationWarning,
)
class MLEngineCreateModelOperator(GoogleCloudBaseOperator):
    """
    Creates a new model.

    .. warning::
        This operator is deprecated. Please use appropriate VertexAI operator from
        :class:`airflow.providers.google.cloud.operators.vertex_ai` instead.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineCreateModelOperator`

    The model should be provided by the `model` parameter.

    :param model: A dictionary containing the information about the model.
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "project_id",
        "model",
        "impersonation_chain",
    )
    operator_extra_links = (MLEngineModelLink(),)

    def __init__(
        self,
        *,
        model: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model = model
        self._gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        project_id = self.project_id or hook.project_id
        if project_id:
            MLEngineModelLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                model_id=self.model["name"],
            )

        return hook.create_model(project_id=self.project_id, model=self.model)
