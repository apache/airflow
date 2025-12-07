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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.firebase.hooks.firestore import CloudFirestoreHook
from airflow.providers.google.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class CloudFirestoreExportDatabaseOperator(BaseOperator):
    """
    Export documents from Google Cloud Firestore to another storage system, such as Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFirestoreExportDatabaseOperator`

    :param database_id: The Database ID.
    :param body: The request body.
        See:
        https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases/exportDocuments
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :param api_version: API version used (for example v1 or v1beta1).
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

    def __init__(
        self,
        *,
        body: dict,
        database_id: str = "(default)",
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database_id = database_id
        self.body = body
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain

    def _validate_inputs(self) -> None:
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")

    def execute(self, context: Context):
        hook = CloudFirestoreHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.export_documents(database_id=self.database_id, body=self.body, project_id=self.project_id)
