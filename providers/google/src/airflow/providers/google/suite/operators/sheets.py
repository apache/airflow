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
from typing import Any

from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.google.version_compat import BaseOperator


class GoogleSheetsCreateSpreadsheetOperator(BaseOperator):
    """
    Creates a new spreadsheet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSheetsCreateSpreadsheetOperator`

    :param spreadsheet: an instance of Spreadsheet
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param api_endpoint: Optional. Custom API endpoint, e.g: private.googleapis.com.
        This can be used to target private VPC or restricted access endpoints.
    """

    template_fields: Sequence[str] = (
        "spreadsheet",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        spreadsheet: dict[str, Any],
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        api_endpoint: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet = spreadsheet
        self.impersonation_chain = impersonation_chain
        self.api_endpoint = api_endpoint

    def execute(self, context: Any) -> dict[str, Any]:
        hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            api_endpoint=self.api_endpoint,
        )
        spreadsheet = hook.create_spreadsheet(spreadsheet=self.spreadsheet)
        context["task_instance"].xcom_push(key="spreadsheet_id", value=spreadsheet["spreadsheetId"])
        context["task_instance"].xcom_push(key="spreadsheet_url", value=spreadsheet["spreadsheetUrl"])
        return spreadsheet
