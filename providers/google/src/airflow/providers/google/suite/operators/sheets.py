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

from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
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
    :param drive_id: Shared Drive ID where the spreadsheet should be created.
        This is useful when using a service account, since service accounts
        do not have personal Drive storage. (templated)
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
        "drive_id",
    )

    def __init__(
        self,
        *,
        spreadsheet: dict[str, Any],
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        drive_id: str | None = None,
        api_endpoint: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet = spreadsheet
        self.impersonation_chain = impersonation_chain
        self.api_endpoint = api_endpoint
        self.drive_id = drive_id

    def execute(self, context: Any) -> dict[str, Any]:
        if self.drive_id:
            spreadsheet = self._create_spreadsheet_via_drive_api()
        else:
            spreadsheet = self._create_spreadsheet_via_sheets_api()
        context["task_instance"].xcom_push(key="spreadsheet_id", value=spreadsheet["spreadsheetId"])
        context["task_instance"].xcom_push(key="spreadsheet_url", value=spreadsheet["spreadsheetUrl"])
        return spreadsheet

    def _construct_spreadsheet_metadata(self, spreadsheet) -> dict:
        return {
            "name": spreadsheet["properties"]["title"],
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [self.drive_id],
        }

    def _create_spreadsheet_via_drive_api(self) -> dict:
        spreadsheet_metadata = self._construct_spreadsheet_metadata(self.spreadsheet)
        hook = GoogleDriveHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        spreadsheet = hook.create_file(file_metadata=spreadsheet_metadata)

        response = {
            "spreadsheetId": spreadsheet.get("id"),
            "spreadsheetUrl": spreadsheet.get("webViewLink"),
        }
        return response

    def _create_spreadsheet_via_sheets_api(self) -> dict:
        hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            api_endpoint=self.api_endpoint,
        )
        return hook.create_spreadsheet(spreadsheet=self.spreadsheet)
