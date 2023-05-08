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

import csv
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GoogleSheetsToGCSOperator(BaseOperator):
    """
    Writes Google Sheet data into Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSheetsToGCSOperator`

    :param spreadsheet_id: The Google Sheet ID to interact with.
    :param sheet_filter: Default to None, if provided, Should be an array of the sheet
        titles to pull from.
    :param destination_bucket: The destination Google cloud storage bucket where the
        report should be written to. (templated)
    :param destination_path: The Google cloud storage URI array for the object created by the operator.
        For example: ``path/to/my/files``.
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
        "spreadsheet_id",
        "destination_bucket",
        "destination_path",
        "sheet_filter",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        spreadsheet_id: str,
        destination_bucket: str,
        sheet_filter: list[str] | None = None,
        destination_path: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_filter = sheet_filter
        self.destination_bucket = destination_bucket
        self.destination_path = destination_path
        self.impersonation_chain = impersonation_chain

    def _upload_data(
        self,
        gcs_hook: GCSHook,
        hook: GSheetsHook,
        sheet_range: str,
        sheet_values: list[Any],
    ) -> str:
        # Construct destination file path
        sheet = hook.get_spreadsheet(self.spreadsheet_id)
        file_name = f"{sheet['properties']['title']}_{sheet_range}.csv".replace(" ", "_")
        dest_file_name = (
            f"{self.destination_path.strip('/')}/{file_name}" if self.destination_path else file_name
        )

        with NamedTemporaryFile("w+") as temp_file:
            # Write data
            writer = csv.writer(temp_file)
            writer.writerows(sheet_values)
            temp_file.flush()

            # Upload to GCS
            gcs_hook.upload(
                bucket_name=self.destination_bucket,
                object_name=dest_file_name,
                filename=temp_file.name,
            )
        return dest_file_name

    def execute(self, context: Context):
        sheet_hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        # Pull data and upload
        destination_array: list[str] = []
        sheet_titles = sheet_hook.get_sheet_titles(
            spreadsheet_id=self.spreadsheet_id, sheet_filter=self.sheet_filter
        )
        for sheet_range in sheet_titles:
            data = sheet_hook.get_values(spreadsheet_id=self.spreadsheet_id, range_=sheet_range)
            gcs_path_to_file = self._upload_data(gcs_hook, sheet_hook, sheet_range, data)
            destination_array.append(gcs_path_to_file)

        self.xcom_push(context, "destination_objects", destination_array)
        return destination_array
