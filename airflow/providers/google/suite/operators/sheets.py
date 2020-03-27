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

import csv
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class GoogleSheetsCreateSpreadsheet(BaseOperator):
    """
    Creates a new spreadsheet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSheetsCreateSpreadsheet`

    :param spreadsheet: an instance of Spreadsheet
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet
    :type spreadsheet: Dict[str, Any]
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
    :type delegate_to: str
    """

    template_fields = ["spreadsheet"]

    def __init__(
        self,
        spreadsheet: Dict[str, Any],
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet = spreadsheet
        self.delegate_to = delegate_to

    def execute(self, context: Any):
        hook = GSheetsHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        spreadsheet = hook.create_spreadsheet(spreadsheet=self.spreadsheet)
        self.xcom_push(context, "spreadsheet_id", spreadsheet["spreadsheetId"])
        self.xcom_push(context, "spreadsheet_url", spreadsheet["spreadsheetUrl"])
        return spreadsheet


class GoogleSheetsToGCSOperator(BaseOperator):
    """
    Writes Google Sheet data into Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSheetsToGCSOperator`

    :param spreadsheet_id: The Google Sheet ID to interact with.
    :type spreadsheet_id: str
    :param sheet_filter: Default to None, if provided, Should be an array of the sheet
        titles to pull from.
    :type sheet_filter: List[str]
    :param destination_bucket: The destination Google cloud storage bucket where the
        report should be written to. (templated)
    :param destination_bucket: str
    :param destination_path: The Google cloud storage URI array for the object created by the operator.
        For example: ``path/to/my/files``.
    :type destination_path: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
    :type delegate_to: str
    """

    template_fields = ["spreadsheet_id", "destination_bucket", "destination_path", "sheet_filter"]

    def __init__(
        self,
        spreadsheet_id: str,
        destination_bucket: str,
        sheet_filter: Optional[List[str]] = None,
        destination_path: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_filter = sheet_filter
        self.destination_bucket = destination_bucket
        self.destination_path = destination_path
        self.delegate_to = delegate_to

    def _upload_data(
        self,
        gcs_hook: GCSHook,
        hook: GSheetsHook,
        sheet_range: str,
        sheet_values: List[Any],
    ) -> str:
        # Construct destination file path
        sheet = hook.get_spreadsheet(self.spreadsheet_id)
        file_name = f"{sheet['properties']['title']}_{sheet_range}.csv".replace(
            " ", "_"
        )
        dest_file_name = (
            f"{self.destination_path.strip('/')}/{file_name}"
            if self.destination_path
            else file_name
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

    def execute(self, context):
        sheet_hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)

        # Pull data and upload
        destination_array: List[str] = []
        sheet_titles = sheet_hook.get_sheet_titles(
            spreadsheet_id=self.spreadsheet_id, sheet_filter=self.sheet_filter
        )
        for sheet_range in sheet_titles:
            data = sheet_hook.get_values(
                spreadsheet_id=self.spreadsheet_id, range_=sheet_range
            )
            gcs_path_to_file = self._upload_data(
                gcs_hook, sheet_hook, sheet_range, data
            )
            destination_array.append(gcs_path_to_file)

        self.xcom_push(context, "destination_objects", destination_array)
        return destination_array


class GCStoGoogleSheets(BaseOperator):
    """
    Uploads .csv file from Google Cloud Storage to provided Google Spreadsheet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCStoGoogleSheets`

    :param spreadsheet_id: The Google Sheet ID to interact with.
    :type spreadsheet_id: str
    :param bucket_name: Name of GCS bucket.:
    :type bucket_name: str
    :param object_name: Path to the .csv file on the GCS bucket.
    :type object_name: str
    :param spreadsheet_range: The A1 notation of the values to retrieve.
    :type spreadsheet_range: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
    :type delegate_to: str
    """

    template_fields = [
        "spreadsheet_id",
        "bucket_name",
        "object_name",
        "spreadsheet_range",
    ]

    def __init__(
        self,
        spreadsheet_id: str,
        bucket_name: str,
        object_name: Optional[str] = None,
        spreadsheet_range: str = "Sheet1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.delegate_to = delegate_to

    def execute(self, context: Any):
        sheet_hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        with NamedTemporaryFile("w+") as temp_file:
            # Download data
            gcs_hook.download(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=temp_file.name,
            )

            # Upload data
            values = list(csv.reader(temp_file))
            sheet_hook.update_values(
                spreadsheet_id=self.spreadsheet_id,
                range_=self.spreadsheet_range,
                values=values,
            )
