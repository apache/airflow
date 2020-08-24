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

import mock

from airflow.providers.google.cloud.transfers.sheets_to_gcs import GoogleSheetsToGCSOperator

RANGE = "test!A:E"
FILTER = ["sheet_filter"]
SHEET_TITLE = "title"
RANGES = ["test1", "test2"]
SPREADSHEET_ID = "1234567890"
VALUES = [[1, 2, 3]]
BUCKET = "destination_bucket"
PATH = "path/to/reports"
GCP_CONN_ID = "test"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestGoogleSheetsToGCSOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.sheets_to_gcs.csv.writer")
    @mock.patch("airflow.providers.google.cloud.transfers.sheets_to_gcs.NamedTemporaryFile")
    def test_upload_data(self, mock_tempfile, mock_writer):
        filename = "file://97g23r"
        file_handle = mock.MagicMock()
        mock_tempfile.return_value.__enter__.return_value = file_handle
        mock_tempfile.return_value.__enter__.return_value.name = filename

        mock_sheet_hook = mock.MagicMock()
        mock_sheet_hook.get_spreadsheet.return_value = {
            "properties": {"title": SHEET_TITLE}
        }
        expected_dest_file = f"{PATH}/{SHEET_TITLE}_{RANGE}.csv"

        mock_gcs_hook = mock.MagicMock()

        op = GoogleSheetsToGCSOperator(
            task_id="test_task",
            spreadsheet_id=SPREADSHEET_ID,
            destination_bucket=BUCKET,
            sheet_filter=FILTER,
            destination_path=PATH,
        )

        result = op._upload_data(
            gcs_hook=mock_gcs_hook,
            hook=mock_sheet_hook,
            sheet_range=RANGE,
            sheet_values=VALUES,
        )

        # Test writing to file
        mock_sheet_hook.get_spreadsheet.assert_called_once_with(SPREADSHEET_ID)
        mock_writer.assert_called_once_with(file_handle)
        mock_writer.return_value.writerows.assert_called_once_with(VALUES)
        file_handle.flush.assert_called_once_with()

        # Test upload
        mock_gcs_hook.upload.assert_called_once_with(
            bucket_name=BUCKET, object_name=expected_dest_file, filename=filename
        )

        # Assert path to file is returned
        assert result == expected_dest_file

    @mock.patch("airflow.providers.google.cloud.transfers.sheets_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sheets_to_gcs.GSheetsHook")
    @mock.patch(
        "airflow.providers.google.cloud.transfers.sheets_to_gcs.GoogleSheetsToGCSOperator.xcom_push"
    )
    @mock.patch(
        "airflow.providers.google.cloud.transfers.sheets_to_gcs.GoogleSheetsToGCSOperator._upload_data"
    )
    def test_execute(self, mock_upload_data, mock_xcom, mock_sheet_hook, mock_gcs_hook):
        context = {}
        data = ["data1", "data2"]
        mock_sheet_hook.return_value.get_sheet_titles.return_value = RANGES
        mock_upload_data.side_effect = [PATH, PATH]

        op = GoogleSheetsToGCSOperator(
            task_id="test_task",
            spreadsheet_id=SPREADSHEET_ID,
            destination_bucket=BUCKET,
            sheet_filter=FILTER,
            destination_path=PATH,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context)

        mock_sheet_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_sheet_hook.return_value.get_sheet_titles.assert_called_once_with(
            spreadsheet_id=SPREADSHEET_ID, sheet_filter=FILTER
        )

        calls = [mock.call(spreadsheet_id=SPREADSHEET_ID, range_=r) for r in RANGES]
        mock_sheet_hook.return_value.get_values.has_calls(calls)

        calls = [
            mock.call(mock_gcs_hook, mock_sheet_hook, r, v)
            for r, v in zip(RANGES, data)
        ]
        mock_upload_data.has_calls(calls)

        mock_xcom.assert_called_once_with(context, "destination_objects", [PATH, PATH])
