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

from unittest import mock

from airflow.providers.google.suite.transfers.gcs_to_sheets import GCSToGoogleSheetsOperator

GCP_CONN_ID = "test"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
SPREADSHEET_ID = "1234567890"
VALUES = [[1, 2, 3]]
BUCKET = "destination_bucket"
PATH = "path/to/reports"
DELEGATE_TO = "test_account@xxx.zzz"


class TestGCSToGoogleSheets:
    @mock.patch("airflow.providers.google.suite.transfers.gcs_to_sheets.GCSHook")
    @mock.patch("airflow.providers.google.suite.transfers.gcs_to_sheets.GSheetsHook")
    @mock.patch("airflow.providers.google.suite.transfers.gcs_to_sheets.NamedTemporaryFile")
    @mock.patch("airflow.providers.google.suite.transfers.gcs_to_sheets.csv.reader")
    def test_execute(self, mock_reader, mock_tempfile, mock_sheet_hook, mock_gcs_hook):
        filename = "file://97g23r"
        file_handle = mock.MagicMock()
        mock_tempfile.return_value.__enter__.return_value = file_handle
        mock_tempfile.return_value.__enter__.return_value.name = filename
        mock_reader.return_value = VALUES

        op = GCSToGoogleSheetsOperator(
            task_id="test_task",
            spreadsheet_id=SPREADSHEET_ID,
            bucket_name=BUCKET,
            object_name=PATH,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            delegate_to=DELEGATE_TO,
        )
        op.execute(None)

        mock_sheet_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_gcs_hook.return_value.download.assert_called_once_with(
            bucket_name=BUCKET, object_name=PATH, filename=filename
        )

        mock_reader.assert_called_once_with(file_handle)

        mock_sheet_hook.return_value.update_values.assert_called_once_with(
            spreadsheet_id=SPREADSHEET_ID,
            range_="Sheet1",
            values=VALUES,
        )
