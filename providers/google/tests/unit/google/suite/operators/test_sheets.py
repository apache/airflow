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

from airflow.providers.google.suite.operators.sheets import GoogleSheetsCreateSpreadsheetOperator

GCP_CONN_ID = "test"
SPREADSHEET_URL = "https://example/sheets"
SPREADSHEET_ID = "1234567890"
DRIVE_ID = "shared_drive_123"
SPREADSHEET_DATA = {"properties": {"title": "My Test Spreadsheet"}}


class TestGoogleSheetsCreateSpreadsheet:
    @mock.patch("airflow.providers.google.suite.operators.sheets.GSheetsHook")
    def test_execute_via_sheets_api(self, mock_sheets_hook):
        """Test spreadsheet creation using the standard Sheets API, no drive_id provided."""
        mock_task_instance = mock.MagicMock()
        context = {"task_instance": mock_task_instance}

        mock_sheets_hook.return_value.create_spreadsheet.return_value = {
            "spreadsheetId": SPREADSHEET_ID,
            "spreadsheetUrl": SPREADSHEET_URL,
        }

        op = GoogleSheetsCreateSpreadsheetOperator(
            task_id="test_task", spreadsheet=SPREADSHEET_DATA, gcp_conn_id=GCP_CONN_ID
        )
        op_execute_result = op.execute(context)

        mock_sheets_hook.return_value.create_spreadsheet.assert_called_once_with(spreadsheet=SPREADSHEET_DATA)

        # Verify xcom_push was called with correct arguments
        assert mock_task_instance.xcom_push.call_count == 2
        mock_task_instance.xcom_push.assert_any_call(key="spreadsheet_id", value=SPREADSHEET_ID)
        mock_task_instance.xcom_push.assert_any_call(key="spreadsheet_url", value=SPREADSHEET_URL)

        assert op_execute_result["spreadsheetId"] == SPREADSHEET_ID
        assert op_execute_result["spreadsheetUrl"] == SPREADSHEET_URL

    @mock.patch("airflow.providers.google.suite.operators.sheets.GoogleDriveHook")
    def test_execute_via_drive_api(self, mock_drive_hook):
        """Test spreadsheet creation using the Drive API with drive_id provided."""
        mock_task_instance = mock.MagicMock()
        context = {"task_instance": mock_task_instance}

        mock_drive_hook.return_value.create_file.return_value = {
            "id": SPREADSHEET_ID,
            "webViewLink": SPREADSHEET_URL,
        }

        op = GoogleSheetsCreateSpreadsheetOperator(
            task_id="test_task", spreadsheet=SPREADSHEET_DATA, gcp_conn_id=GCP_CONN_ID, drive_id=DRIVE_ID
        )
        op_execute_result = op.execute(context)

        expected_metadata = {
            "name": "My Test Spreadsheet",
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [DRIVE_ID],
        }

        mock_drive_hook.return_value.create_file.assert_called_once_with(file_metadata=expected_metadata)

        assert mock_task_instance.xcom_push.call_count == 2
        mock_task_instance.xcom_push.assert_any_call(key="spreadsheet_id", value=SPREADSHEET_ID)
        mock_task_instance.xcom_push.assert_any_call(key="spreadsheet_url", value=SPREADSHEET_URL)

        assert op_execute_result["spreadsheetId"] == SPREADSHEET_ID
        assert op_execute_result["spreadsheetUrl"] == SPREADSHEET_URL
