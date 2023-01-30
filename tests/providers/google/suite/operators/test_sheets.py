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


class TestGoogleSheetsCreateSpreadsheet:
    @mock.patch("airflow.providers.google.suite.operators.sheets.GSheetsHook")
    @mock.patch(
        "airflow.providers.google.suite.operators.sheets.GoogleSheetsCreateSpreadsheetOperator.xcom_push"
    )
    def test_execute(self, mock_xcom, mock_hook):
        context = {}
        spreadsheet = mock.MagicMock()
        mock_hook.return_value.create_spreadsheet.return_value = {
            "spreadsheetId": SPREADSHEET_ID,
            "spreadsheetUrl": SPREADSHEET_URL,
        }
        op = GoogleSheetsCreateSpreadsheetOperator(
            task_id="test_task", spreadsheet=spreadsheet, gcp_conn_id=GCP_CONN_ID
        )
        op_execute_result = op.execute(context)

        mock_hook.return_value.create_spreadsheet.assert_called_once_with(spreadsheet=spreadsheet)

        assert op_execute_result["spreadsheetId"] == "1234567890"
        assert op_execute_result["spreadsheetUrl"] == "https://example/sheets"
