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

from unittest.mock import Mock, patch

from airflow.providers.google.suite.transfers.sql_to_sheets import (
    SQLToGoogleSheetsOperator,
)


class TestSQLToGoogleSheets:
    """
    Test class for SQLToGoogleSheetsOperator
    """

    def setup_method(self):
        """
        setup
        """

        self.gcp_conn_id = "test"
        self.sql_conn_id = "test"
        self.sql = "select 1 as my_col"
        self.spreadsheet_id = "1234567890"
        self.values = [[1, 2, 3]]

    @patch("airflow.providers.google.suite.transfers.sql_to_sheets.GSheetsHook")
    def test_execute(self, mock_sheet_hook):
        op = SQLToGoogleSheetsOperator(
            task_id="test_task",
            spreadsheet_id=self.spreadsheet_id,
            gcp_conn_id=self.gcp_conn_id,
            sql_conn_id=self.sql_conn_id,
            sql=self.sql,
        )

        op._get_data = Mock(return_value=self.values)

        op.execute(None)

        mock_sheet_hook.assert_called_once_with(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=None,
            impersonation_chain=None,
        )

        mock_sheet_hook.return_value.update_values.assert_called_once_with(
            spreadsheet_id=self.spreadsheet_id,
            range_="Sheet1",
            values=self.values,
        )
