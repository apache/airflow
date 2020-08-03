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

from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class GoogleSheetsCreateSpreadsheetOperator(BaseOperator):
    """
    Creates a new spreadsheet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSheetsCreateSpreadsheetOperator`

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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet = spreadsheet
        self.delegate_to = delegate_to

    def execute(self, context: Any):
        hook = GSheetsHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        spreadsheet = hook.create_spreadsheet(spreadsheet=self.spreadsheet)
        self.xcom_push(context, "spreadsheet_id", spreadsheet["spreadsheetId"])
        self.xcom_push(context, "spreadsheet_url", spreadsheet["spreadsheetUrl"])
        return spreadsheet
