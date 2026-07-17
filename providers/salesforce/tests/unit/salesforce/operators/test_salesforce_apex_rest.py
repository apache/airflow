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

from airflow.providers.salesforce.operators.salesforce_apex_rest import SalesforceApexRestOperator


class TestSalesforceApexRestOperator:
    """
    Test class for SalesforceApexRestOperator
    """

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_salesforce_apex_rest(self, mock_get_conn):
        """
        Test execute apex rest
        """

        endpoint = "User/Activity"
        method = "POST"
        payload = {"activity": [{"user": "12345", "action": "update page", "time": "2014-04-21T13:00:15Z"}]}

        mock_get_conn.return_value.apexecute = Mock()

        operator = SalesforceApexRestOperator(
            task_id="task", endpoint=endpoint, method=method, payload=payload
        )

        operator.execute(context={})

        mock_get_conn.return_value.apexecute.assert_called_once_with(
            action=endpoint, method=method, data=payload
        )
