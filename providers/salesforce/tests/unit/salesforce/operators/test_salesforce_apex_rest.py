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

    def test_template_fields(self):
        """Test that template_fields are defined correctly."""
        assert SalesforceApexRestOperator.template_fields == (
            "endpoint",
            "method",
            "payload",
        )

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_salesforce_apex_rest_post(self, mock_get_conn):
        """
        Test execute apex rest with POST method
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

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_salesforce_apex_rest_get(self, mock_get_conn):
        """
        Test execute apex rest with GET method
        """

        endpoint = "User/Activity"
        method = "GET"
        payload = {"user": "12345"}

        mock_get_conn.return_value.apexecute = Mock()

        operator = SalesforceApexRestOperator(
            task_id="task", endpoint=endpoint, method=method, payload=payload
        )

        operator.execute(context={})

        mock_get_conn.return_value.apexecute.assert_called_once_with(
            action=endpoint, method=method, data=payload
        )

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_salesforce_apex_rest_put(self, mock_get_conn):
        """
        Test execute apex rest with PUT method
        """

        endpoint = "User/Activity"
        method = "PUT"
        payload = {"user": "12345", "action": "replace page"}

        mock_get_conn.return_value.apexecute = Mock()

        operator = SalesforceApexRestOperator(
            task_id="task", endpoint=endpoint, method=method, payload=payload
        )

        operator.execute(context={})

        mock_get_conn.return_value.apexecute.assert_called_once_with(
            action=endpoint, method=method, data=payload
        )

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_salesforce_apex_rest_delete(self, mock_get_conn):
        """
        Test execute apex rest with DELETE method
        """

        endpoint = "User/Activity"
        method = "DELETE"
        payload = {"user": "12345"}

        mock_get_conn.return_value.apexecute = Mock()

        operator = SalesforceApexRestOperator(
            task_id="task", endpoint=endpoint, method=method, payload=payload
        )

        operator.execute(context={})

        mock_get_conn.return_value.apexecute.assert_called_once_with(
            action=endpoint, method=method, data=payload
        )

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_salesforce_apex_rest_patch(self, mock_get_conn):
        """
        Test execute apex rest with PATCH method
        """

        endpoint = "User/Activity"
        method = "PATCH"
        payload = {"user": "12345", "action": "partial update"}

        mock_get_conn.return_value.apexecute = Mock()

        operator = SalesforceApexRestOperator(
            task_id="task", endpoint=endpoint, method=method, payload=payload
        )

        operator.execute(context={})

        mock_get_conn.return_value.apexecute.assert_called_once_with(
            action=endpoint, method=method, data=payload
        )

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_default_method_is_get(self, mock_get_conn):
        """
        Test that default method is GET when not specified
        """

        endpoint = "User/Activity"
        payload = {"user": "12345"}

        mock_get_conn.return_value.apexecute = Mock()

        operator = SalesforceApexRestOperator(
            task_id="task", endpoint=endpoint, payload=payload
        )

        operator.execute(context={})

        mock_get_conn.return_value.apexecute.assert_called_once_with(
            action=endpoint, method="GET", data=payload
        )

    @patch("airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn")
    def test_execute_xcom_push_disabled(self, mock_get_conn):
        """
        Test that empty dict is returned when do_xcom_push is False
        """

        endpoint = "User/Activity"
        method = "GET"
        payload = {"user": "12345"}

        mock_get_conn.return_value.apexecute = Mock(return_value={"id": "001"})

        operator = SalesforceApexRestOperator(
            task_id="task", endpoint=endpoint, method=method, payload=payload, do_xcom_push=False
        )

        result = operator.execute(context={})

        assert result == {}
        mock_get_conn.return_value.apexecute.assert_called_once_with(
            action=endpoint, method=method, data=payload
        )
