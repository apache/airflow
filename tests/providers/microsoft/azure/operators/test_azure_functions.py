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

import json
from unittest import mock

from requests import Response

from airflow.providers.microsoft.azure.operators.azure_functions import AzureFunctionsInvokeOperator


class TestAzureFunctionOperator:
    def test_init(self):
        """
        Test init by creating AzureFunctionsInvokeOperator with task id, function_name and asserting
        with values
        """
        azure_function_invoke_operator = AzureFunctionsInvokeOperator(
            task_id="test_azure_function", function_name="test_function_name"
        )
        assert azure_function_invoke_operator.task_id == "test_azure_function"
        assert azure_function_invoke_operator.function_name == "test_function_name"

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_functions.AzureFunctionsHook.get_conn")
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_functions.AzureFunctionsHook.invoke_function")
    def test_invoke_azure_function(self, mock_invoke_func, mock_conn):
        test_event_input = {"TestInput": "Testdata"}
        Response.text = "Test"
        mock_invoke_func.return_value = Response
        invoke_azure_function = AzureFunctionsInvokeOperator(
            task_id="task_test",
            function_name="test",
            payload=json.dumps(test_event_input),
        )
        value = invoke_azure_function.execute(None)
        assert value == "Test"
