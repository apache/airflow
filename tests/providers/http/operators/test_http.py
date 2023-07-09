#
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

import base64
import pickle
from unittest import mock

import pytest
from requests import Response

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.triggers.http import HttpTrigger


@mock.patch.dict("os.environ", AIRFLOW_CONN_HTTP_EXAMPLE="http://www.example.com")
class TestSimpleHttpOp:
    def test_response_in_logs(self, requests_mock):
        """
        Test that when using SimpleHttpOperator with 'GET',
        the log contains 'Example Domain' in it
        """

        requests_mock.get("http://www.example.com", text="Example.com fake response")
        operator = SimpleHttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            log_response=True,
        )

        result = operator.execute("Example.com fake response")
        assert result == "Example.com fake response"

    def test_response_in_logs_after_failed_check(self, requests_mock):
        """
        Test that when using SimpleHttpOperator with log_response=True,
        the response is logged even if request_check fails
        """

        def response_check(response):
            return response.text != "invalid response"

        requests_mock.get("http://www.example.com", text="invalid response")
        operator = SimpleHttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            log_response=True,
            response_check=response_check,
        )

        with mock.patch.object(operator.log, "info") as mock_info:
            with pytest.raises(AirflowException):
                operator.execute({})
            calls = [mock.call("Calling HTTP method"), mock.call("invalid response")]
            mock_info.assert_has_calls(calls, any_order=True)

    def test_filters_response(self, requests_mock):
        requests_mock.get("http://www.example.com", json={"value": 5})
        operator = SimpleHttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            response_filter=lambda response: response.json(),
        )
        result = operator.execute({})
        assert result == {"value": 5}

    def test_async_defer_successfully(self, requests_mock):
        operator = SimpleHttpOperator(
            task_id="test_HTTP_op",
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute({})
        assert isinstance(exc.value.trigger, HttpTrigger), "Trigger is not a HttpTrigger"

    def test_async_execute_successfully(self, requests_mock):
        operator = SimpleHttpOperator(
            task_id="test_HTTP_op",
            deferrable=True,
        )
        response = Response()
        response._content = b"content"
        result = operator.execute_complete(
            context={},
            event={
                "status": "success",
                "response": base64.standard_b64encode(pickle.dumps(response)).decode("ascii"),
            },
        )
        assert result == "content"
