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
import contextlib
import pickle
from unittest import mock

import pytest
from requests import Response

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.triggers.http import HttpTrigger


@mock.patch.dict("os.environ", AIRFLOW_CONN_HTTP_EXAMPLE="http://www.example.com")
class TestHttpOperator:
    def test_response_in_logs(self, requests_mock):
        """
        Test that when using HttpOperator with 'GET',
        the log contains 'Example Domain' in it
        """

        requests_mock.get("http://www.example.com", text="Example.com fake response")
        operator = HttpOperator(
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
        Test that when using HttpOperator with log_response=True,
        the response is logged even if request_check fails
        """

        def response_check(response):
            return response.text != "invalid response"

        requests_mock.get("http://www.example.com", text="invalid response")
        operator = HttpOperator(
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
        operator = HttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            response_filter=lambda response: response.json(),
        )
        result = operator.execute({})
        assert result == {"value": 5}

    def test_async_defer_successfully(self, requests_mock):
        operator = HttpOperator(
            task_id="test_HTTP_op",
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute({})
        assert isinstance(exc.value.trigger, HttpTrigger), "Trigger is not a HttpTrigger"

    def test_async_execute_successfully(self, requests_mock):
        operator = HttpOperator(
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

    def test_paginated_responses(self, requests_mock):
        """
        Test that the HttpOperator calls repetitively the API when a
        pagination_function is provided, and as long as this function returns
        a dictionary that override previous' call parameters.
        """
        iterations: int = 0

        def pagination_function(response: Response) -> dict | None:
            """Paginated function which returns None at the second call."""
            nonlocal iterations
            if iterations < 2:
                iterations += 1
                return dict(
                    endpoint=response.json()["endpoint"],
                    data={},
                    headers={},
                    extra_options={},
                )
            return None

        requests_mock.get("http://www.example.com/foo", json={"value": 5, "endpoint": "bar"})
        requests_mock.get("http://www.example.com/bar", json={"value": 10, "endpoint": "foo"})
        operator = HttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/foo",
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=pagination_function,
            response_filter=lambda resp: [entry.json()["value"] for entry in resp],
        )
        result = operator.execute({})
        assert result == [5, 10, 5]

    def test_async_paginated_responses(self, requests_mock):
        """
        Test that the HttpOperator calls asynchronously and repetitively
        the API when a pagination_function is provided, and as long as this function
        returns a dictionary that override previous' call parameters.
        """

        def make_response_object() -> Response:
            response = Response()
            response._content = b'{"value": 5}'
            return response

        def create_resume_response_parameters() -> dict:
            response = make_response_object()
            return dict(
                context={},
                event={
                    "status": "success",
                    "response": base64.standard_b64encode(pickle.dumps(response)).decode("ascii"),
                },
            )

        has_returned: bool = False

        def pagination_function(response: Response) -> dict | None:
            """Paginated function which returns None at the second call."""
            nonlocal has_returned
            if not has_returned:
                has_returned = True
                return dict(endpoint="/")
            return None

        operator = HttpOperator(
            task_id="test_HTTP_op",
            pagination_function=pagination_function,
            deferrable=True,
        )

        # Do two calls: On the first one, the pagination_function creates a new
        # deferrable trigger. On the second one, the pagination_function returns
        # None, which ends the execution of the Operator
        with contextlib.suppress(TaskDeferred):
            operator.execute_complete(**create_resume_response_parameters())
            result = operator.execute_complete(
                **create_resume_response_parameters(), paginated_responses=[make_response_object()]
            )
            assert result == ['{"value": 5}', '{"value": 5}']
