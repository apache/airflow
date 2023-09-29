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

from requests import Response

from airflow.exceptions import TaskDeferred
from airflow.providers.http.operators.http import PaginatedHttpOperator


@mock.patch.dict("os.environ", AIRFLOW_CONN_HTTP_EXAMPLE="http://www.example.com")
class TestPaginatedHttpOperator:
    def test_paginated_responses(self, requests_mock):
        """
        Test that the SimpleHttpOperator calls repetitively the API when a
        pagination_function is provided, and as long as this function returns
        a dictionary that override previous' call parameters.
        """
        has_returned: bool = False

        def pagination_function(response: Response) -> dict | None:
            """Paginated function which returns None at the second call."""
            nonlocal has_returned
            if not has_returned:
                has_returned = True
                return dict(
                    endpoint="/",
                    data={"cursor": "example"},
                    headers={},
                    extra_options={},
                )

        requests_mock.get("http://www.example.com", json={"value": 5})
        operator = PaginatedHttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=pagination_function,
        )
        result = operator.execute({})
        assert result == ['{"value": 5}', '{"value": 5}']

    def test_async_paginated_responses(self, requests_mock):
        """
        Test that the SimpleHttpOperator calls asynchronously and repetitively
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

        operator = PaginatedHttpOperator(
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
