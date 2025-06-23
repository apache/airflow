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
import json
import pickle
from types import SimpleNamespace
from unittest import mock
from unittest.mock import call, patch

import pytest
import tenacity
from aiohttp import BasicAuth
from requests import Response
from requests.models import RequestEncodingMixin

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.hooks import base
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.triggers.http import HttpTrigger, serialize_auth_type


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

    @pytest.mark.db_test
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

    @pytest.mark.parametrize(
        "data, headers, extra_options, pagination_data, pagination_headers, pagination_extra_options",
        [
            ({"data": 1}, {"x-head": "1"}, {"verify": False}, {"data": 2}, {"x-head": "0"}, {"verify": True}),
            ("data foo", {"x-head": "1"}, {"verify": False}, {"data": 2}, {"x-head": "0"}, {"verify": True}),
            ("data foo", {"x-head": "1"}, {"verify": False}, "data bar", {"x-head": "0"}, {"verify": True}),
            ({"data": 1}, {"x-head": "1"}, {"verify": False}, "data foo", {"x-head": "0"}, {"verify": True}),
        ],
    )
    def test_pagination(
        self,
        requests_mock,
        data,
        headers,
        extra_options,
        pagination_data,
        pagination_headers,
        pagination_extra_options,
    ):
        """
        Test that the HttpOperator calls repetitively the API when a
        pagination_function is provided, and as long as this function returns
        a dictionary that override previous' call parameters.
        """
        is_second_call: bool = False
        extra_options_verify = extra_options["verify"]

        def pagination_function(response: Response) -> dict | None:
            """Paginated function which returns None at the second call."""
            nonlocal is_second_call
            if not is_second_call:
                is_second_call = True
                return dict(
                    endpoint=response.json()["endpoint"],
                    data=pagination_data,
                    headers=pagination_headers,
                    extra_options=pagination_extra_options,
                )
            return None

        first_endpoint = requests_mock.post("http://www.example.com/1", json={"value": 5, "endpoint": "2"})
        second_endpoint = requests_mock.post("http://www.example.com/2", json={"value": 10, "endpoint": "3"})
        operator = HttpOperator(
            task_id="test_HTTP_op",
            method="POST",
            endpoint="/1",
            data=data,
            headers=headers,
            extra_options=extra_options,
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=pagination_function,
            response_filter=lambda resp: [entry.json()["value"] for entry in resp],
        )
        result = operator.execute({})

        # Ensure the initial call is made with parameters passed to the Operator
        first_call = first_endpoint.request_history[0]
        assert first_call.headers.items() >= headers.items()
        assert first_call.body == RequestEncodingMixin._encode_params(data)
        assert first_call.verify == extra_options_verify

        # Ensure the second - paginated - call is made with parameters merged from the pagination function
        second_call = second_endpoint.request_history[0]
        assert second_call.headers.items() >= pagination_headers.items()
        assert second_call.body == RequestEncodingMixin._encode_params(pagination_data)
        assert second_call.verify is pagination_extra_options["verify"]

        assert result == [5, 10]

    @pytest.mark.db_test
    def test_async_pagination(self, requests_mock):
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

    @patch.object(HttpHook, "run_with_advanced_retry")
    def test_retry_args(self, mock_run_with_advanced_retry, requests_mock):
        requests_mock.get("http://www.example.com", exc=Exception("Example Exception"))
        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(5),
            retry=tenacity.retry_if_exception_type(Exception),
        )
        operator = HttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            retry_args=retry_args,
        )
        operator.execute({})
        mock_run_with_advanced_retry.assert_called_with(retry_args, "/", {}, {}, {})
        assert mock_run_with_advanced_retry.call_count == 1

    @patch.object(HttpHook, "run_with_advanced_retry")
    def test_pagination_retry_args(
        self,
        mock_run_with_advanced_retry,
        requests_mock,
    ):
        is_second_call: bool = False

        def pagination_function(response: Response) -> dict | None:
            """Paginated function which returns None at the second call."""
            nonlocal is_second_call
            if not is_second_call:
                is_second_call = True
                return dict(
                    endpoint=response.json()["endpoint"],
                )
            return None

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(5),
            retry=tenacity.retry_if_exception_type(Exception),
        )
        operator = HttpOperator(
            task_id="test_HTTP_op",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=pagination_function,
            retry_args=retry_args,
        )

        response = Response()
        response.status_code = 200
        response._content = json.dumps({"value": 5, "endpoint": "/"}).encode("utf-8")
        response.headers["Content-Type"] = "application/json"

        mock_run_with_advanced_retry.return_value = response
        operator.execute({})
        mock_run_with_advanced_retry.assert_has_calls(
            [
                call(retry_args, "/", {}, {}, {}),
                call(retry_args, endpoint="/", data={}, headers={}, extra_options={}),
            ]
        )

        assert mock_run_with_advanced_retry.call_count == 2

    def _capture_defer(self, monkeypatch):
        captured = {}

        def _fake_defer(self, *, trigger, method_name, **kwargs):
            captured["trigger"] = trigger
            captured["kwargs"] = kwargs

        monkeypatch.setattr(HttpOperator, "defer", _fake_defer)
        return captured

    @pytest.mark.parametrize(
        "login, password, auth_type, expect_cls",
        [
            ("user", "password", None, BasicAuth),
            (None, None, None, type(None)),
            ("user", "password", BasicAuth, BasicAuth),
        ],
    )
    def test_auth_type_is_serialised_as_string(self, monkeypatch, login, password, auth_type, expect_cls):
        monkeypatch.setattr(
            base.BaseHook, "get_connection", lambda _cid: SimpleNamespace(login=login, password=password)
        )
        captured = self._capture_defer(monkeypatch)

        HttpOperator(task_id="test_HTTP_op", deferrable=True, auth_type=auth_type).execute(context={})

        trigger = captured["trigger"]
        kwargs = captured["trigger"].serialize()[1]

        expected_str = serialize_auth_type(expect_cls) if expect_cls is not type(None) else None
        assert kwargs["auth_type"] == expected_str

        assert trigger.auth_type == expect_cls or (trigger.auth_type is None and expect_cls is type(None))

    def test_resolve_auth_type_variants(self, monkeypatch):
        monkeypatch.setattr(
            base.BaseHook, "get_connection", lambda _cid: SimpleNamespace(login="user", password="password")
        )
        assert HttpOperator(task_id="test_HTTP_op_1")._resolve_auth_type() is BasicAuth

        class DummyAuth:
            def __init__(self, *_, **__): ...

        assert HttpOperator(task_id="test_HTTP_op_2", auth_type=DummyAuth)._resolve_auth_type() is DummyAuth

        monkeypatch.setattr(
            base.BaseHook, "get_connection", lambda _cid: SimpleNamespace(login=None, password=None)
        )
        assert HttpOperator(task_id="test_HTTP_op_3")._resolve_auth_type() is None
