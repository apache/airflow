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
from asyncio import Future
from http.cookies import SimpleCookie
from typing import Any
from unittest import mock

import pytest
from aiohttp.client_reqrep import ClientResponse
from multidict import CIMultiDict, CIMultiDictProxy
from requests.structures import CaseInsensitiveDict
from yarl import URL

from airflow.models import Connection
from airflow.providers.http.triggers.http import HttpEventTrigger, HttpSensorTrigger, HttpTrigger
from airflow.triggers.base import TriggerEvent

HTTP_PATH = "airflow.providers.http.triggers.http.{}"
TEST_CONN_ID = "http_default"
TEST_AUTH_TYPE = None
TEST_METHOD = "POST"
TEST_ENDPOINT = "endpoint"
TEST_HEADERS = {"Authorization": "Bearer test"}
TEST_DATA = {"key": "value"}
TEST_EXTRA_OPTIONS: dict[str, Any] = {}
TEST_RESPONSE_CHECK_PATH = "mock.path"
TEST_POLL_INTERVAL = 5


@pytest.fixture
def trigger():
    return HttpTrigger(
        http_conn_id=TEST_CONN_ID,
        auth_type=TEST_AUTH_TYPE,
        method=TEST_METHOD,
        endpoint=TEST_ENDPOINT,
        headers=TEST_HEADERS,
        data=TEST_DATA,
        extra_options=TEST_EXTRA_OPTIONS,
    )


@pytest.fixture
def sensor_trigger():
    return HttpSensorTrigger(
        http_conn_id=TEST_CONN_ID,
        endpoint=TEST_ENDPOINT,
        method=TEST_METHOD,
        headers=TEST_HEADERS,
        data=TEST_DATA,
        extra_options=TEST_EXTRA_OPTIONS,
    )


@pytest.fixture
def event_trigger():
    return HttpEventTrigger(
        http_conn_id=TEST_CONN_ID,
        auth_type=TEST_AUTH_TYPE,
        method=TEST_METHOD,
        endpoint=TEST_ENDPOINT,
        headers=TEST_HEADERS,
        data=TEST_DATA,
        extra_options=TEST_EXTRA_OPTIONS,
        response_check_path=TEST_RESPONSE_CHECK_PATH,
        poll_interval=TEST_POLL_INTERVAL,
    )


@pytest.fixture
def client_response():
    client_response = mock.AsyncMock(ClientResponse)
    client_response.read.return_value = b"content"
    client_response.status = 200
    client_response.headers = CIMultiDictProxy(CIMultiDict([("header", "value")]))
    client_response.url = URL("https://example.com")
    client_response.history = ()
    client_response.get_encoding.return_value = "utf-8"
    client_response.reason = "reason"
    client_response.cookies = SimpleCookie()
    return client_response


class TestHttpTrigger:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="http_default", conn_type="http", host="test:8080/", extra='{"bearer": "test"}'
            )
        )

    @staticmethod
    def _mock_run_result(result_to_mock):
        f = Future()
        f.set_result(result_to_mock)
        return f

    def test_serialization(self, trigger):
        """
        Asserts that the HttpTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.http.triggers.http.HttpTrigger"
        assert kwargs == {
            "http_conn_id": TEST_CONN_ID,
            "auth_type": TEST_AUTH_TYPE,
            "method": TEST_METHOD,
            "endpoint": TEST_ENDPOINT,
            "headers": TEST_HEADERS,
            "data": TEST_DATA,
            "extra_options": TEST_EXTRA_OPTIONS,
        }

    @pytest.mark.asyncio
    @mock.patch(HTTP_PATH.format("HttpAsyncHook"))
    async def test_trigger_on_success_yield_successfully(self, mock_hook, trigger, client_response):
        """
        Tests the HttpTrigger only fires once the job execution reaches a successful state.
        """
        mock_hook.return_value.run.return_value = self._mock_run_result(client_response)
        response = await HttpTrigger._convert_response(client_response)

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent(
            {
                "status": "success",
                "response": base64.standard_b64encode(pickle.dumps(response)).decode("ascii"),
            }
        )

    @pytest.mark.asyncio
    @mock.patch(HTTP_PATH.format("HttpAsyncHook"))
    async def test_trigger_on_exec_yield_successfully(self, mock_hook, trigger):
        """
        Test that HttpTrigger fires the correct event in case of an error.
        """
        mock_hook.return_value.run.side_effect = Exception("Test exception")

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "error", "message": "Test exception"})

    @pytest.mark.asyncio
    async def test_convert_response(self, client_response):
        """
        Assert convert aiohttp.client_reqrep.ClientResponse to requests.Response.
        """
        response = await HttpTrigger._convert_response(client_response)
        assert response.content == await client_response.read()
        assert response.status_code == client_response.status
        assert response.headers == CaseInsensitiveDict(client_response.headers)
        assert response.url == str(client_response.url)
        assert response.history == [HttpTrigger._convert_response(h) for h in client_response.history]
        assert response.encoding == client_response.get_encoding()
        assert response.reason == client_response.reason
        assert dict(response.cookies) == dict(client_response.cookies)

    @pytest.mark.asyncio
    @mock.patch("aiohttp.client.ClientSession.post")
    async def test_trigger_on_post_with_data(self, mock_http_post, trigger):
        """
        Test that HttpTrigger fires the correct event in case of an error.
        """
        generator = trigger.run()
        await generator.asend(None)
        mock_http_post.assert_called_once()
        _, kwargs = mock_http_post.call_args
        assert kwargs["data"] == TEST_DATA
        assert kwargs["json"] is None
        assert kwargs["params"] is None


class TestHttpSensorTrigger:
    def test_serialization(self, sensor_trigger):
        """
        Asserts that the HttpSensorTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = sensor_trigger.serialize()
        assert classpath == "airflow.providers.http.triggers.http.HttpSensorTrigger"
        assert kwargs == {
            "http_conn_id": TEST_CONN_ID,
            "endpoint": TEST_ENDPOINT,
            "method": TEST_METHOD,
            "headers": TEST_HEADERS,
            "data": TEST_DATA,
            "extra_options": TEST_EXTRA_OPTIONS,
            "poke_interval": 5.0,
        }


class TestHttpEventTrigger:
    @staticmethod
    def _mock_run_result(result_to_mock):
        f = Future()
        f.set_result(result_to_mock)
        return f

    def test_serialization(self, event_trigger):
        """
        Asserts that the HttpEventTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = event_trigger.serialize()
        assert classpath == "airflow.providers.http.triggers.http.HttpEventTrigger"
        assert kwargs == {
            "http_conn_id": TEST_CONN_ID,
            "auth_type": TEST_AUTH_TYPE,
            "method": TEST_METHOD,
            "endpoint": TEST_ENDPOINT,
            "headers": TEST_HEADERS,
            "data": TEST_DATA,
            "extra_options": TEST_EXTRA_OPTIONS,
            "response_check_path": TEST_RESPONSE_CHECK_PATH,
            "poll_interval": TEST_POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch(HTTP_PATH.format("HttpAsyncHook"))
    async def test_trigger_on_success_yield_successfully(self, mock_hook, event_trigger, client_response):
        """
        Tests the HttpEventTrigger only fires once the job execution reaches a successful state.
        """
        mock_hook.return_value.run.return_value = self._mock_run_result(client_response)
        event_trigger._run_response_check = mock.AsyncMock(side_effect=[False, True])
        response = await HttpEventTrigger._convert_response(client_response)

        generator = event_trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent(
            {
                "status": "success",
                "response": base64.standard_b64encode(pickle.dumps(response)).decode("ascii"),
            }
        )
        assert mock_hook.return_value.run.call_count == 2
        assert event_trigger._run_response_check.call_count == 2

    @pytest.mark.asyncio
    @mock.patch(HTTP_PATH.format("HttpAsyncHook"))
    async def test_trigger_on_exception_logs_error_and_never_yields(
        self, mock_hook, event_trigger, monkeypatch
    ):
        """
        Tests the HttpEventTrigger logs the appropriate message and does not yield a TriggerEvent when an exception is raised.
        """
        mock_hook.return_value.run.side_effect = Exception("Test exception")
        mock_logger = mock.Mock()
        monkeypatch.setattr(type(event_trigger), "log", mock_logger)

        generator = event_trigger.run()
        with pytest.raises(StopAsyncIteration):
            await generator.asend(None)

        mock_logger.error.assert_called_once_with("status: error, message: %s", "Test exception")

    @pytest.mark.asyncio
    async def test_convert_response(self, client_response):
        """
        Assert convert aiohttp.client_reqrep.ClientResponse to requests.Response.
        """
        response = await HttpEventTrigger._convert_response(client_response)
        assert response.content == await client_response.read()
        assert response.status_code == client_response.status
        assert response.headers == CaseInsensitiveDict(client_response.headers)
        assert response.url == str(client_response.url)
        assert response.history == [HttpEventTrigger._convert_response(h) for h in client_response.history]
        assert response.encoding == client_response.get_encoding()
        assert response.reason == client_response.reason
        assert dict(response.cookies) == dict(client_response.cookies)

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("aiohttp.client.ClientSession.post")
    async def test_trigger_on_post_with_data(self, mock_http_post, event_trigger):
        """
        Test that HttpEventTrigger posts the correct payload when a request is made.
        """
        generator = event_trigger.run()
        with pytest.raises(StopAsyncIteration):
            await generator.asend(None)
        mock_http_post.assert_called_once()
        _, kwargs = mock_http_post.call_args
        assert kwargs["data"] == TEST_DATA
        assert kwargs["json"] is None
        assert kwargs["params"] is None
