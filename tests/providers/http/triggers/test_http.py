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

from airflow.providers.http.triggers.http import HttpTrigger
from airflow.triggers.base import TriggerEvent

HTTP_PATH = "airflow.providers.http.triggers.http.{}"
TEST_CONN_ID = "http_default"
TEST_AUTH_TYPE = None
TEST_METHOD = "POST"
TEST_ENDPOINT = "endpoint"
TEST_HEADERS = {"Authorization": "Bearer test"}
TEST_DATA = ""
TEST_EXTRA_OPTIONS: dict[str, Any] = {}


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
