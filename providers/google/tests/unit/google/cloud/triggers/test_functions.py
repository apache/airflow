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

import asyncio
from unittest import mock

import pytest

from airflow.providers.google.cloud.triggers.functions import CloudFunctionInvokeTrigger
from airflow.triggers.base import TriggerEvent

FUNCTION_URI = "https://example.com/function"
JSON_PAYLOAD = {"key": "value"}
HEADERS = {"Authorization": "Bearer token"}


class TestCloudFunctionInvokeTrigger:
    def test_serialization(self):
        trigger = CloudFunctionInvokeTrigger(
            function_uri=FUNCTION_URI,
            json_payload=JSON_PAYLOAD,
            headers=HEADERS,
            timeout=30.0,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.functions.CloudFunctionInvokeTrigger"
        assert kwargs == {
            "function_uri": FUNCTION_URI,
            "json_payload": JSON_PAYLOAD,
            "headers": HEADERS,
            "timeout": 30.0,
        }

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.post")
    async def test_run_success(self, mock_post):
        mock_response = mock.AsyncMock()
        mock_response.status = 200
        mock_response.json = mock.AsyncMock(return_value={"result": "success"})
        mock_post.return_value.__aenter__.return_value = mock_response

        trigger = CloudFunctionInvokeTrigger(
            function_uri=FUNCTION_URI,
            json_payload=JSON_PAYLOAD,
            headers=HEADERS,
        )

        generator = trigger.run()
        event = await generator.asend(None)

        assert isinstance(event, TriggerEvent)
        assert event.payload["status"] == "success"
        assert event.payload["response"] == {"result": "success"}

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.post")
    async def test_run_error(self, mock_post):
        mock_response = mock.AsyncMock()
        mock_response.status = 400
        mock_response.text = mock.AsyncMock(return_value="Bad Request")
        mock_post.return_value.__aenter__.return_value = mock_response

        trigger = CloudFunctionInvokeTrigger(
            function_uri=FUNCTION_URI,
            json_payload=JSON_PAYLOAD,
            headers=HEADERS,
        )

        generator = trigger.run()
        event = await generator.asend(None)

        assert isinstance(event, TriggerEvent)
        assert event.payload["status"] == "error"
        assert event.payload["status_code"] == 400

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.post")
    async def test_run_timeout(self, mock_post):
        mock_post.side_effect = asyncio.TimeoutError()

        trigger = CloudFunctionInvokeTrigger(
            function_uri=FUNCTION_URI,
            json_payload=JSON_PAYLOAD,
            headers=HEADERS,
            timeout=10.0,
        )

        generator = trigger.run()
        event = await generator.asend(None)

        assert isinstance(event, TriggerEvent)
        assert event.payload["status"] == "error"
        assert "timed out" in event.payload["message"]
