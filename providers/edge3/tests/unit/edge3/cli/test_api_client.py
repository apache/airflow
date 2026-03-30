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

from http import HTTPStatus
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from aiohttp import ClientResponseError, ConnectionTimeoutError
from aioresponses import aioresponses
from yarl import URL

from airflow.providers.edge3.cli.api_client import _make_generic_request

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from aioresponses.core import RequestCall

pytestmark = [pytest.mark.asyncio]

MOCK_ENDPOINT = "https://mock-api-test-endpoint"
MOCK_OK_PAYLOAD = {"test": "ok"}


@pytest.fixture
def mock_responses():
    with conf_vars({("edge", "api_url"): MOCK_ENDPOINT}), aioresponses() as m:
        yield m


class TestApiClient:
    async def test_make_generic_request_success(self, mock_responses: aioresponses):
        mock_responses.get(f"{MOCK_ENDPOINT}/mock_service", repeat=True, payload=MOCK_OK_PAYLOAD)
        mock_responses.post(f"{MOCK_ENDPOINT}/service_no_content", status=HTTPStatus.NO_CONTENT, repeat=True)

        result1 = await _make_generic_request("GET", f"{MOCK_ENDPOINT}/mock_service")
        result2 = await _make_generic_request("POST", f"{MOCK_ENDPOINT}/service_no_content", "test")

        assert result1 == MOCK_OK_PAYLOAD
        assert result2 is None
        assert len(mock_responses.requests) == 2

    @patch("asyncio.sleep", return_value=None)
    async def test_make_generic_request_retry(self, mock_sleep, mock_responses: aioresponses):
        flaky_service = f"{MOCK_ENDPOINT}/flaky_service"
        mock_responses.get(flaky_service, status=HTTPStatus.SERVICE_UNAVAILABLE)
        mock_responses.get(flaky_service, status=HTTPStatus.SERVICE_UNAVAILABLE)
        mock_responses.get(flaky_service, status=HTTPStatus.SERVICE_UNAVAILABLE)
        mock_responses.get(flaky_service, exception=ConnectionTimeoutError())
        mock_responses.get(flaky_service, payload=MOCK_OK_PAYLOAD)
        result = await _make_generic_request("GET", flaky_service)

        assert result == MOCK_OK_PAYLOAD
        calls: list[RequestCall] | None = mock_responses.requests.get(("GET", URL(flaky_service)))
        assert calls
        assert len(calls) == 5

    @patch("asyncio.sleep", return_value=None)
    async def test_make_generic_request_unrecoverable_error(self, mock_sleep, mock_responses: aioresponses):
        unreliable_service = f"{MOCK_ENDPOINT}/bad_service"
        mock_responses.post(unreliable_service, status=HTTPStatus.INTERNAL_SERVER_ERROR, repeat=True)

        with pytest.raises(ClientResponseError) as err:
            await _make_generic_request("POST", unreliable_service, "test")

        mock_sleep.assert_called()
        assert err.value.status == HTTPStatus.INTERNAL_SERVER_ERROR
        calls: list[RequestCall] | None = mock_responses.requests.get(("POST", URL(unreliable_service)))
        assert calls
        assert len(calls) == 10
