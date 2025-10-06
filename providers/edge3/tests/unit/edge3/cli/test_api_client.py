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
from unittest.mock import patch

import pytest
from aioresponses import aioresponses
from requests import HTTPError
from requests.exceptions import ConnectTimeout

from airflow.providers.edge3.cli.api_client import _make_generic_request

from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.asyncio]

MOCK_ENDPOINT = "https://invalid-api-test-endpoint"


@pytest.fixture
def aio_response_mock():
    """
    Creates mock async API response.
    """
    with aioresponses() as async_response:
        yield async_response


class TestApiClient:
    @conf_vars({("edge", "api_url"): MOCK_ENDPOINT})
    async def test_make_generic_request_success(self, aio_response_mock: aioresponses):
        aio_response_mock.get(f"{MOCK_ENDPOINT}/dummy_service", repeat=True, json={"test": "ok"})
        aio_response_mock.get(
            f"{MOCK_ENDPOINT}/service_no_content", status=HTTPStatus.NO_CONTENT, repeat=True
        )

        result1 = await _make_generic_request("GET", f"{MOCK_ENDPOINT}/dummy_service", "test")
        result2 = await _make_generic_request("GET", f"{MOCK_ENDPOINT}/service_no_content", "test")

        assert result1 == {"test": "ok"}
        assert result2 is None
        assert len(aio_response_mock.requests) == 2

    @patch("time.sleep", return_value=None)
    @conf_vars({("edge", "api_url"): MOCK_ENDPOINT})
    async def test_make_generic_request_retry(self, mock_sleep, aio_response_mock: aioresponses):
        aio_response_mock.get(f"{MOCK_ENDPOINT}/flaky_service", status=HTTPStatus.SERVICE_UNAVAILABLE)
        aio_response_mock.get(f"{MOCK_ENDPOINT}/flaky_service", status=HTTPStatus.SERVICE_UNAVAILABLE)
        aio_response_mock.get(f"{MOCK_ENDPOINT}/flaky_service", status=HTTPStatus.SERVICE_UNAVAILABLE)
        aio_response_mock.get(f"{MOCK_ENDPOINT}/flaky_service", exception=ConnectTimeout())
        aio_response_mock.get(f"{MOCK_ENDPOINT}/flaky_service", body={"test": 42})

        result = await _make_generic_request("GET", f"{MOCK_ENDPOINT}/flaky_service", "test")

        assert result == {"test": 42}
        assert len(aio_response_mock.requests) == 5

    @patch("time.sleep", return_value=None)
    @conf_vars({("edge", "api_url"): MOCK_ENDPOINT})
    async def test_make_generic_request_unrecoverable_error(
        self, mock_sleep, aio_response_mock: aioresponses
    ):
        aio_response_mock.get(
            f"{MOCK_ENDPOINT}/flaky_service", status=HTTPStatus.INTERNAL_SERVER_ERROR, repeat=True
        )

        with pytest.raises(HTTPError) as err:
            await _make_generic_request("GET", f"{MOCK_ENDPOINT}/broken_service", "test")

        assert err.value.response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert len(aio_response_mock.requests) == 10
