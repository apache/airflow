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
from requests import HTTPError
from requests.exceptions import ConnectTimeout
from requests_mock import ANY

from airflow.providers.edge3.cli.api_client import _make_generic_request

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from requests_mock import Mocker as RequestsMocker

MOCK_ENDPOINT = "https://invalid-api-test-endpoint"


class TestApiClient:
    @conf_vars({("edge", "api_url"): MOCK_ENDPOINT})
    def test_make_generic_request_success(self, requests_mock: RequestsMocker):
        requests_mock.get(
            ANY,
            [
                {"json": {"test": "ok"}},
                {"status_code": HTTPStatus.NO_CONTENT},
            ],
        )

        result1 = _make_generic_request("GET", f"{MOCK_ENDPOINT}/dummy_service", "test")
        result2 = _make_generic_request("GET", f"{MOCK_ENDPOINT}/service_no_content", "test")

        assert result1 == {"test": "ok"}
        assert result2 is None
        assert requests_mock.call_count == 2

    @patch("time.sleep", return_value=None)
    @conf_vars({("edge", "api_url"): MOCK_ENDPOINT})
    def test_make_generic_request_retry(self, mock_sleep, requests_mock: RequestsMocker):
        requests_mock.get(
            ANY,
            [
                *[{"status_code": HTTPStatus.SERVICE_UNAVAILABLE}] * 3,
                {"exc": ConnectTimeout},
                {"json": {"test": 42}},
            ],
        )

        result = _make_generic_request("GET", f"{MOCK_ENDPOINT}/flaky_service", "test")

        assert result == {"test": 42}
        assert requests_mock.call_count == 5

    @patch("time.sleep", return_value=None)
    @conf_vars({("edge", "api_url"): MOCK_ENDPOINT})
    def test_make_generic_request_unrecoverable_error(self, mock_sleep, requests_mock: RequestsMocker):
        requests_mock.get(
            ANY,
            [
                *[{"status_code": HTTPStatus.INTERNAL_SERVER_ERROR}] * 11,
                {"json": {"test": "uups"}},
            ],
        )

        with pytest.raises(HTTPError) as err:
            _make_generic_request("GET", f"{MOCK_ENDPOINT}/broken_service", "test")

        assert err.value.response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert requests_mock.call_count == 10
