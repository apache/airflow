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

import os
from unittest.mock import patch

import httpx
import pytest

from airflowctl.api.client import Client, ClientKind, Credentials

pytest_plugins = "tests_common.pytest_plugin"

# Task SDK does not need access to the Airflow database
os.environ["_AIRFLOW_SKIP_DB_TESTS"] = "true"
os.environ["_AIRFLOW__AS_LIBRARY"] = "true"


@pytest.hookimpl()
def pytest_addhooks(pluginmanager: pytest.PytestPluginManager):
    # Python 3.12 starts warning about mixing os.fork + Threads, and the pytest-rerunfailures plugin uses
    # threads internally. Since this is new code, and it should be flake free, we disable the re-run failures
    # plugin early (so that it doesn't run it's pytest_configure which is where the thread starts up if xdist
    # is discovered).
    pluginmanager.set_blocked("rerunfailures")


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    if next(item.iter_markers(name="db_test"), None):
        pytest.fail("Airflow CTL tests must not use database")


@pytest.fixture(scope="session")
def api_client_maker(client_credentials):
    """
    Create a CLI API client with a custom transport and returns callable to create a client with a custom transport
    """

    def make_api_client(transport: httpx.MockTransport, kind: ClientKind = ClientKind.CLI) -> Client:
        """Get a client with a custom transport"""
        return Client(base_url="test://server", transport=transport, token="", kind=kind)

    def _api_client(
        path: str, response_json: dict, expected_http_status_code: int, kind: ClientKind = ClientKind.CLI
    ) -> Client:
        """Get a client with a custom transport"""

        def handle_request(request: httpx.Request) -> httpx.Response:
            """Handle the request and return a response"""
            assert request.url.path == path
            return httpx.Response(expected_http_status_code, json=response_json)

        return make_api_client(transport=httpx.MockTransport(handle_request), kind=kind)

    return _api_client


@pytest.fixture(scope="session")
def client_credentials():
    """Create credentials for CLI API"""
    with patch("airflowctl.api.client.keyring"):
        Credentials(api_url="http://localhost:9091", api_token="NO_TOKEN").save()
