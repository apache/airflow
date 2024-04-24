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
from unittest.mock import patch

from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from msgraph_core import APIVersion, NationalClouds

from airflow.providers.microsoft.azure.hooks.msgraph import CallableResponseHandler, KiotaRequestAdapterHook
from tests.providers.microsoft.conftest import (
    get_airflow_connection,
    load_json,
    mock_json_response,
    mock_connection,
)


class TestKiotaRequestAdapterHook:
    def test_get_conn(self):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_conn()

            assert isinstance(actual, HttpxRequestAdapter)
            assert actual.base_url == "https://graph.microsoft.com/v1.0"

    def test_api_version(self):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            assert hook.api_version == APIVersion.v1

    def test_get_api_version_when_empty_config_dict(self):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({})

            assert actual == APIVersion.v1

    def test_get_api_version_when_api_version_in_config_dict(self):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({"api_version": "beta"})

            assert actual == APIVersion.beta

    def test_get_host_when_connection_has_scheme_and_host(self):
        connection = mock_connection(schema="https", host="graph.microsoft.de")
        actual = KiotaRequestAdapterHook.get_host(connection)

        assert actual == NationalClouds.Germany.value

    def test_get_host_when_connection_has_no_scheme_or_host(self):
        connection = mock_connection()
        actual = KiotaRequestAdapterHook.get_host(connection)

        assert actual == NationalClouds.Global.value

    def test_encoded_query_parameters(self):
        actual = KiotaRequestAdapterHook.encoded_query_parameters(
            query_parameters={"$expand": "reports,users,datasets,dataflows,dashboards", "$top": 5000},
        )

        assert actual == {"%24expand": "reports,users,datasets,dataflows,dashboards", "%24top": 5000}


class TestResponseHandler:
    def test_handle_response_async(self):
        users = load_json("resources", "users.json")
        response = mock_json_response(200, users)

        actual = asyncio.run(
            CallableResponseHandler(lambda response, error_map: response.json()).handle_response_async(
                response, None
            )
        )

        assert isinstance(actual, dict)
        assert actual == users
