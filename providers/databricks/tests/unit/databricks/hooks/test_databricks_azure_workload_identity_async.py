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

import json
import os
from unittest import mock

import pytest
import tenacity
from azure.core.credentials import AccessToken

from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_base import DEFAULT_AZURE_CREDENTIAL_SETTING_KEY
from airflow.utils.session import provide_session


def create_successful_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    response.status_code = 200
    return response


def create_aad_token_for_resource() -> AccessToken:
    return AccessToken(expires_on=1575500666, token="sample-token")


HOST = "xx.cloud.databricks.com"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_RETRY_NUMBER = 3
DEFAULT_RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(DEFAULT_RETRY_NUMBER),
)


@pytest.mark.db_test
class TestDatabricksHookAadTokenWorkloadIdentityAsync:
    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.extra = json.dumps(
            {
                DEFAULT_AZURE_CREDENTIAL_SETTING_KEY: True,
            }
        )
        session.commit()

    @pytest.mark.asyncio
    @mock.patch(
        "azure.identity.aio.DefaultAzureCredential.get_token", return_value=create_aad_token_for_resource()
    )
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_one(self, requests_mock, get_token_mock: mock.MagicMock):
        with mock.patch.dict(
            os.environ,
            {
                "AZURE_CLIENT_ID": "fake-client-id",
                "AZURE_TENANT_ID": "fake-tenant-id",
                "AZURE_FEDERATED_TOKEN_FILE": "/badpath",
                "KUBERNETES_SERVICE_HOST": "fakeip",
            },
        ):
            requests_mock.return_value.__aenter__.return_value.json.side_effect = mock.AsyncMock(
                side_effect=[{"data": 1}]
            )

            async with DatabricksHook(retry_args=DEFAULT_RETRY_ARGS) as hook:
                result = await hook.a_get_run_output(0)

            assert result == {"data": 1}
