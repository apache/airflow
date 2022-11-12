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

from unittest import mock

from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook

CLOUD_RUN_CONN_ID = "test-cloud-run-conn"
GCP_CONN_ID = "test-conn"
TOKEN = "TOKEN"
AUTHENTICATION_HEADER = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}
CREDENTIALS = {
    "token": TOKEN,
}


class MockedCredentials:
    def __init__(self):
        self.token = TOKEN

    def refresh(self, *args):
        pass


class TestCloudRunHook:
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.GoogleBaseHook.get_id_token_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.BaseHook.get_connection")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.google.auth.transport.requests.Request")
    def test_get_conn(self, mock_auth_request, mock_base_hook_get_conn, mock_google_get_credentials):
        mock_google_get_credentials.return_value = MockedCredentials()
        hook = CloudRunHook(
            gcp_conn_id=GCP_CONN_ID,
            cloud_run_conn_id=CLOUD_RUN_CONN_ID,
        )
        authentication_header = hook.get_conn()
        mock_base_hook_get_conn.assert_called_with(CLOUD_RUN_CONN_ID)
        mock_auth_request.assert_called_once()

        assert authentication_header == AUTHENTICATION_HEADER
