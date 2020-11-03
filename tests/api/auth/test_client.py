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

import unittest
from unittest import mock

from airflow.api.client import get_current_api_client
from tests.test_utils.config import conf_vars


class TestGetCurrentApiClient(unittest.TestCase):

    @mock.patch("airflow.api.client.json_client.Client")
    @mock.patch("airflow.api.auth.backend.default.CLIENT_AUTH", "CLIENT_AUTH")
    @conf_vars({
        ("api", 'auth_backend'): 'airflow.api.auth.backend.default',
        ("cli", 'api_client'): 'airflow.api.client.json_client',
        ("cli", 'endpoint_url'): 'http://localhost:1234',
    })
    def test_should_create_client(self, mock_client):
        result = get_current_api_client()

        mock_client.assert_called_once_with(
            api_base_url='http://localhost:1234', auth='CLIENT_AUTH', session=None
        )
        self.assertEqual(mock_client.return_value, result)

    @mock.patch("airflow.api.client.json_client.Client")
    @mock.patch("airflow.providers.google.common.auth_backend.google_openid.create_client_session")
    @conf_vars({
        ("api", 'auth_backend'): 'airflow.providers.google.common.auth_backend.google_openid',
        ("cli", 'api_client'): 'airflow.api.client.json_client',
        ("cli", 'endpoint_url'): 'http://localhost:1234',
    })
    def test_should_create_google_open_id_client(self, mock_create_client_session, mock_client):
        result = get_current_api_client()

        mock_client.assert_called_once_with(
            api_base_url='http://localhost:1234',
            auth=None,
            session=mock_create_client_session.return_value
        )
        self.assertEqual(mock_client.return_value, result)
