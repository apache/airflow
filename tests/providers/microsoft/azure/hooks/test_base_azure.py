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

import unittest
from unittest.mock import Mock, patch

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.base_azure import AzureBaseHook


class TestBaseAzureHook(unittest.TestCase):
    @patch('airflow.providers.microsoft.azure.hooks.base_azure.get_client_from_auth_file')
    @patch(
        'airflow.providers.microsoft.azure.hooks.base_azure.AzureBaseHook.get_connection',
        return_value=Connection(conn_id='azure_default', extra='{ "key_path": "key_file.json" }'),
    )
    def test_get_conn_with_key_path(self, mock_connection, mock_get_client_from_auth_file):
        mock_sdk_client = Mock()

        auth_sdk_client = AzureBaseHook(mock_sdk_client).get_conn()

        mock_get_client_from_auth_file.assert_called_once_with(
            client_class=mock_sdk_client, auth_path=mock_connection.return_value.extra_dejson['key_path']
        )
        assert auth_sdk_client == mock_get_client_from_auth_file.return_value

    @patch('airflow.providers.microsoft.azure.hooks.base_azure.get_client_from_json_dict')
    @patch(
        'airflow.providers.microsoft.azure.hooks.base_azure.AzureBaseHook.get_connection',
        return_value=Connection(conn_id='azure_default', extra='{ "key_json": { "test": "test" } }'),
    )
    def test_get_conn_with_key_json(self, mock_connection, mock_get_client_from_json_dict):
        mock_sdk_client = Mock()

        auth_sdk_client = AzureBaseHook(mock_sdk_client).get_conn()

        mock_get_client_from_json_dict.assert_called_once_with(
            client_class=mock_sdk_client, config_dict=mock_connection.return_value.extra_dejson['key_json']
        )
        assert auth_sdk_client == mock_get_client_from_json_dict.return_value

    @patch('airflow.providers.microsoft.azure.hooks.base_azure.ServicePrincipalCredentials')
    @patch(
        'airflow.providers.microsoft.azure.hooks.base_azure.AzureBaseHook.get_connection',
        return_value=Connection(
            conn_id='azure_default',
            login='my_login',
            password='my_password',
            extra='{ "tenantId": "my_tenant", "subscriptionId": "my_subscription" }',
        ),
    )
    def test_get_conn_with_credentials(self, mock_connection, mock_spc):
        mock_sdk_client = Mock()

        auth_sdk_client = AzureBaseHook(mock_sdk_client).get_conn()

        mock_spc.assert_called_once_with(
            client_id=mock_connection.return_value.login,
            secret=mock_connection.return_value.password,
            tenant=mock_connection.return_value.extra_dejson['tenantId'],
        )
        mock_sdk_client.assert_called_once_with(
            credentials=mock_spc.return_value,
            subscription_id=mock_connection.return_value.extra_dejson['subscriptionId'],
        )
        assert auth_sdk_client == mock_sdk_client.return_value
