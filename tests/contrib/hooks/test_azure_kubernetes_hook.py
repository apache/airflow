# -*- coding: utf-8 -*-
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
#

import json
import unittest

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow import models
from airflow.contrib.hooks.azure_kubernetes_hook import AzureKubernetesServiceHook
from airflow.utils import db

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

CONFIG_DATA = {
    "clientId": "Id",
    "clientSecret": "secret",
    "subscriptionId": "subscription",
    "tenantId": "tenant"
}


class TestAzureKubernetesHook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='azure_default',
                extra=json.dumps({"key_path": "azureauth.json"})
            )
        )

    @mock.patch('airflow.contrib.hooks.azure_kubernetes_hook.load_json')
    @mock.patch('airflow.contrib.hooks.azure_kubernetes_hook.ServicePrincipalCredentials')
    def test_conn(self, mock_json, mock_service):
        from azure.mgmt.containerservice import ContainerServiceClient
        mock_json.return_value = CONFIG_DATA
        hook = AzureKubernetesServiceHook(conn_id='azure_default')
        self.assertEqual(hook.conn_id, 'azure_default')
        self.assertIsInstance(hook.connection, ContainerServiceClient)

    @mock.patch('airflow.contrib.hooks.azure_kubernetes_hook.load_json')
    @mock.patch('airflow.contrib.hooks.azure_kubernetes_hook.ServicePrincipalCredentials')
    @mock.patch('os.environ.get', new={'AZURE_AUTH_LOCATION': 'azureauth.json'}.get, spec_set=True)
    def test_conn_env(self, mock_json, mock_service):
        from azure.mgmt.containerservice import ContainerServiceClient
        mock_json.return_value = CONFIG_DATA
        hook = AzureKubernetesServiceHook(conn_id=None)
        self.assertEqual(hook.conn_id, None)
        self.assertIsInstance(hook.connection, ContainerServiceClient)

    @mock.patch('os.environ.get', new={'AZURE_AUTH_LOCATION': 'azureauth.jpeg'}.get, spec_set=True)
    def test_conn_with_failures(self):
        with self.assertRaises(AirflowException) as ex:
            AzureKubernetesServiceHook(conn_id=None)

        self.assertEqual(str(ex.exception), "Unrecognised extension for key file.")


if __name__ == '__main__':
    unittest.main()
