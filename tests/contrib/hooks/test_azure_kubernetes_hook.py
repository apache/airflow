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
"""
Tests for the Azure Kubernetes Hook
"""

import json
import unittest

from unittest import mock
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.contrib.hooks.azure_kubernetes_hook import AzureKubernetesServiceHook
from airflow.utils import db

CONFIG_DATA = {
    "clientId": "Id",
    "clientSecret": "secret",
    "subscriptionId": "subscription",
    "tenantId": "tenant"
}


class TestAzureKubernetesHook(unittest.TestCase):
    """
    Tests for AKS Hook
    """

    def setUp(self):
        configuration.load_test_config()

        self.conn = conn = mock.MagicMock()
        self.conn.conn_id = 'azure_default'
        self.conn.extra_dejson = {'key_path': 'test.json'}
        db.merge_conn(
            Connection(
                conn_id='azure_default',
                extra=json.dumps({"key_path": "azureauth.json"})
            )
        )

        class AzureKubernetesUnitHook(AzureKubernetesServiceHook):
            """
            Mock AKS hook
            """
            def get_conn(self):
                return conn

            # pylint:disable=unused-argument
            def get_connection(self, connection_id):
                return conn

        self.test_hook = AzureKubernetesUnitHook(conn_id='azure_default')

    @mock.patch('airflow.contrib.hooks.azure_kubernetes_hook.load_json')
    def test_conn(self, mock_json):
        """ Test Connection ID """
        expected_connection = self.test_hook.get_conn()
        mock_json.return_value = CONFIG_DATA
        self.assertEqual(expected_connection.conn_id, 'azure_default')

    # pylint: disable=unused-argument
    @mock.patch('airflow.contrib.hooks.azure_kubernetes_hook.load_json')
    @mock.patch('airflow.contrib.hooks.azure_kubernetes_hook.ServicePrincipalCredentials')
    @mock.patch('os.environ.get', new={'AIRFLOW_CONN_AZURE_DEFAULT': 'azureauth.json'}.get, spec_set=True)
    def test_no_conn_id(self, mock_json, mock_service):
        """ Test without a connection id set """
        from azure.mgmt.containerservice import ContainerServiceClient
        mock_json.return_value = CONFIG_DATA
        hook = AzureKubernetesServiceHook(conn_id=None)
        self.assertEqual(hook.conn_id, None)
        self.assertIsInstance(hook.connection, ContainerServiceClient)

    @mock.patch('os.environ.get', new={'AIRFLOW_CONN_AZURE_DEFAULT': 'azureauth.jpeg'}.get, spec_set=True)
    def test_conn_with_failures(self):
        """ Test a failed connection """
        with self.assertRaises(AirflowException) as ex:
            AzureKubernetesServiceHook(conn_id=None)

        self.assertEqual(str(ex.exception), "Unrecognised extension for key file.")


if __name__ == '__main__':
    unittest.main()
