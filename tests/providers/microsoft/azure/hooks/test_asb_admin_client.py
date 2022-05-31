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

from unittest import mock

import pytest
from azure.servicebus.management import ServiceBusAdministrationClient

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.asb_admin_client import AzureServiceBusAdminClientHook


class TestAzureServiceBusAdminClientHook:
    def setup_class(self) -> None:
        self.queue_name: str = "test_queue"
        self.conn_id: str = 'azure_service_bus_default'
        self.connection_string = (
            "Endpoint=sb://test-service-bus-provider.servicebus.windows.net/;"
            "SharedAccessKeyName=Test;SharedAccessKey=1234566acbc"
        )
        self.client_id = "test_client_id"
        self.secret_key = "test_client_secret"
        self.mock_conn = Connection(
            conn_id='azure_service_bus_default',
            conn_type='azure_service_bus',
            login=self.client_id,
            password=self.secret_key,
            extra={"connection_string": self.connection_string},
        )

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.asb_admin_client."
        "AzureServiceBusAdminClientHook.get_connection"
    )
    def test_get_conn(self, mock_connection):
        mock_connection.return_value = self.mock_conn
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.conn_id)
        assert isinstance(hook.get_conn(), ServiceBusAdministrationClient)

    @mock.patch('azure.servicebus.management.QueueProperties')
    @mock.patch(
        'airflow.providers.microsoft.azure.hooks.asb_admin_client.AzureServiceBusAdminClientHook.get_conn'
    )
    def test_create_queue(self, mock_sb_admin_client, mock_queue_properties):
        """
        Test `create_queue` hook function with mocking connection, queue properties value and
        the azure service bus `create_queue` function
        """
        mock_queue_properties.name = self.queue_name
        mock_sb_admin_client.return_value.__enter__.return_value.create_queue.return_value = (
            mock_queue_properties
        )
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.conn_id)
        response = hook.create_queue(self.queue_name)
        assert response == mock_queue_properties

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb_admin_client.ServiceBusAdministrationClient')
    def test_create_queue_exception(self, mock_sb_admin_client):
        """Test `create_queue` functionality to raise ValueError by passing queue name as None"""
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(ValueError):
            hook.create_queue(None)

    @mock.patch(
        'airflow.providers.microsoft.azure.hooks.asb_admin_client.AzureServiceBusAdminClientHook.get_conn'
    )
    def test_delete_queue(self, mock_sb_admin_client):
        """
        Test Delete queue functionality by passing queue name, assert the function with values,
        mock the azure service bus function  `delete_queue`
        """
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.conn_id)
        hook.delete_queue(self.queue_name)
        expected_calls = [mock.call().__enter__().delete_queue(self.queue_name)]
        mock_sb_admin_client.assert_has_calls(expected_calls)

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb_admin_client.ServiceBusAdministrationClient')
    def test_delete_queue_exception(self, mock_sb_admin_client):
        """Test `delete_queue` functionality to raise ValueError, by passing queue name as None"""
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(ValueError):
            hook.delete_queue(None)
