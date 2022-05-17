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

import json
import unittest
from unittest import mock

import pytest
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusMessageBatch

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.asb_message import ServiceBusMessageHook


class TestServiceBusMessageHook(unittest.TestCase):
    def setUp(self) -> None:
        self.queue_name: str = "test_queue"
        self.conn_id: str = 'azure_service_bus_default'
        self.connection_string = (
            "Endpoint=sb://test-service-bus-provider.servicebus.windows.net/;"
            "SharedAccessKeyName=Test;SharedAccessKey=1234566acbc"
        )
        self.client_id = "test_client_id"
        self.secret_key = "test_client_secret"
        self.conn = Connection(
            conn_id='azure_service_bus_default',
            conn_type='azure_service_bus',
            login=self.client_id,
            password=self.secret_key,
            extra=json.dumps({'connection_string': self.connection_string}),
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_connection")
    def test_get_service_bus_message_conn(self, mock_connection):
        mock_connection.return_value = self.conn
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.conn_id)
        assert isinstance(hook.get_conn(), ServiceBusClient)

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_connection")
    def test_get_conn_value_error(self, mock_connection):
        mock_connection.return_value = Connection(
            conn_id='azure_service_bus_default',
            conn_type='azure_service_bus',
            login=self.client_id,
            password=self.secret_key,
            extra=json.dumps({"connection_string": "test connection"}),
        )
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(ValueError):
            hook.get_conn()

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn')
    def test_send_message_without_batch(self, mock_sb_client):
        """
        Test `send_message` hook function with batch flag as `False`, which will be a normal message,
        mock the azure service bus `send_messages` function
        """
        hook = ServiceBusMessageHook(azure_service_bus_conn_id="azure_service_bus_default")
        hook.send_message(queue_name=self.queue_name, message="test message", batch_message_flag=False)
        expected_calls = [
            mock.call()
            .__enter__()
            .get_queue_sender(self.queue_name)
            .__enter__()
            .send_messages(ServiceBusMessage("test message"))
            .__exit__()
        ]
        mock_sb_client.assert_has_calls(expected_calls, any_order=False)

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn')
    def test_send_message_with_batch(self, mock_sb_client):
        """
        Test `send_message` hook function with batch flag as `True`, which will be considered as
        batch message, mock the azure service bus `send_messages` function
        """
        hook = ServiceBusMessageHook(azure_service_bus_conn_id="azure_service_bus_default")
        hook.send_message(queue_name=self.queue_name, message="test message", batch_message_flag=True)
        expected_calls = [
            mock.call()
            .__enter__()
            .get_queue_sender(self.queue_name)
            .__enter__()
            .send_messages(ServiceBusMessageBatch("test message"))
            .__exit__()
        ]
        mock_sb_client.assert_has_calls(expected_calls, any_order=False)

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn')
    def test_send_message_exception(self, mock_sb_client):
        """
        Test `send_message` functionality to raise AirflowException in Azure ServiceBusMessageHook
         by passing queue name as None
        """
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(AirflowException):
            hook.send_message(queue_name=None, message="", batch_message_flag=False)

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn')
    def test_receive_message(self, mock_sb_client):
        """
        Test `receive_message` hook function and assert the function with mock value,
        mock the azure service bus `receive_messages` function
        """
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.conn_id)
        hook.receive_message(self.queue_name)
        expected_calls = [
            mock.call()
            .__enter__()
            .get_queue_receiver(self.queue_name)
            .__enter__()
            .receive_messages(max_message_count=10, max_wait_time=5)
            .__iter__()
        ]
        mock_sb_client.assert_has_calls(expected_calls)

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn')
    def test_receive_message_exception(self, mock_sb_client):
        """
        Test `receive_message` functionality to raise AirflowException in Azure ServiceBusMessageHook
         by passing queue name as None
        """
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(AirflowException):
            hook.receive_message(None)
