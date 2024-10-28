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

from typing import Any
from unittest import mock

import pytest

try:
    from azure.servicebus import (
        ServiceBusClient,
        ServiceBusMessage,
        ServiceBusMessageBatch,
    )
    from azure.servicebus.management import ServiceBusAdministrationClient
except ImportError:
    pytest.skip("Azure Service Bus not available", allow_module_level=True)

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.asb import AdminClientHook, MessageHook
from airflow.utils.context import Context

MESSAGE = "Test Message"
MESSAGE_LIST = [f"{MESSAGE} {n}" for n in range(10)]
MODULE = "airflow.providers.microsoft.azure.hooks.asb"


class TestAdminClientHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        self.queue_name = "test_queue"
        self.conn_id = "azure_service_bus_default"
        self.connection_string = (
            "Endpoint=sb://test-service-bus-provider.servicebus.windows.net/;"
            "SharedAccessKeyName=Test;SharedAccessKey=1234566acbc"
        )
        self.mock_conn = create_mock_connection(
            Connection(
                conn_id=self.conn_id,
                conn_type="azure_service_bus",
                schema=self.connection_string,
            )
        )
        self.mock_conn_without_schema = Connection(
            conn_id="azure_service_bus_default",
            conn_type="azure_service_bus",
            schema="",
            extra={"fully_qualified_namespace": "fully_qualified_namespace"},
        )

    def test_get_conn(self):
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        assert isinstance(hook.get_conn(), ServiceBusAdministrationClient)

    @mock.patch(f"{MODULE}.get_sync_default_azure_credential")
    @mock.patch(f"{MODULE}.AdminClientHook.get_connection")
    def test_get_conn_fallback_to_default_azure_credential_when_schema_is_not_provided(
        self, mock_connection, mock_default_azure_credential
    ):
        mock_connection.return_value = self.mock_conn_without_schema
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        assert isinstance(hook.get_conn(), ServiceBusAdministrationClient)
        mock_default_azure_credential.assert_called_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )

    @mock.patch("azure.servicebus.management.QueueProperties")
    @mock.patch(f"{MODULE}.AdminClientHook.get_conn")
    def test_create_queue(self, mock_sb_admin_client, mock_queue_properties):
        """
        Test `create_queue` hook function with mocking connection, queue properties value and
        the azure service bus `create_queue` function
        """
        mock_queue_properties.name = self.queue_name
        mock_sb_admin_client.return_value.__enter__.return_value.create_queue.return_value = mock_queue_properties
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        response = hook.create_queue(self.queue_name)
        assert response == mock_queue_properties

    @mock.patch(f"{MODULE}.ServiceBusAdministrationClient")
    def test_create_queue_exception(self, mock_sb_admin_client):
        """Test `create_queue` functionality to raise ValueError by passing queue name as None"""
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(TypeError):
            hook.create_queue(None)

    @mock.patch(f"{MODULE}.AdminClientHook.get_conn")
    def test_delete_queue(self, mock_sb_admin_client):
        """
        Test Delete queue functionality by passing queue name, assert the function with values,
        mock the azure service bus function  `delete_queue`
        """
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        hook.delete_queue(self.queue_name)
        expected_calls = [mock.call().__enter__().delete_queue(self.queue_name)]
        mock_sb_admin_client.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.ServiceBusAdministrationClient")
    def test_delete_queue_exception(self, mock_sb_admin_client):
        """Test `delete_queue` functionality to raise ValueError, by passing queue name as None"""
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(TypeError):
            hook.delete_queue(None)

    @mock.patch(f"{MODULE}.AdminClientHook.get_conn")
    def test_delete_subscription(self, mock_sb_admin_client):
        """
        Test Delete subscription functionality by passing subscription name and topic name,
        assert the function with values, mock the azure service bus function  `delete_subscription`
        """
        subscription_name = "test_subscription_name"
        topic_name = "test_topic_name"
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        hook.delete_subscription(subscription_name, topic_name)
        expected_calls = [
            mock.call().__enter__().delete_subscription(topic_name, subscription_name)
        ]
        mock_sb_admin_client.assert_has_calls(expected_calls)

    @pytest.mark.parametrize(
        "mock_subscription_name, mock_topic_name",
        [("subscription_1", None), (None, "topic_1")],
    )
    @mock.patch(f"{MODULE}.AdminClientHook")
    def test_delete_subscription_exception(
        self, mock_sb_admin_client, mock_subscription_name, mock_topic_name
    ):
        """
        Test `delete_subscription` functionality to raise AirflowException,
         by passing subscription name and topic name as None and pytest raise Airflow Exception
        """
        hook = AdminClientHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(TypeError):
            hook.delete_subscription(mock_subscription_name, mock_topic_name)


class TestMessageHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        self.queue_name = "test_queue"
        self.conn_id = "azure_service_bus_default"
        self.connection_string = (
            "Endpoint=sb://test-service-bus-provider.servicebus.windows.net/;"
            "SharedAccessKeyName=Test;SharedAccessKey=1234566acbc"
        )
        self.mock_conn = create_mock_connection(
            Connection(
                conn_id=self.conn_id,
                conn_type="azure_service_bus",
                schema=self.connection_string,
            )
        )
        self.mock_conn_without_schema = Connection(
            conn_id="azure_service_bus_default",
            conn_type="azure_service_bus",
            schema="",
            extra={"fully_qualified_namespace": "fully_qualified_namespace"},
        )

    def test_get_service_bus_message_conn(self):
        """
        Test get_conn() function and check whether the get_conn() function returns value
        is instance of ServiceBusClient
        """
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)
        assert isinstance(hook.get_conn(), ServiceBusClient)

    @mock.patch(f"{MODULE}.get_sync_default_azure_credential")
    @mock.patch(f"{MODULE}.MessageHook.get_connection")
    def test_get_conn_fallback_to_default_azure_credential_when_schema_is_not_provided(
        self, mock_connection, mock_default_azure_credential
    ):
        mock_connection.return_value = self.mock_conn_without_schema
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)
        assert isinstance(hook.get_conn(), ServiceBusClient)
        mock_default_azure_credential.assert_called_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )

    @pytest.mark.parametrize(
        "mock_message, mock_batch_flag",
        [
            (MESSAGE, True),
            (MESSAGE, False),
            (MESSAGE_LIST, True),
            (MESSAGE_LIST, False),
        ],
    )
    @mock.patch(f"{MODULE}.MessageHook.send_list_messages")
    @mock.patch(f"{MODULE}.MessageHook.send_batch_message")
    @mock.patch(f"{MODULE}.MessageHook.get_conn")
    def test_send_message(
        self,
        mock_sb_client,
        mock_batch_message,
        mock_list_message,
        mock_message,
        mock_batch_flag,
    ):
        """
        Test `send_message` hook function with batch flag and message passed as mocked params,
        which can be string or list of string, mock the azure service bus `send_messages` function
        """
        hook = MessageHook(azure_service_bus_conn_id="azure_service_bus_default")
        hook.send_message(
            queue_name=self.queue_name,
            messages=mock_message,
            batch_message_flag=mock_batch_flag,
        )
        if isinstance(mock_message, list):
            if mock_batch_flag:
                message = ServiceBusMessageBatch(mock_message)
            else:
                message = [ServiceBusMessage(msg) for msg in mock_message]
        elif isinstance(mock_message, str):
            if mock_batch_flag:
                message = ServiceBusMessageBatch(mock_message)
            else:
                message = ServiceBusMessage(mock_message)

        expected_calls = [
            mock.call()
            .__enter__()
            .get_queue_sender(self.queue_name)
            .__enter__()
            .send_messages(message)
            .__exit__()
        ]
        mock_sb_client.assert_has_calls(expected_calls, any_order=False)

    @mock.patch(f"{MODULE}.MessageHook.get_conn")
    def test_send_message_exception(self, mock_sb_client):
        """
        Test `send_message` functionality to raise AirflowException in Azure MessageHook
        by passing queue name as None
        """
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(TypeError):
            hook.send_message(queue_name=None, messages="", batch_message_flag=False)

    @mock.patch("azure.servicebus.ServiceBusMessage")
    @mock.patch(f"{MODULE}.MessageHook.get_conn")
    def test_receive_message(self, mock_sb_client, mock_service_bus_message):
        """
        Test `receive_message` hook function and assert the function with mock value,
        mock the azure service bus `receive_messages` function
        """
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)
        mock_sb_client.return_value.get_queue_receiver.return_value.receive_messages.return_value = [
            mock_service_bus_message
        ]
        hook.receive_message(self.queue_name, Context())
        expected_calls = [
            mock.call()
            .__enter__()
            .get_queue_receiver(self.queue_name)
            .__enter__()
            .receive_messages(max_message_count=30, max_wait_time=5)
            .get_queue_receiver(self.queue_name)
            .__exit__()
            .mock_call()
            .__exit__
        ]
        mock_sb_client.assert_has_calls(expected_calls)

    @mock.patch("azure.servicebus.ServiceBusReceivedMessage")
    @mock.patch(f"{MODULE}.MessageHook.get_conn", autospec=True)
    def test_receive_message_callback(self, mock_sb_client, mock_service_bus_message):
        """
        Test `receive_message` hook function and assert the function with mock value,
        mock the azure service bus `receive_messages` function
        """
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)

        mock_sb_client.return_value.__enter__.return_value.get_queue_receiver.return_value.__enter__.return_value.receive_messages.return_value = [
            mock_service_bus_message
        ]

        received_messages = []

        def message_callback(msg: Any, context: Context) -> None:
            nonlocal received_messages
            print("received message:", msg)
            assert context is not None
            received_messages.append(msg)

        hook.receive_message(
            self.queue_name, Context(), message_callback=message_callback
        )

        assert len(received_messages) == 1
        assert received_messages[0] == mock_service_bus_message

    @mock.patch(f"{MODULE}.MessageHook.get_conn")
    def test_receive_message_exception(self, mock_sb_client):
        """
        Test `receive_message` functionality to raise AirflowException in Azure MessageHook
        by passing queue name as None
        """
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(TypeError):
            hook.receive_message(None)

    @mock.patch(f"{MODULE}.MessageHook.get_conn")
    def test_receive_subscription_message(self, mock_sb_client):
        """
        Test `receive_subscription_message` hook function and assert the function with mock value,
        mock the azure service bus `receive_message` function of subscription
        """
        subscription_name = "subscription_1"
        topic_name = "topic_name"
        max_message_count = 10
        max_wait_time = 5
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)
        hook.receive_subscription_message(
            topic_name, subscription_name, Context(), max_message_count, max_wait_time
        )
        expected_calls = [
            mock.call()
            .__enter__()
            .get_subscription_receiver(subscription_name, topic_name)
            .__enter__()
            .receive_messages(
                max_message_count=max_message_count, max_wait_time=max_wait_time
            )
            .get_subscription_receiver(subscription_name, topic_name)
            .__exit__()
            .mock_call()
            .__exit__
        ]
        mock_sb_client.assert_has_calls(expected_calls)

    @mock.patch("azure.servicebus.ServiceBusReceivedMessage")
    @mock.patch(f"{MODULE}.MessageHook.get_conn")
    def test_receive_subscription_message_callback(self, mock_sb_client, mock_sb_message):
        """
        Test `receive_subscription_message` hook function and assert the function with mock value,
        mock the azure service bus `receive_message` function of subscription
        """
        subscription_name = "subscription_1"
        topic_name = "topic_name"
        max_message_count = 10
        max_wait_time = 5
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)

        mock_sb_client.return_value.__enter__.return_value.get_subscription_receiver.return_value.__enter__.return_value.receive_messages.return_value = [
            mock_sb_message,
            mock_sb_message,
        ]

        received_messages = []

        def message_callback(msg: ServiceBusMessage, context: Context) -> None:
            nonlocal received_messages
            print("received message:", msg)
            assert context is not None
            received_messages.append(msg)

        hook.receive_subscription_message(
            topic_name,
            subscription_name,
            Context(),
            max_message_count,
            max_wait_time,
            message_callback=message_callback,
        )

        assert len(received_messages) == 2

    @pytest.mark.parametrize(
        "mock_subscription_name, mock_topic_name, mock_max_count, mock_wait_time",
        [("subscription_1", None, None, None), (None, "topic_1", None, None)],
    )
    @mock.patch(f"{MODULE}.MessageHook.get_conn")
    def test_receive_subscription_message_exception(
        self,
        mock_sb_client,
        mock_subscription_name,
        mock_topic_name,
        mock_max_count,
        mock_wait_time,
    ):
        """
        Test `receive_subscription_message` hook function to raise exception
        by sending the subscription and topic name as none
        """
        hook = MessageHook(azure_service_bus_conn_id=self.conn_id)
        with pytest.raises(TypeError):
            hook.receive_subscription_message(
                mock_subscription_name, mock_topic_name, mock_max_count, mock_wait_time
            )
