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
from azure.servicebus import ServiceBusMessage

from airflow.providers.microsoft.azure.operators.asb import (
    AzureServiceBusCreateQueueOperator,
    AzureServiceBusDeleteQueueOperator,
    AzureServiceBusReceiveMessageOperator,
    AzureServiceBusSendMessageOperator,
)

QUEUE_NAME = "test_queue"
MESSAGE = "Test Message"
MESSAGE_LIST = [MESSAGE + " " + str(n) for n in range(0, 10)]


class TestAzureServiceBusCreateQueueOperator:
    @pytest.mark.parametrize(
        "mock_dl_msg_expiration, mock_batched_operation",
        [
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ],
    )
    def test_init(self, mock_dl_msg_expiration, mock_batched_operation):
        """
        Test init by creating AzureServiceBusCreateQueueOperator with task id,
        queue_name and asserting with value
        """
        asb_create_queue_operator = AzureServiceBusCreateQueueOperator(
            task_id="asb_create_queue",
            queue_name=QUEUE_NAME,
            max_delivery_count=10,
            dead_lettering_on_message_expiration=mock_dl_msg_expiration,
            enable_batched_operations=mock_batched_operation,
        )
        assert asb_create_queue_operator.task_id == "asb_create_queue"
        assert asb_create_queue_operator.queue_name == QUEUE_NAME
        assert asb_create_queue_operator.max_delivery_count == 10
        assert asb_create_queue_operator.dead_lettering_on_message_expiration is mock_dl_msg_expiration
        assert asb_create_queue_operator.enable_batched_operations is mock_batched_operation

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    def test_create_queue(self, mock_get_conn):
        """
        Test AzureServiceBusCreateQueueOperator passed with the queue name,
        mocking the connection details, hook create_queue function
        """
        asb_create_queue_operator = AzureServiceBusCreateQueueOperator(
            task_id="asb_create_queue_operator",
            queue_name=QUEUE_NAME,
            max_delivery_count=10,
            dead_lettering_on_message_expiration=True,
            enable_batched_operations=True,
        )
        asb_create_queue_operator.execute(None)
        mock_get_conn.return_value.__enter__.return_value.create_queue.assert_called_once_with(
            QUEUE_NAME,
            max_delivery_count=10,
            dead_lettering_on_message_expiration=True,
            enable_batched_operations=True,
        )


class TestAzureServiceBusDeleteQueueOperator:
    def test_init(self):
        """
        Test init by creating AzureServiceBusDeleteQueueOperator with task id, queue_name and asserting
        with values
        """
        asb_delete_queue_operator = AzureServiceBusDeleteQueueOperator(
            task_id="asb_delete_queue",
            queue_name=QUEUE_NAME,
        )
        assert asb_delete_queue_operator.task_id == "asb_delete_queue"
        assert asb_delete_queue_operator.queue_name == QUEUE_NAME

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    def test_delete_queue(self, mock_get_conn):
        """Test AzureServiceBusDeleteQueueOperator by mocking queue name, connection and hook delete_queue"""
        asb_delete_queue_operator = AzureServiceBusDeleteQueueOperator(
            task_id="asb_delete_queue",
            queue_name=QUEUE_NAME,
        )
        asb_delete_queue_operator.execute(None)
        mock_get_conn.return_value.__enter__.return_value.delete_queue.assert_called_once_with(QUEUE_NAME)


class TestAzureServiceBusSendMessageOperator:
    @pytest.mark.parametrize(
        "mock_message, mock_batch_flag",
        [
            (MESSAGE, True),
            (MESSAGE, False),
            (MESSAGE_LIST, True),
            (MESSAGE_LIST, False),
        ],
    )
    def test_init(self, mock_message, mock_batch_flag):
        """
        Test init by creating AzureServiceBusSendMessageOperator with task id, queue_name, message,
        batch and asserting with values
        """
        asb_send_message_queue_operator = AzureServiceBusSendMessageOperator(
            task_id="asb_send_message_queue_without_batch",
            queue_name=QUEUE_NAME,
            message=mock_message,
            batch=mock_batch_flag,
        )
        assert asb_send_message_queue_operator.task_id == "asb_send_message_queue_without_batch"
        assert asb_send_message_queue_operator.queue_name == QUEUE_NAME
        assert asb_send_message_queue_operator.message == mock_message
        assert asb_send_message_queue_operator.batch is mock_batch_flag

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook.get_conn")
    def test_send_message_queue(self, mock_get_conn):
        """
        Test AzureServiceBusSendMessageOperator with queue name, batch boolean flag, mock
        the send_messages of azure service bus function
        """
        asb_send_message_queue_operator = AzureServiceBusSendMessageOperator(
            task_id="asb_send_message_queue",
            queue_name=QUEUE_NAME,
            message="Test message",
            batch=False,
        )
        asb_send_message_queue_operator.execute(None)
        expected_calls = [
            mock.call()
            .__enter__()
            .get_queue_sender(QUEUE_NAME)
            .__enter__()
            .send_messages(ServiceBusMessage("Test message"))
            .__exit__()
        ]
        mock_get_conn.assert_has_calls(expected_calls, any_order=False)


class TestAzureServiceBusReceiveMessageOperator:
    def test_init(self):
        """
        Test init by creating AzureServiceBusReceiveMessageOperator with task id, queue_name, message,
        batch and asserting with values
        """

        asb_receive_queue_operator = AzureServiceBusReceiveMessageOperator(
            task_id="asb_receive_message_queue",
            queue_name=QUEUE_NAME,
        )
        assert asb_receive_queue_operator.task_id == "asb_receive_message_queue"
        assert asb_receive_queue_operator.queue_name == QUEUE_NAME

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook.get_conn")
    def test_receive_message_queue(self, mock_get_conn):
        """
        Test AzureServiceBusReceiveMessageOperator by mock connection, values
        and the service bus receive message
        """
        asb_receive_queue_operator = AzureServiceBusReceiveMessageOperator(
            task_id="asb_receive_message_queue",
            queue_name=QUEUE_NAME,
        )
        asb_receive_queue_operator.execute(None)
        expected_calls = [
            mock.call()
            .__enter__()
            .get_queue_receiver(QUEUE_NAME)
            .__enter__()
            .receive_messages(max_message_count=10, max_wait_time=5)
            .get_queue_receiver(QUEUE_NAME)
            .__exit__()
            .mock_call()
            .__exit__
        ]
        mock_get_conn.assert_has_calls(expected_calls)
