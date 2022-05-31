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

import datetime
import unittest
from unittest import mock

from azure.servicebus import ServiceBusMessage

from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.azure_service_bus_queue import (
    AzureServiceBusCreateQueueOperator,
    AzureServiceBusDeleteQueueOperator,
    AzureServiceBusReceiveMessageOperator,
    AzureServiceBusSendMessageOperator,
)

QUEUE_NAME = "test_queue"
OWNER_NAME = "airflow"
DAG_ID = "test_azure_service_bus_queue"


class TestAzureServiceBusCreateQueueOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating AzureServiceBusCreateQueueOperator with task id, queue_name and
         asserting with value
        """
        asb_create_queue_operator = AzureServiceBusCreateQueueOperator(
            task_id="asb_create_queue",
            queue_name=QUEUE_NAME,
            dag=self.dag,
            max_delivery_count=10,
            dead_lettering_on_message_expiration=True,
            enable_batched_operations=True,
        )
        assert asb_create_queue_operator.task_id == "asb_create_queue"
        assert asb_create_queue_operator.queue_name == QUEUE_NAME
        assert asb_create_queue_operator.max_delivery_count == 10
        assert asb_create_queue_operator.dead_lettering_on_message_expiration is True
        assert asb_create_queue_operator.enable_batched_operations is True

        asb_create_queue_operator = AzureServiceBusCreateQueueOperator(
            task_id="asb_create_queue_test",
            queue_name=QUEUE_NAME,
            dag=self.dag,
            max_delivery_count=10,
            dead_lettering_on_message_expiration=False,
            enable_batched_operations=False,
        )
        assert asb_create_queue_operator.task_id == "asb_create_queue_test"
        assert asb_create_queue_operator.queue_name == QUEUE_NAME
        assert asb_create_queue_operator.max_delivery_count == 10
        assert asb_create_queue_operator.dead_lettering_on_message_expiration is False
        assert asb_create_queue_operator.enable_batched_operations is False

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.asb_admin_client.AzureServiceBusAdminClientHook.get_conn"
    )
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
            dag=self.dag,
        )
        asb_create_queue_operator.execute(None)
        mock_get_conn.return_value.__enter__.return_value.create_queue.assert_called_once_with(
            QUEUE_NAME,
            max_delivery_count=10,
            dead_lettering_on_message_expiration=True,
            enable_batched_operations=True,
        )


class TestAzureServiceBusDeleteQueueOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating AzureServiceBusDeleteQueueOperator with task id, queue_name and
         asserting with values
        """
        asb_delete_queue_operator = AzureServiceBusDeleteQueueOperator(
            task_id="asb_delete_queue",
            queue_name=QUEUE_NAME,
            dag=self.dag,
        )
        assert asb_delete_queue_operator.task_id == "asb_delete_queue"
        assert asb_delete_queue_operator.queue_name == QUEUE_NAME

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.asb_admin_client.AzureServiceBusAdminClientHook.get_conn"
    )
    def test_delete_queue(self, mock_get_conn):
        """Test AzureServiceBusDeleteQueueOperator by mocking queue name, connection and hook delete_queue"""
        asb_delete_queue_operator = AzureServiceBusDeleteQueueOperator(
            task_id="asb_delete_queue",
            queue_name=QUEUE_NAME,
            dag=self.dag,
        )
        asb_delete_queue_operator.execute(None)
        mock_get_conn.return_value.__enter__.return_value.delete_queue.assert_called_once_with(QUEUE_NAME)


class TestAzureServiceBusSendMessageOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating AzureServiceBusSendMessageOperator with task id, queue_name, message,
        batch and asserting with values
        """
        msg = "test message"
        asb_send_message_queue_operator = AzureServiceBusSendMessageOperator(
            task_id="asb_send_message_queue_without_batch",
            queue_name=QUEUE_NAME,
            message=msg,
            batch=False,
            dag=self.dag,
        )
        assert asb_send_message_queue_operator.task_id == "asb_send_message_queue_without_batch"
        assert asb_send_message_queue_operator.queue_name == QUEUE_NAME
        assert asb_send_message_queue_operator.message == msg
        assert asb_send_message_queue_operator.batch is False

        asb_send_message_queue_operator = AzureServiceBusSendMessageOperator(
            task_id="asb_send_message_queue_with_batch",
            queue_name=QUEUE_NAME,
            message=msg,
            batch=True,
            dag=self.dag,
        )
        assert asb_send_message_queue_operator.task_id == "asb_send_message_queue_with_batch"
        assert asb_send_message_queue_operator.queue_name == QUEUE_NAME
        assert asb_send_message_queue_operator.message == msg
        assert asb_send_message_queue_operator.batch is True

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn")
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
            dag=self.dag,
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


class TestAzureServiceBusReceiveMessageOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating AzureServiceBusReceiveMessageOperator with task id, queue_name, message,
        batch and asserting with values
        """

        asb_receive_queue_operator = AzureServiceBusReceiveMessageOperator(
            task_id="asb_receive_message_queue",
            queue_name=QUEUE_NAME,
            dag=self.dag,
        )
        assert asb_receive_queue_operator.task_id == "asb_receive_message_queue"
        assert asb_receive_queue_operator.queue_name == QUEUE_NAME

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn")
    def test_receive_message_queue(self, mock_get_conn):
        """
        Test AzureServiceBusReceiveMessageOperator by mock connection, values
        and the service bus receive message
        """
        asb_receive_queue_operator = AzureServiceBusReceiveMessageOperator(
            task_id="asb_receive_message_queue",
            queue_name=QUEUE_NAME,
            dag=self.dag,
        )
        asb_receive_queue_operator.execute(None)
        expected_calls = [
            mock.call(),
            mock.call().__enter__(),
            mock.call().get_queue_receiver(queue_name='test_queue'),
            mock.call().get_queue_receiver().__enter__(),
            mock.call().get_queue_receiver().receive_messages(max_message_count=10, max_wait_time=5),
            mock.call().get_queue_receiver().receive_messages().__iter__(),
            mock.call().get_queue_receiver().__exit__(None, None, None),
            mock.call().__exit__(None, None, None),
        ]
        mock_get_conn.assert_has_calls(expected_calls)
