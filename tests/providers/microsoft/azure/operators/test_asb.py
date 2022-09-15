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

import pytest
from azure.servicebus import ServiceBusMessage

from airflow.providers.microsoft.azure.operators.asb import (
    ASBReceiveSubscriptionMessageOperator,
    AzureServiceBusCreateQueueOperator,
    AzureServiceBusDeleteQueueOperator,
    AzureServiceBusReceiveMessageOperator,
    AzureServiceBusSendMessageOperator,
    AzureServiceBusSubscriptionCreateOperator,
    AzureServiceBusSubscriptionDeleteOperator,
    AzureServiceBusTopicCreateOperator,
    AzureServiceBusTopicDeleteOperator,
    AzureServiceBusUpdateSubscriptionOperator,
)

QUEUE_NAME = "test_queue"
MESSAGE = "Test Message"
MESSAGE_LIST = [MESSAGE + " " + str(n) for n in range(0, 10)]

OWNER_NAME = "airflow"
DAG_ID = "test_azure_service_bus_subscription"
TOPIC_NAME = "sb_mgmt_topic_test"
SUBSCRIPTION_NAME = "sb_mgmt_subscription"


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


class TestABSTopicCreateOperator:
    def test_init(self):
        """
        Test init by creating AzureServiceBusTopicCreateOperator with task id and topic name,
        by asserting the value
        """
        asb_create_topic = AzureServiceBusTopicCreateOperator(
            task_id="asb_create_topic",
            topic_name=TOPIC_NAME,
        )
        assert asb_create_topic.task_id == "asb_create_topic"
        assert asb_create_topic.topic_name == TOPIC_NAME

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    @mock.patch('azure.servicebus.management.TopicProperties')
    def test_create_topic(self, mock_topic_properties, mock_get_conn):
        """
        Test AzureServiceBusTopicCreateOperator passed with the topic name
        mocking the connection details, hook create_topic function
        """
        asb_create_topic = AzureServiceBusTopicCreateOperator(
            task_id="asb_create_topic",
            topic_name=TOPIC_NAME,
        )
        mock_topic_properties.name = TOPIC_NAME
        mock_get_conn.return_value.__enter__.return_value.create_topic.return_value = mock_topic_properties

        with mock.patch.object(asb_create_topic.log, "info") as mock_log_info:
            asb_create_topic.execute(None)
        mock_log_info.assert_called_with("Created Topic %s", TOPIC_NAME)

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb.AdminClientHook')
    def test_create_subscription_exception(self, mock_sb_admin_client):
        """
        Test `AzureServiceBusTopicCreateOperator` functionality to raise AirflowException,
         by passing topic name as None and pytest raise Airflow Exception
        """
        asb_create_topic_exception = AzureServiceBusTopicCreateOperator(
            task_id="create_service_bus_subscription",
            topic_name=None,
        )
        with pytest.raises(TypeError):
            asb_create_topic_exception.execute(None)


class TestASBCreateSubscriptionOperator:
    def test_init(self):
        """
        Test init by creating ASBCreateSubscriptionOperator with task id, subscription name, topic name and
        asserting with value
        """
        asb_create_subscription = AzureServiceBusSubscriptionCreateOperator(
            task_id="asb_create_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
        )
        assert asb_create_subscription.task_id == "asb_create_subscription"
        assert asb_create_subscription.subscription_name == SUBSCRIPTION_NAME
        assert asb_create_subscription.topic_name == TOPIC_NAME

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    @mock.patch('azure.servicebus.management.SubscriptionProperties')
    def test_create_subscription(self, mock_subscription_properties, mock_get_conn):
        """
        Test AzureServiceBusSubscriptionCreateOperator passed with the subscription name, topic name
        mocking the connection details, hook create_subscription function
        """
        asb_create_subscription = AzureServiceBusSubscriptionCreateOperator(
            task_id="create_service_bus_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
        )
        mock_subscription_properties.name = SUBSCRIPTION_NAME
        mock_subscription_properties.to = SUBSCRIPTION_NAME
        mock_get_conn.return_value.__enter__.return_value.create_subscription.return_value = (
            mock_subscription_properties
        )

        with mock.patch.object(asb_create_subscription.log, "info") as mock_log_info:
            asb_create_subscription.execute(None)
        mock_log_info.assert_called_with("Created subscription %s", SUBSCRIPTION_NAME)

    @pytest.mark.parametrize(
        "mock_subscription_name, mock_topic_name",
        [("subscription_1", None), (None, "topic_1")],
    )
    @mock.patch('airflow.providers.microsoft.azure.hooks.asb.AdminClientHook')
    def test_create_subscription_exception(
        self, mock_sb_admin_client, mock_subscription_name, mock_topic_name
    ):
        """
        Test `AzureServiceBusSubscriptionCreateOperator` functionality to raise AirflowException,
         by passing subscription name and topic name as None and pytest raise Airflow Exception
        """
        asb_create_subscription = AzureServiceBusSubscriptionCreateOperator(
            task_id="create_service_bus_subscription",
            topic_name=mock_topic_name,
            subscription_name=mock_subscription_name,
        )
        with pytest.raises(TypeError):
            asb_create_subscription.execute(None)


class TestASBDeleteSubscriptionOperator:
    def test_init(self):
        """
        Test init by creating AzureServiceBusSubscriptionDeleteOperator with task id, subscription name,
        topic name and asserting with values
        """
        asb_delete_subscription_operator = AzureServiceBusSubscriptionDeleteOperator(
            task_id="asb_delete_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
        )
        assert asb_delete_subscription_operator.task_id == "asb_delete_subscription"
        assert asb_delete_subscription_operator.topic_name == TOPIC_NAME
        assert asb_delete_subscription_operator.subscription_name == SUBSCRIPTION_NAME

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    def test_delete_subscription(self, mock_get_conn):
        """
        Test AzureServiceBusSubscriptionDeleteOperator by mocking subscription name, topic name and
         connection and hook delete_subscription
        """
        asb_delete_subscription_operator = AzureServiceBusSubscriptionDeleteOperator(
            task_id="asb_delete_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
        )
        asb_delete_subscription_operator.execute(None)
        mock_get_conn.return_value.__enter__.return_value.delete_subscription.assert_called_once_with(
            TOPIC_NAME, SUBSCRIPTION_NAME
        )


class TestAzureServiceBusUpdateSubscriptionOperator:
    def test_init(self):
        """
        Test init by creating AzureServiceBusUpdateSubscriptionOperator with task id, subscription name,
        topic name and asserting with values
        """
        asb_update_subscription_operator = AzureServiceBusUpdateSubscriptionOperator(
            task_id="asb_update_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            max_delivery_count=10,
        )
        assert asb_update_subscription_operator.task_id == "asb_update_subscription"
        assert asb_update_subscription_operator.topic_name == TOPIC_NAME
        assert asb_update_subscription_operator.subscription_name == SUBSCRIPTION_NAME
        assert asb_update_subscription_operator.max_delivery_count == 10

    @mock.patch('azure.servicebus.management.SubscriptionProperties')
    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    def test_update_subscription(self, mock_get_conn, mock_subscription_properties):
        """
        Test AzureServiceBusUpdateSubscriptionOperator passed with the subscription name, topic name
        mocking the connection details, hook update_subscription function
        """
        mock_subscription_properties.name = SUBSCRIPTION_NAME
        mock_subscription_properties.max_delivery_count = 20
        mock_get_conn.return_value.__enter__.return_value.get_subscription.return_value = (
            mock_subscription_properties
        )
        asb_update_subscription = AzureServiceBusUpdateSubscriptionOperator(
            task_id="asb_update_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            max_delivery_count=20,
        )
        with mock.patch.object(asb_update_subscription.log, "info") as mock_log_info:
            asb_update_subscription.execute(None)
        mock_log_info.assert_called_with("Subscription Updated successfully %s", mock_subscription_properties)


class TestASBSubscriptionReceiveMessageOperator:
    def test_init(self):
        """
        Test init by creating ASBReceiveSubscriptionMessageOperator with task id, topic_name,
        subscription_name, batch and asserting with values
        """

        asb_subscription_receive_message = ASBReceiveSubscriptionMessageOperator(
            task_id="asb_subscription_receive_message",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            max_message_count=10,
        )
        assert asb_subscription_receive_message.task_id == "asb_subscription_receive_message"
        assert asb_subscription_receive_message.topic_name == TOPIC_NAME
        assert asb_subscription_receive_message.subscription_name == SUBSCRIPTION_NAME
        assert asb_subscription_receive_message.max_message_count == 10

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook.get_conn")
    def test_receive_message_queue(self, mock_get_conn):
        """
        Test ASBReceiveSubscriptionMessageOperator by mock connection, values
        and the service bus receive message
        """
        asb_subscription_receive_message = ASBReceiveSubscriptionMessageOperator(
            task_id="asb_subscription_receive_message",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            max_message_count=10,
        )
        asb_subscription_receive_message.execute(None)
        expected_calls = [
            mock.call()
            .__enter__()
            .get_subscription_receiver(SUBSCRIPTION_NAME, TOPIC_NAME)
            .__enter__()
            .receive_messages(max_message_count=10, max_wait_time=5)
            .get_subscription_receiver(SUBSCRIPTION_NAME, TOPIC_NAME)
            .__exit__()
            .mock_call()
            .__exit__
        ]
        mock_get_conn.assert_has_calls(expected_calls)


class TestASBTopicDeleteOperator:
    def test_init(self):
        """
        Test init by creating AzureServiceBusTopicDeleteOperator with task id, topic name and asserting
        with values
        """
        asb_delete_topic_operator = AzureServiceBusTopicDeleteOperator(
            task_id="asb_delete_topic",
            topic_name=TOPIC_NAME,
        )
        assert asb_delete_topic_operator.task_id == "asb_delete_topic"
        assert asb_delete_topic_operator.topic_name == TOPIC_NAME

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    @mock.patch('azure.servicebus.management.TopicProperties')
    def test_delete_topic(self, mock_topic_properties, mock_get_conn):
        """
        Test AzureServiceBusTopicDeleteOperator by mocking topic name, connection
        """
        asb_delete_topic = AzureServiceBusTopicDeleteOperator(
            task_id="asb_delete_topic",
            topic_name=TOPIC_NAME,
        )
        mock_topic_properties.name = TOPIC_NAME
        mock_get_conn.return_value.__enter__.return_value.get_topic.return_value = mock_topic_properties
        with mock.patch.object(asb_delete_topic.log, "info") as mock_log_info:
            asb_delete_topic.execute(None)
        mock_log_info.assert_called_with("Topic %s deleted.", TOPIC_NAME)

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb.AdminClientHook.get_conn")
    def test_delete_topic_not_exists(self, mock_get_conn):
        """
        Test AzureServiceBusTopicDeleteOperator by mocking topic name, connection
        """
        asb_delete_topic_not_exists = AzureServiceBusTopicDeleteOperator(
            task_id="asb_delete_topic_not_exists",
            topic_name=TOPIC_NAME,
        )
        mock_get_conn.return_value.__enter__.return_value.get_topic.return_value = None
        with mock.patch.object(asb_delete_topic_not_exists.log, "info") as mock_log_info:
            asb_delete_topic_not_exists.execute(None)
        mock_log_info.assert_called_with("Topic %s does not exist.", TOPIC_NAME)

    @mock.patch('airflow.providers.microsoft.azure.hooks.asb.AdminClientHook')
    def test_delete_topic_exception(self, mock_sb_admin_client):
        """
        Test `delete_topic` functionality to raise AirflowException,
         by passing topic name as None and pytest raise Airflow Exception
        """
        asb_delete_topic_exception = AzureServiceBusTopicDeleteOperator(
            task_id="delete_service_bus_subscription",
            topic_name=None,
        )
        with pytest.raises(TypeError):
            asb_delete_topic_exception.execute(None)
