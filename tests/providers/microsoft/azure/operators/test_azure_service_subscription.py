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

from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.azure_service_bus_subscription import (
    ASBCreateSubscriptionOperator,
    ASBDeleteSubscriptionOperator,
    ASBReceiveSubscriptionMessageOperator,
    ASBUpdateSubscriptionOperator,
)

OWNER_NAME = "airflow"
DAG_ID = "test_azure_service_bus_subscription"
TOPIC_NAME = "sb_mgmt_topic_test"
SUBSCRIPTION_NAME = "sb_mgmt_subscription"


class TestASBCreateSubscriptionOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating ASBCreateSubscriptionOperator with task id, subscription name, topic name and
         asserting with value
        """
        asb_create_subscription = ASBCreateSubscriptionOperator(
            task_id="asb_create_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
        )
        assert asb_create_subscription.task_id == "asb_create_subscription"
        assert asb_create_subscription.subscription_name == SUBSCRIPTION_NAME
        assert asb_create_subscription.topic_name == TOPIC_NAME

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.asb_admin_client.AzureServiceBusAdminClientHook.get_conn"
    )
    def test_create_subscription(self, mock_get_conn):
        """
        Test ASBCreateSubscriptionOperator passed with the subscription name, topic name
        mocking the connection details, hook create_subscription function
        """
        asb_create_subscription = ASBCreateSubscriptionOperator(
            task_id="create_service_bus_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            dag=self.dag,
        )
        asb_create_subscription.execute(None)
        mock_get_conn.return_value.__enter__.return_value.create_subscription.assert_called_once_with(
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            lock_duration=None,
            requires_session=None,
            default_message_time_to_live=None,
            dead_lettering_on_message_expiration=True,
            dead_lettering_on_filter_evaluation_exceptions=None,
            max_delivery_count=10,
            enable_batched_operations=True,
            forward_to=None,
            user_metadata=None,
            forward_dead_lettered_messages_to=None,
            auto_delete_on_idle=None,
        )


class TestASBDeleteSubscriptionOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating ASBDeleteSubscriptionOperator with task id, subscription name, topic name and
         asserting with values
        """
        asb_delete_subscription_operator = ASBDeleteSubscriptionOperator(
            task_id="asb_delete_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            dag=self.dag,
        )
        assert asb_delete_subscription_operator.task_id == "asb_delete_subscription"
        assert asb_delete_subscription_operator.topic_name == TOPIC_NAME
        assert asb_delete_subscription_operator.subscription_name == SUBSCRIPTION_NAME

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.asb_admin_client.AzureServiceBusAdminClientHook.get_conn"
    )
    def test_delete_subscription(self, mock_get_conn):
        """
        Test ASBDeleteSubscriptionOperator by mocking subscription name, topic name and
         connection and hook delete_subscription
        """
        asb_delete_subscription_operator = ASBDeleteSubscriptionOperator(
            task_id="asb_delete_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            dag=self.dag,
        )
        asb_delete_subscription_operator.execute(None)
        mock_get_conn.return_value.__enter__.return_value.delete_subscription.assert_called_once_with(
            TOPIC_NAME, SUBSCRIPTION_NAME
        )


class TestASBUpdateSubscriptionOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating ASBUpdateSubscriptionOperator with task id, subscription name, topic name and
         asserting with values
        """
        asb_update_subscription_operator = ASBUpdateSubscriptionOperator(
            task_id="asb_update_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            max_delivery_count=10,
            dag=self.dag,
        )
        assert asb_update_subscription_operator.task_id == "asb_update_subscription"
        assert asb_update_subscription_operator.topic_name == TOPIC_NAME
        assert asb_update_subscription_operator.subscription_name == SUBSCRIPTION_NAME
        assert asb_update_subscription_operator.max_delivery_count == 10

    @mock.patch('azure.servicebus.management.SubscriptionProperties')
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.asb_admin_client.AzureServiceBusAdminClientHook.get_conn"
    )
    def test_update_subscription(self, mock_get_conn, mock_subscription_properties):
        """
        Test ASBUpdateSubscriptionOperator passed with the subscription name, topic name
        mocking the connection details, hook update_subscription function
        """
        mock_subscription_properties.name = SUBSCRIPTION_NAME
        mock_subscription_properties.max_delivery_count = 20
        mock_get_conn.return_value.__enter__.return_value.get_subscription.return_value = (
            mock_subscription_properties
        )
        asb_update_subscription = ASBUpdateSubscriptionOperator(
            task_id="asb_update_subscription",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            max_delivery_count=20,
            dag=self.dag,
        )
        asb_update_subscription.execute(None)
        expected_calls = [
            mock.call().__enter__().update_subscription(TOPIC_NAME, mock_subscription_properties)
        ]
        mock_get_conn.assert_has_calls(expected_calls)


class TestASBSubscriptionReceiveMessageOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': OWNER_NAME, 'start_date': datetime.datetime(2017, 1, 1)}
        self.dag = DAG(DAG_ID, default_args=args)

    def test_init(self):
        """
        Test init by creating AzureServiceBusReceiveMessageOperator with task id, queue_name, message,
        batch and asserting with values
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

    @mock.patch("airflow.providers.microsoft.azure.hooks.asb_message.ServiceBusMessageHook.get_conn")
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
            .get_queue_receiver(self.queue_name)
            .__exit__()
            .mock_call()
            .__exit__
        ]
        mock_get_conn.assert_has_calls(expected_calls)
