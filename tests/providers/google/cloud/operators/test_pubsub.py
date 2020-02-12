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

import unittest

import mock

from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator, PubSubCreateTopicOperator, PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator, PubSubPublishMessageOperator,
)

TASK_ID = 'test-task-id'
TEST_PROJECT = 'test-project'
TEST_TOPIC = 'test-topic'
TEST_SUBSCRIPTION = 'test-subscription'
TEST_MESSAGES = [
    {
        'data': b'Hello, World!',
        'attributes': {'type': 'greeting'}
    },
    {'data': b'Knock, knock'},
    {'attributes': {'foo': ''}}]
TEST_POKE_INTERVAl = 0


class TestPubSubTopicCreateOperator(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_failifexists(self, mock_hook):
        operator = PubSubCreateTopicOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_exists=True
        )

        operator.execute(None)
        mock_hook.return_value.create_topic.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_exists=True,
            labels=None,
            message_storage_policy=None,
            kms_key_name=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_succeedifexists(self, mock_hook):
        operator = PubSubCreateTopicOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_exists=False
        )

        operator.execute(None)
        mock_hook.return_value.create_topic.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_exists=False,
            labels=None,
            message_storage_policy=None,
            kms_key_name=None,
            retry=None,
            timeout=None,
            metadata=None
        )


class TestPubSubTopicDeleteOperator(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubDeleteTopicOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC
        )

        operator.execute(None)
        mock_hook.return_value.delete_topic.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_not_exists=False,
            retry=None,
            timeout=None,
            metadata=None
        )


class TestPubSubSubscriptionCreateOperator(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubCreateSubscriptionOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            subscription=TEST_SUBSCRIPTION
        )
        mock_hook.return_value.create_subscription.return_value = TEST_SUBSCRIPTION
        response = operator.execute(None)
        mock_hook.return_value.create_subscription.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            subscription=TEST_SUBSCRIPTION,
            subscription_project_id=None,
            ack_deadline_secs=10,
            fail_if_exists=False,
            push_config=None,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(response, TEST_SUBSCRIPTION)

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_execute_different_project_ids(self, mock_hook):
        another_project = 'another-project'
        operator = PubSubCreateSubscriptionOperator(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            subscription=TEST_SUBSCRIPTION,
            subscription_project_id=another_project,
            task_id=TASK_ID
        )
        mock_hook.return_value.create_subscription.return_value = TEST_SUBSCRIPTION
        response = operator.execute(None)
        mock_hook.return_value.create_subscription.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            subscription=TEST_SUBSCRIPTION,
            subscription_project_id=another_project,
            ack_deadline_secs=10,
            fail_if_exists=False,
            push_config=None,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            retry=None,
            timeout=None,
            metadata=None
        )
        self.assertEqual(response, TEST_SUBSCRIPTION)

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_execute_no_subscription(self, mock_hook):
        operator = PubSubCreateSubscriptionOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC
        )
        mock_hook.return_value.create_subscription.return_value = TEST_SUBSCRIPTION
        response = operator.execute(None)
        mock_hook.return_value.create_subscription.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            subscription=None,
            subscription_project_id=None,
            ack_deadline_secs=10,
            fail_if_exists=False,
            push_config=None,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(response, TEST_SUBSCRIPTION)


class TestPubSubSubscriptionDeleteOperator(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubDeleteSubscriptionOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION
        )

        operator.execute(None)
        mock_hook.return_value.delete_subscription.assert_called_once_with(
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            fail_if_not_exists=False,
            retry=None,
            timeout=None,
            metadata=None
        )


class TestPubSubPublishOperator(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
    def test_publish(self, mock_hook):
        operator = PubSubPublishMessageOperator(task_id=TASK_ID,
                                                project_id=TEST_PROJECT,
                                                topic=TEST_TOPIC,
                                                messages=TEST_MESSAGES)

        operator.execute(None)
        mock_hook.return_value.publish.assert_called_once_with(
            project_id=TEST_PROJECT, topic=TEST_TOPIC, messages=TEST_MESSAGES
        )
