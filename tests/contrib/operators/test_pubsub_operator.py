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

from __future__ import unicode_literals

from base64 import b64encode as b64e
import unittest

from airflow.contrib.operators.pubsub_operator import (
    PubSubTopicCreateOperator, PubSubTopicDeleteOperator,
    PubSubSubscriptionCreateOperator, PubSubSubscriptionDeleteOperator,
    PubSubPublishOperator)


try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'test-task-id'
TEST_PROJECT = 'test-project'
TEST_TOPIC = 'test-topic'
TEST_SUBSCRIPTION = 'test-subscription'
TEST_MESSAGES = [
    {
        'data': b64e(b'Hello, World!'),
        'attributes': {'type': 'greeting'}
    },
    {'data': b64e(b'Knock, knock')},
    {'attributes': {'foo': ''}}]
TEST_POKE_INTERVAl = 0


class PubSubTopicCreateOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_failifexists(self, mock_hook):
        operator = PubSubTopicCreateOperator(task_id=TASK_ID,
                                             project=TEST_PROJECT,
                                             topic=TEST_TOPIC,
                                             fail_if_exists=True)

        operator.execute(None)
        mock_hook.return_value.create_topic.assert_called_once_with(
            TEST_PROJECT, TEST_TOPIC, fail_if_exists=True)

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_succeedifexists(self, mock_hook):
        operator = PubSubTopicCreateOperator(task_id=TASK_ID,
                                             project=TEST_PROJECT,
                                             topic=TEST_TOPIC,
                                             fail_if_exists=False)

        operator.execute(None)
        mock_hook.return_value.create_topic.assert_called_once_with(
            TEST_PROJECT, TEST_TOPIC, fail_if_exists=False)


class PubSubTopicDeleteOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubTopicDeleteOperator(task_id=TASK_ID,
                                             project=TEST_PROJECT,
                                             topic=TEST_TOPIC)

        operator.execute(None)
        mock_hook.return_value.delete_topic.assert_called_once_with(
            TEST_PROJECT, TEST_TOPIC, fail_if_not_exists=False)


class PubSubSubscriptionCreateOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubSubscriptionCreateOperator(
            task_id=TASK_ID, topic_project=TEST_PROJECT, topic=TEST_TOPIC,
            subscription=TEST_SUBSCRIPTION)
        mock_hook.return_value.create_subscription.return_value = (
            TEST_SUBSCRIPTION)
        response = operator.execute(None)
        mock_hook.return_value.create_subscription.assert_called_once_with(
            TEST_PROJECT, TEST_TOPIC, TEST_SUBSCRIPTION, None,
            10, False)
        self.assertEquals(response, TEST_SUBSCRIPTION)

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_execute_different_project_ids(self, mock_hook):
        another_project = 'another-project'
        operator = PubSubSubscriptionCreateOperator(
            task_id=TASK_ID, topic_project=TEST_PROJECT, topic=TEST_TOPIC,
            subscription=TEST_SUBSCRIPTION,
            subscription_project=another_project)
        mock_hook.return_value.create_subscription.return_value = (
            TEST_SUBSCRIPTION)
        response = operator.execute(None)
        mock_hook.return_value.create_subscription.assert_called_once_with(
            TEST_PROJECT, TEST_TOPIC, TEST_SUBSCRIPTION, another_project,
            10, False)
        self.assertEquals(response, TEST_SUBSCRIPTION)

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_execute_no_subscription(self, mock_hook):
        operator = PubSubSubscriptionCreateOperator(
            task_id=TASK_ID, topic_project=TEST_PROJECT, topic=TEST_TOPIC)
        mock_hook.return_value.create_subscription.return_value = (
            TEST_SUBSCRIPTION)
        response = operator.execute(None)
        mock_hook.return_value.create_subscription.assert_called_once_with(
            TEST_PROJECT, TEST_TOPIC, None, None, 10, False)
        self.assertEquals(response, TEST_SUBSCRIPTION)


class PubSubSubscriptionDeleteOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubSubscriptionDeleteOperator(
            task_id=TASK_ID, project=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION)

        operator.execute(None)
        mock_hook.return_value.delete_subscription.assert_called_once_with(
            TEST_PROJECT, TEST_SUBSCRIPTION, fail_if_not_exists=False)


class PubSubPublishOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.pubsub_operator.PubSubHook')
    def test_publish(self, mock_hook):
        operator = PubSubPublishOperator(task_id=TASK_ID,
                                         project=TEST_PROJECT,
                                         topic=TEST_TOPIC,
                                         messages=TEST_MESSAGES)

        operator.execute(None)
        mock_hook.return_value.publish.assert_called_once_with(
            TEST_PROJECT, TEST_TOPIC, TEST_MESSAGES)
