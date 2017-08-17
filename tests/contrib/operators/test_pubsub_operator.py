# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from base64 import b64encode as b64e
import unittest

from airflow.contrib.operators.pubsub_operator import PubSubPublishOperator
from airflow.contrib.operators.pubsub_operator import PubSubTopicCreateOperator

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
TEST_MESSAGES = [
    {
        'data': b64e('Hello, World!'),
        'attributes': {'type': 'greeting'}
    },
    {'data': b64e('Knock, knock')},
    {'attributes': {'foo': ''}}]


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
