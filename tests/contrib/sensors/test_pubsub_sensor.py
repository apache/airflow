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

import unittest

from base64 import b64encode as b64e

from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from airflow.exceptions import AirflowSensorTimeout
from tests.compat import mock

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


class PubSubPullSensorTest(unittest.TestCase):

    def _generate_messages(self, count):
        messages = []
        for i in range(1, count + 1):
            messages.append({
                'ackId': '%s' % i,
                'message': {
                    'data': b64e('Message {}'.format(i).encode('utf8')),
                    'attributes': {'type': 'generated message'}
                }
            })
        return messages

    @mock.patch('airflow.contrib.sensors.pubsub_sensor.PubSubHook')
    def test_poke_no_messages(self, mock_hook):
        operator = PubSubPullSensor(task_id=TASK_ID, project=TEST_PROJECT,
                                    subscription=TEST_SUBSCRIPTION)
        mock_hook.return_value.pull.return_value = []
        self.assertEqual([], operator.poke(None))

    @mock.patch('airflow.contrib.sensors.pubsub_sensor.PubSubHook')
    def test_poke_with_ack_messages(self, mock_hook):
        operator = PubSubPullSensor(task_id=TASK_ID, project=TEST_PROJECT,
                                    subscription=TEST_SUBSCRIPTION,
                                    ack_messages=True)
        generated_messages = self._generate_messages(5)
        mock_hook.return_value.pull.return_value = generated_messages
        self.assertEqual(generated_messages, operator.poke(None))
        mock_hook.return_value.acknowledge.assert_called_with(
            TEST_PROJECT, TEST_SUBSCRIPTION, ['1', '2', '3', '4', '5']
        )

    @mock.patch('airflow.contrib.sensors.pubsub_sensor.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubPullSensor(task_id=TASK_ID, project=TEST_PROJECT,
                                    subscription=TEST_SUBSCRIPTION,
                                    poke_interval=0)
        generated_messages = self._generate_messages(5)
        mock_hook.return_value.pull.return_value = generated_messages
        response = operator.execute(None)
        mock_hook.return_value.pull.assert_called_with(
            TEST_PROJECT, TEST_SUBSCRIPTION, 5, False)
        self.assertEqual(response, generated_messages)

    @mock.patch('airflow.contrib.sensors.pubsub_sensor.PubSubHook')
    def test_execute_timeout(self, mock_hook):
        operator = PubSubPullSensor(task_id=TASK_ID, project=TEST_PROJECT,
                                    subscription=TEST_SUBSCRIPTION,
                                    poke_interval=0, timeout=1)
        mock_hook.return_value.pull.return_value = []
        with self.assertRaises(AirflowSensorTimeout):
            operator.execute(None)
            mock_hook.return_value.pull.assert_called_with(
                TEST_PROJECT, TEST_SUBSCRIPTION, 5, False)
