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
from google.cloud.pubsub_v1.types import ReceivedMessage
from google.protobuf.json_format import MessageToDict, ParseDict

from airflow.exceptions import AirflowSensorTimeout
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

TASK_ID = 'test-task-id'
TEST_PROJECT = 'test-project'
TEST_SUBSCRIPTION = 'test-subscription'


class TestPubSubPullSensor(unittest.TestCase):
    def _generate_messages(self, count):
        return [
            ParseDict(
                {
                    "ack_id": "%s" % i,
                    "message": {
                        "data": 'Message {}'.format(i).encode('utf8'),
                        "attributes": {"type": "generated message"},
                    },
                },
                ReceivedMessage(),
            )
            for i in range(1, count + 1)
        ]

    def _generate_dicts(self, count):
        return [MessageToDict(m) for m in self._generate_messages(count)]

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_poke_no_messages(self, mock_hook):
        operator = PubSubPullSensor(task_id=TASK_ID, project_id=TEST_PROJECT,
                                    subscription=TEST_SUBSCRIPTION)
        mock_hook.return_value.pull.return_value = []
        self.assertEqual([], operator.poke(None))

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_poke_with_ack_messages(self, mock_hook):
        operator = PubSubPullSensor(task_id=TASK_ID, project_id=TEST_PROJECT,
                                    subscription=TEST_SUBSCRIPTION,
                                    ack_messages=True)
        generated_messages = self._generate_messages(5)
        generated_dicts = self._generate_dicts(5)
        mock_hook.return_value.pull.return_value = generated_messages
        self.assertEqual(generated_dicts, operator.poke(None))
        mock_hook.return_value.acknowledge.assert_called_once_with(
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            ack_ids=['1', '2', '3', '4', '5']
        )

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubPullSensor(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            poke_interval=0
        )
        generated_messages = self._generate_messages(5)
        generated_dicts = self._generate_dicts(5)
        mock_hook.return_value.pull.return_value = generated_messages
        response = operator.execute(None)
        mock_hook.return_value.pull.assert_called_once_with(
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            max_messages=5,
            return_immediately=False
        )
        self.assertEqual(generated_dicts, response)

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_execute_timeout(self, mock_hook):
        operator = PubSubPullSensor(task_id=TASK_ID, project_id=TEST_PROJECT,
                                    subscription=TEST_SUBSCRIPTION,
                                    poke_interval=0, timeout=1)
        mock_hook.return_value.pull.return_value = []
        with self.assertRaises(AirflowSensorTimeout):
            operator.execute(None)
            mock_hook.return_value.pull.assert_called_once_with(
                project_id=TEST_PROJECT,
                subscription=TEST_SUBSCRIPTION,
                max_messages=5,
                return_immediately=False
            )
