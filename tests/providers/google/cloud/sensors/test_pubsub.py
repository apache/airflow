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
from typing import Any, Dict, List

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
        operator = PubSubPullSensor(task_id=TASK_ID, project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION,)

        mock_hook.return_value.pull.return_value = []
        self.assertEqual(False, operator.poke({}))

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_poke_with_ack_messages(self, mock_hook):
        operator = PubSubPullSensor(
            task_id=TASK_ID, project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, ack_messages=True,
        )

        generated_messages = self._generate_messages(5)

        mock_hook.return_value.pull.return_value = generated_messages

        self.assertEqual(True, operator.poke({}))
        mock_hook.return_value.acknowledge.assert_called_once_with(
            project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, messages=generated_messages,
        )

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_execute(self, mock_hook):
        operator = PubSubPullSensor(
            task_id=TASK_ID, project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, poke_interval=0,
        )

        generated_messages = self._generate_messages(5)
        generated_dicts = self._generate_dicts(5)
        mock_hook.return_value.pull.return_value = generated_messages

        response = operator.execute({})
        mock_hook.return_value.pull.assert_called_once_with(
            project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, max_messages=5, return_immediately=True
        )
        self.assertEqual(generated_dicts, response)

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_execute_timeout(self, mock_hook):
        operator = PubSubPullSensor(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            poke_interval=0,
            timeout=1,
        )

        mock_hook.return_value.pull.return_value = []

        with self.assertRaises(AirflowSensorTimeout):
            operator.execute({})
            mock_hook.return_value.pull.assert_called_once_with(
                project_id=TEST_PROJECT,
                subscription=TEST_SUBSCRIPTION,
                max_messages=5,
                return_immediately=False,
            )

    @mock.patch('airflow.providers.google.cloud.sensors.pubsub.PubSubHook')
    def test_execute_with_messages_callback(self, mock_hook):
        generated_messages = self._generate_messages(5)
        messages_callback_return_value = 'asdfg'

        def messages_callback(
            pulled_messages: List[ReceivedMessage], context: Dict[str, Any],
        ):
            assert pulled_messages == generated_messages

            assert isinstance(context, dict)
            for key in context.keys():
                assert isinstance(key, str)

            return messages_callback_return_value

        messages_callback = mock.Mock(side_effect=messages_callback)

        operator = PubSubPullSensor(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            poke_interval=0,
            messages_callback=messages_callback,
        )

        mock_hook.return_value.pull.return_value = generated_messages

        response = operator.execute({})
        mock_hook.return_value.pull.assert_called_once_with(
            project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, max_messages=5, return_immediately=True
        )

        messages_callback.assert_called_once()

        assert response == messages_callback_return_value
