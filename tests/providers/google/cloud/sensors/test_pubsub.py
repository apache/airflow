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
from __future__ import annotations

from typing import Any
from unittest import mock

import pytest
from google.cloud.pubsub_v1.types import ReceivedMessage

from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException, TaskDeferred
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger

TASK_ID = "test-task-id"
TEST_PROJECT = "test-project"
TEST_SUBSCRIPTION = "test-subscription"


class TestPubSubPullSensor:
    def _generate_messages(self, count):
        return [
            ReceivedMessage(
                ack_id=f"{i}",
                message={
                    "data": f"Message {i}".encode(),
                    "attributes": {"type": "generated message"},
                },
            )
            for i in range(1, count + 1)
        ]

    def _generate_dicts(self, count):
        return [ReceivedMessage.to_dict(m) for m in self._generate_messages(count)]

    @mock.patch("airflow.providers.google.cloud.sensors.pubsub.PubSubHook")
    def test_poke_no_messages(self, mock_hook):
        operator = PubSubPullSensor(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
        )

        mock_hook.return_value.pull.return_value = []
        assert operator.poke({}) is False

    @mock.patch("airflow.providers.google.cloud.sensors.pubsub.PubSubHook")
    def test_poke_with_ack_messages(self, mock_hook):
        operator = PubSubPullSensor(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            ack_messages=True,
        )

        generated_messages = self._generate_messages(5)

        mock_hook.return_value.pull.return_value = generated_messages

        assert operator.poke({}) is True
        mock_hook.return_value.acknowledge.assert_called_once_with(
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            messages=generated_messages,
        )

    @mock.patch("airflow.providers.google.cloud.sensors.pubsub.PubSubHook")
    def test_execute(self, mock_hook):
        operator = PubSubPullSensor(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            poke_interval=0,
        )

        generated_messages = self._generate_messages(5)
        generated_dicts = self._generate_dicts(5)
        mock_hook.return_value.pull.return_value = generated_messages

        response = operator.execute({})
        mock_hook.return_value.pull.assert_called_once_with(
            project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, max_messages=5, return_immediately=True
        )
        assert generated_dicts == response

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowSensorTimeout), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.google.cloud.sensors.pubsub.PubSubHook")
    def test_execute_timeout(self, mock_hook, soft_fail, expected_exception):
        operator = PubSubPullSensor(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            poke_interval=0,
            timeout=1,
            soft_fail=soft_fail,
        )

        mock_hook.return_value.pull.return_value = []

        with pytest.raises(expected_exception):
            operator.execute({})
            mock_hook.return_value.pull.assert_called_once_with(
                project_id=TEST_PROJECT,
                subscription=TEST_SUBSCRIPTION,
                max_messages=5,
                return_immediately=False,
            )

    @mock.patch("airflow.providers.google.cloud.sensors.pubsub.PubSubHook")
    def test_execute_with_messages_callback(self, mock_hook):
        generated_messages = self._generate_messages(5)
        messages_callback_return_value = "asdfg"

        def messages_callback(
            pulled_messages: list[ReceivedMessage],
            context: dict[str, Any],
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

    def test_pubsub_pull_sensor_async(self):
        """
        Asserts that a task is deferred and a PubsubPullTrigger will be fired
        when the PubSubPullSensor is executed.
        """
        task = PubSubPullSensor(
            task_id="test_task_id",
            ack_messages=True,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context={})
        assert isinstance(exc.value.trigger, PubsubPullTrigger), "Trigger is not a PubsubPullTrigger"

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    def test_pubsub_pull_sensor_async_execute_should_throw_exception(self, soft_fail, expected_exception):
        """Tests that an AirflowException is raised in case of error event"""

        operator = PubSubPullSensor(
            task_id="test_task",
            ack_messages=True,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            deferrable=True,
            soft_fail=soft_fail,
        )

        with pytest.raises(expected_exception):
            operator.execute_complete(
                context=mock.MagicMock(), event={"status": "error", "message": "test failure message"}
            )

    def test_pubsub_pull_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""
        operator = PubSubPullSensor(
            task_id="test_task",
            ack_messages=True,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            deferrable=True,
        )

        test_message = "test"
        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context={}, event={"status": "success", "message": test_message})
        mock_log_info.assert_called_with("Sensor pulls messages: %s", test_message)
