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
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.pubsub_v1.types import ReceivedMessage

from airflow.exceptions import TaskDeferred
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
    PubSubPublishMessageOperator,
    PubSubPullOperator,
)

TASK_ID = "test-task-id"
TEST_PROJECT = "test-project"
TEST_TOPIC = "test-topic"
TEST_SUBSCRIPTION = "test-subscription"
TEST_MESSAGES = [
    {"data": b"Hello, World!", "attributes": {"type": "greeting"}},
    {"data": b"Knock, knock"},
    {"attributes": {"foo": ""}},
]
TEST_MESSAGES_ORDERING_KEY = [
    {"data": b"Hello, World!", "attributes": {"ordering_key": "key"}},
]


class TestPubSubTopicCreateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_failifexists(self, mock_hook):
        operator = PubSubCreateTopicOperator(
            task_id=TASK_ID, project_id=TEST_PROJECT, topic=TEST_TOPIC, fail_if_exists=True
        )

        context = mock.MagicMock()
        operator.execute(context=context)
        mock_hook.return_value.create_topic.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_exists=True,
            labels=None,
            message_storage_policy=None,
            kms_key_name=None,
            schema_settings=None,
            message_retention_duration=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_succeedifexists(self, mock_hook):
        operator = PubSubCreateTopicOperator(
            task_id=TASK_ID, project_id=TEST_PROJECT, topic=TEST_TOPIC, fail_if_exists=False
        )

        context = mock.MagicMock()
        operator.execute(context=context)
        mock_hook.return_value.create_topic.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_exists=False,
            labels=None,
            message_storage_policy=None,
            kms_key_name=None,
            schema_settings=None,
            message_retention_duration=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestPubSubTopicDeleteOperator:
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute(self, mock_hook):
        operator = PubSubDeleteTopicOperator(task_id=TASK_ID, project_id=TEST_PROJECT, topic=TEST_TOPIC)

        operator.execute(None)
        mock_hook.return_value.delete_topic.assert_called_once_with(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            fail_if_not_exists=False,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestPubSubSubscriptionCreateOperator:
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute(self, mock_hook):
        operator = PubSubCreateSubscriptionOperator(
            task_id=TASK_ID, project_id=TEST_PROJECT, topic=TEST_TOPIC, subscription=TEST_SUBSCRIPTION
        )
        mock_hook.return_value.create_subscription.return_value = TEST_SUBSCRIPTION
        context = mock.MagicMock()
        response = operator.execute(context=context)
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
            enable_message_ordering=False,
            expiration_policy=None,
            filter_=None,
            dead_letter_policy=None,
            retry_policy=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert response == TEST_SUBSCRIPTION

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute_different_project_ids(self, mock_hook):
        another_project = "another-project"
        operator = PubSubCreateSubscriptionOperator(
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            subscription=TEST_SUBSCRIPTION,
            subscription_project_id=another_project,
            task_id=TASK_ID,
        )
        mock_hook.return_value.create_subscription.return_value = TEST_SUBSCRIPTION
        context = mock.MagicMock()
        response = operator.execute(context=context)
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
            enable_message_ordering=False,
            expiration_policy=None,
            filter_=None,
            dead_letter_policy=None,
            retry_policy=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert response == TEST_SUBSCRIPTION

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute_no_subscription(self, mock_hook):
        operator = PubSubCreateSubscriptionOperator(
            task_id=TASK_ID, project_id=TEST_PROJECT, topic=TEST_TOPIC
        )
        mock_hook.return_value.create_subscription.return_value = TEST_SUBSCRIPTION
        context = mock.MagicMock()
        response = operator.execute(context=context)
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
            enable_message_ordering=False,
            expiration_policy=None,
            filter_=None,
            dead_letter_policy=None,
            retry_policy=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert response == TEST_SUBSCRIPTION

    @pytest.mark.parametrize(
        ("project_id", "subscription", "subscription_project_id", "expected_input", "expected_output"),
        [
            (
                TEST_PROJECT,
                TEST_SUBSCRIPTION,
                None,
                f"topic:{TEST_PROJECT}:{TEST_TOPIC}",
                f"subscription:{TEST_PROJECT}:{TEST_SUBSCRIPTION}",
            ),
            (
                TEST_PROJECT,
                TEST_SUBSCRIPTION,
                "another-project",
                f"topic:{TEST_PROJECT}:{TEST_TOPIC}",
                f"subscription:another-project:{TEST_SUBSCRIPTION}",
            ),
            (
                TEST_PROJECT,
                None,
                None,
                f"topic:{TEST_PROJECT}:{TEST_TOPIC}",
                f"subscription:{TEST_PROJECT}:generated",
            ),
            (
                TEST_PROJECT,
                None,
                "another-project",
                f"topic:{TEST_PROJECT}:{TEST_TOPIC}",
                "subscription:another-project:generated",
            ),
            (
                None,
                None,
                None,
                f"topic:connection-project:{TEST_TOPIC}",
                "subscription:connection-project:generated",
            ),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_get_openlineage_facets(
        self,
        mock_hook,
        project_id,
        subscription,
        subscription_project_id,
        expected_input,
        expected_output,
    ):
        operator = PubSubCreateSubscriptionOperator(
            task_id=TASK_ID,
            project_id=project_id,
            topic=TEST_TOPIC,
            subscription=subscription,
            subscription_project_id=subscription_project_id,
        )
        mock_hook.return_value.create_subscription.return_value = subscription or "generated"
        mock_hook.return_value.project_id = project_id or "connection-project"
        context = mock.MagicMock()
        response = operator.execute(context=context)
        mock_hook.return_value.create_subscription.assert_called_once_with(
            project_id=project_id,
            topic=TEST_TOPIC,
            subscription=subscription,
            subscription_project_id=subscription_project_id,
            ack_deadline_secs=10,
            fail_if_exists=False,
            push_config=None,
            retain_acked_messages=None,
            message_retention_duration=None,
            labels=None,
            enable_message_ordering=False,
            expiration_policy=None,
            filter_=None,
            dead_letter_policy=None,
            retry_policy=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

        if subscription:
            assert response == TEST_SUBSCRIPTION
        else:
            assert response == "generated"

        result = operator.get_openlineage_facets_on_complete(operator)
        assert not result.run_facets
        assert not result.job_facets
        assert len(result.inputs) == 1
        assert result.inputs[0].namespace == "pubsub"
        assert result.inputs[0].name == expected_input
        assert len(result.outputs) == 1
        assert result.outputs[0].namespace == "pubsub"
        assert result.outputs[0].name == expected_output


class TestPubSubSubscriptionDeleteOperator:
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute(self, mock_hook):
        operator = PubSubDeleteSubscriptionOperator(
            task_id=TASK_ID, project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION
        )

        operator.execute(None)
        mock_hook.return_value.delete_subscription.assert_called_once_with(
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            fail_if_not_exists=False,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestPubSubPublishOperator:
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_publish(self, mock_hook):
        operator = PubSubPublishMessageOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            messages=TEST_MESSAGES,
        )

        operator.execute(None)
        mock_hook.return_value.publish.assert_called_once_with(
            project_id=TEST_PROJECT, topic=TEST_TOPIC, messages=TEST_MESSAGES
        )

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_publish_with_ordering_key(self, mock_hook):
        operator = PubSubPublishMessageOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            messages=TEST_MESSAGES_ORDERING_KEY,
            enable_message_ordering=True,
        )

        operator.execute(None)
        mock_hook.return_value.publish.assert_called_once_with(
            project_id=TEST_PROJECT, topic=TEST_TOPIC, messages=TEST_MESSAGES_ORDERING_KEY
        )

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_publish_with_open_telemetry_tracing(self, mock_hook):
        operator = PubSubPublishMessageOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            messages=TEST_MESSAGES,
            enable_open_telemetry_tracing=True,
        )

        operator.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
            enable_message_ordering=False,
            enable_open_telemetry_tracing=True,
        )
        mock_hook.return_value.publish.assert_called_once_with(
            project_id=TEST_PROJECT, topic=TEST_TOPIC, messages=TEST_MESSAGES
        )

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_publish_with_ordering_and_tracing(self, mock_hook):
        operator = PubSubPublishMessageOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            topic=TEST_TOPIC,
            messages=TEST_MESSAGES_ORDERING_KEY,
            enable_message_ordering=True,
            enable_open_telemetry_tracing=True,
        )

        operator.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
            enable_message_ordering=True,
            enable_open_telemetry_tracing=True,
        )
        mock_hook.return_value.publish.assert_called_once_with(
            project_id=TEST_PROJECT, topic=TEST_TOPIC, messages=TEST_MESSAGES_ORDERING_KEY
        )

    @pytest.mark.parametrize(
        ("project_id", "expected_dataset"),
        [
            # 1. project_id provided
            (TEST_PROJECT, f"topic:{TEST_PROJECT}:{TEST_TOPIC}"),
            # 2. project_id not provided (use project_id from connection)
            (None, f"topic:connection-project:{TEST_TOPIC}"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_get_openlineage_facets(self, mock_hook, project_id, expected_dataset):
        operator = PubSubPublishMessageOperator(
            task_id=TASK_ID,
            project_id=project_id,
            topic=TEST_TOPIC,
            messages=TEST_MESSAGES,
        )

        operator.execute(None)
        mock_hook.return_value.publish.assert_called_once_with(
            project_id=project_id, topic=TEST_TOPIC, messages=TEST_MESSAGES
        )
        mock_hook.return_value.project_id = project_id or "connection-project"

        result = operator.get_openlineage_facets_on_complete(operator)
        assert not result.run_facets
        assert not result.job_facets
        assert len(result.inputs) == 0
        assert len(result.outputs) == 1
        assert result.outputs[0].namespace == "pubsub"
        assert result.outputs[0].name == expected_dataset


class TestPubSubPullOperator:
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

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute_no_messages(self, mock_hook):
        operator = PubSubPullOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
        )

        mock_hook.return_value.pull.return_value = []
        assert operator.execute({}) == []

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute_with_ack_messages(self, mock_hook):
        operator = PubSubPullOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            ack_messages=True,
        )

        generated_messages = self._generate_messages(5)
        generated_dicts = self._generate_dicts(5)
        mock_hook.return_value.pull.return_value = generated_messages

        assert generated_dicts == operator.execute({})
        mock_hook.return_value.acknowledge.assert_called_once_with(
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            messages=generated_messages,
        )

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
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

        operator = PubSubPullOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            messages_callback=messages_callback,
        )

        mock_hook.return_value.pull.return_value = generated_messages

        response = operator.execute({})
        mock_hook.return_value.pull.assert_called_once_with(
            project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, max_messages=5, return_immediately=True
        )

        messages_callback.assert_called_once()

        assert response == messages_callback_return_value

    @pytest.mark.db_test
    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_execute_deferred(self, mock_hook, create_task_instance_of_operator):
        """
        Asserts that a task is deferred and a PubSubPullOperator will be fired
        when the PubSubPullOperator is executed with deferrable=True.
        """
        ti = create_task_instance_of_operator(
            PubSubPullOperator,
            dag_id="dag_id",
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as _:
            ti.task.execute(mock.MagicMock())

    @mock.patch("airflow.providers.google.cloud.operators.pubsub.PubSubHook")
    def test_get_openlineage_facets(self, mock_hook):
        operator = PubSubPullOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            subscription=TEST_SUBSCRIPTION,
        )

        generated_messages = self._generate_messages(5)
        generated_dicts = self._generate_dicts(5)
        mock_hook.return_value.pull.return_value = generated_messages

        assert generated_dicts == operator.execute({})
        mock_hook.return_value.pull.assert_called_once_with(
            project_id=TEST_PROJECT, subscription=TEST_SUBSCRIPTION, max_messages=5, return_immediately=True
        )

        result = operator.get_openlineage_facets_on_complete(operator)
        assert not result.run_facets
        assert not result.job_facets
        assert len(result.inputs) == 0
        assert len(result.outputs) == 1
        assert result.outputs[0].namespace == "pubsub"
        assert result.outputs[0].name == f"subscription:{TEST_PROJECT}:{TEST_SUBSCRIPTION}"
