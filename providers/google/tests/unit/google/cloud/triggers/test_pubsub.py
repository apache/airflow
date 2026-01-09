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
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.pubsub_v1.types import ReceivedMessage

from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger
from airflow.triggers.base import TriggerEvent

TEST_POLL_INTERVAL = 10
TEST_GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "test_project_id"
MAX_MESSAGES = 5
ACK_MESSAGES = True


@pytest.fixture
def trigger():
    return PubsubPullTrigger(
        project_id=PROJECT_ID,
        subscription="subscription",
        max_messages=MAX_MESSAGES,
        ack_messages=ACK_MESSAGES,
        poke_interval=TEST_POLL_INTERVAL,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
    )


async def generate_messages(count: int) -> list[ReceivedMessage]:
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


class TestPubsubPullTrigger:
    def test_async_pubsub_pull_trigger_serialization_should_execute_successfully(self, trigger):
        """
        Asserts that the PubsubPullTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.pubsub.PubsubPullTrigger"
        assert kwargs == {
            "project_id": PROJECT_ID,
            "subscription": "subscription",
            "max_messages": MAX_MESSAGES,
            "ack_messages": ACK_MESSAGES,
            "poke_interval": TEST_POLL_INTERVAL,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": None,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.pubsub.PubSubAsyncHook.pull")
    async def test_async_pubsub_pull_trigger_return_event(self, mock_pull):
        mock_pull.return_value = generate_messages(1)
        trigger = PubsubPullTrigger(
            project_id=PROJECT_ID,
            subscription="subscription",
            max_messages=MAX_MESSAGES,
            ack_messages=False,
            poke_interval=TEST_POLL_INTERVAL,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=None,
        )

        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": [
                    {
                        "ack_id": "1",
                        "message": {
                            "data": "TWVzc2FnZSAx",
                            "attributes": {"type": "generated message"},
                            "message_id": "",
                            "ordering_key": "",
                        },
                        "delivery_attempt": 0,
                    }
                ],
            }
        )

        response = await trigger.run().asend(None)

        assert response == expected_event

    @mock.patch("airflow.providers.google.cloud.triggers.pubsub.PubSubAsyncHook")
    def test_hook(self, mock_async_hook):
        trigger = PubsubPullTrigger(
            project_id=PROJECT_ID,
            subscription="subscription",
            max_messages=MAX_MESSAGES,
            ack_messages=False,
            poke_interval=TEST_POLL_INTERVAL,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=None,
        )
        async_hook_actual = trigger.hook

        mock_async_hook.assert_called_once_with(
            gcp_conn_id=trigger.gcp_conn_id,
            impersonation_chain=trigger.impersonation_chain,
            project_id=trigger.project_id,
        )
        assert async_hook_actual == mock_async_hook.return_value

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.pubsub.PubSubAsyncHook.pull")
    async def test_async_pubsub_pull_trigger_exception_during_pull(self, mock_pull):
        """Test that exceptions during pull are propagated and not caught."""
        mock_pull.side_effect = GoogleAPICallError("Connection error")

        trigger = PubsubPullTrigger(
            project_id=PROJECT_ID,
            subscription="subscription",
            max_messages=MAX_MESSAGES,
            ack_messages=False,
            poke_interval=TEST_POLL_INTERVAL,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=None,
        )

        with pytest.raises(GoogleAPICallError, match="Connection error"):
            await trigger.run().asend(None)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.pubsub.PubSubAsyncHook.acknowledge")
    @mock.patch("airflow.providers.google.cloud.hooks.pubsub.PubSubAsyncHook.pull")
    async def test_async_pubsub_pull_trigger_exception_during_ack(self, mock_pull, mock_acknowledge):
        """Test that exceptions during message acknowledgement are propagated."""
        # Return a coroutine that can be awaited
        mock_pull.return_value = generate_messages(1)
        mock_acknowledge.side_effect = GoogleAPICallError("Acknowledgement failed")

        trigger = PubsubPullTrigger(
            project_id=PROJECT_ID,
            subscription="subscription",
            max_messages=MAX_MESSAGES,
            ack_messages=True,
            poke_interval=TEST_POLL_INTERVAL,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=None,
        )

        with pytest.raises(GoogleAPICallError, match="Acknowledgement failed"):
            await trigger.run().asend(None)
