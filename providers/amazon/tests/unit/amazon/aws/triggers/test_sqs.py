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

from unittest.mock import AsyncMock

import pytest

from tests_common.test_utils.common_msg_queue import (
    collect_queue_param_deprecation_warning,
    mark_common_msg_queue_test,
)

USED_FIXTURES = [collect_queue_param_deprecation_warning]

TEST_SQS_QUEUE = "test-sqs-queue"
TEST_AWS_CONN_ID = "test-aws-conn-id"
TEST_MAX_MESSAGES = 1
TEST_NUM_BATCHES = 1
TEST_WAIT_TIME_SECONDS = 1
TEST_VISIBILITY_TIMEOUT = 1
TEST_MESSAGE_FILTERING_MATCH_VALUES = "test"
TEST_MESSAGE_FILTERING_CONFIG = "test-message-filtering-config"
TEST_DELETE_MESSAGE_ON_RECEPTION = False
TEST_WAITER_DELAY = 1
TEST_REGION_NAME = "eu-central-1"
TEST_VERIFY = True
TEST_BOTOCORE_CONFIG = {"region_name": "us-east-1"}


class TestSqsTriggers:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self, cleanup_providers_manager):
        from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger

        self.sqs_trigger = SqsSensorTrigger(
            sqs_queue=TEST_SQS_QUEUE,
            aws_conn_id=TEST_AWS_CONN_ID,
            max_messages=TEST_MAX_MESSAGES,
            num_batches=TEST_NUM_BATCHES,
            wait_time_seconds=TEST_WAIT_TIME_SECONDS,
            visibility_timeout=TEST_VISIBILITY_TIMEOUT,
            message_filtering="literal",
            message_filtering_match_values=TEST_MESSAGE_FILTERING_MATCH_VALUES,
            message_filtering_config=TEST_MESSAGE_FILTERING_CONFIG,
            delete_message_on_reception=TEST_DELETE_MESSAGE_ON_RECEPTION,
            waiter_delay=TEST_WAITER_DELAY,
            region_name=TEST_REGION_NAME,
            verify=TEST_VERIFY,
            botocore_config=TEST_BOTOCORE_CONFIG,
        )

    @pytest.mark.asyncio
    async def test_poke(self):
        mock_client = AsyncMock()
        message = {
            "MessageId": "test_message_id",
            "Body": "test",
        }
        mock_response = {
            "Messages": [message],
        }
        mock_client.receive_message.return_value = mock_response
        messages = await self.sqs_trigger.poke(client=mock_client)
        assert messages[0] == message

    @pytest.mark.asyncio
    async def test_poke_filtered_message(self):
        mock_client = AsyncMock()
        message = {
            "MessageId": "test_message_id",
            "Body": "This will be filtered out",
        }
        mock_response = {
            "Messages": [message],
        }
        mock_client.receive_message.return_value = mock_response
        messages = await self.sqs_trigger.poke(client=mock_client)
        assert len(messages) == 0

    @pytest.mark.asyncio
    async def test_poke_no_messages(self):
        mock_client = AsyncMock()
        mock_response = {"Messages": []}
        mock_client.receive_message.return_value = mock_response
        messages = await self.sqs_trigger.poke(client=mock_client)
        assert len(messages) == 0


@mark_common_msg_queue_test
class TestMessageQueueTrigger:
    @pytest.mark.usefixtures("collect_queue_param_deprecation_warning")
    def test_provider_integrations_with_queue_param(self, cleanup_providers_manager):
        queue = "https://sqs.us-east-1.amazonaws.com/0123456789/Test"
        from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        trigger = MessageQueueTrigger(queue=queue)
        assert isinstance(trigger.trigger, SqsSensorTrigger)

    def test_provider_integrations_with_scheme_param(self, cleanup_providers_manager):
        from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        trigger = MessageQueueTrigger(
            scheme="sqs", sqs_queue="https://sqs.us-east-1.amazonaws.com/0123456789/Test"
        )
        assert isinstance(trigger.trigger, SqsSensorTrigger)
