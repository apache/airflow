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
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.triggers.base import BaseEventTrigger


class MockProvider:
    """Mock provider for testing."""

    def __init__(self, name: str, pattern: str):
        self.name = name
        self.pattern = pattern
        self.mock_trigger = MagicMock(spec=BaseEventTrigger)

    def queue_matches(self, queue: str) -> bool:
        return queue.startswith(self.pattern)

    def trigger_class(self):
        return type(self.mock_trigger)

    def trigger_kwargs(self, queue: str, **kwargs):
        return {"queue": queue}


PROVIDER_1_NAME = "SQSMessageQueueProvider"
PROVIDER_1_PATTERN = "sqs://"
PROVIDER_1_QUEUE = "sqs://test-queue"
PROVIDER_2_NAME = "KafkaMessageQueueProvider"
PROVIDER_2_PATTERN = "kafka://"
PROVIDER_2_QUEUE = "kafka://test-queue"

UNKNOWN_QUEUE = "unknown://queue"
UNSUPPORTED_QUEUE = "unsupported://queue"
TEST_QUEUE = "test://queue"

NO_PROVIDERS_ERROR = "No message queue providers are available"
INSTALL_PROVIDERS_MESSAGE = "Please ensure that you have the necessary providers installed"
NOT_RECOGNIZED_ERROR = "The queue is not recognized by any of the registered providers"
MULTIPLE_PROVIDERS_ERROR = "The queue '{queue}' is recognized by more than one provider"


MESSAGE_QUEUE_PROVIDERS_PATH = "airflow.providers.common.messaging.triggers.msg_queue.MESSAGE_QUEUE_PROVIDERS"


class TestMessageQueueTrigger:
    """Test cases for MessageQueueTrigger error handling and provider matching."""

    def test_no_providers_available(self):
        """Test error when no message queue providers are available."""
        trigger = MessageQueueTrigger(queue=TEST_QUEUE)
        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, []):
            with pytest.raises(ValueError, match=NO_PROVIDERS_ERROR):
                _ = trigger.trigger

    def test_queue_not_recognized_by_any_provider(self):
        """Test error when queue is not recognized by any provider."""
        # Create mock providers that don't match the queue
        provider1 = MockProvider(PROVIDER_1_NAME, PROVIDER_1_PATTERN)
        provider2 = MockProvider(PROVIDER_2_NAME, PROVIDER_2_PATTERN)

        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, [provider1, provider2]):
            trigger = MessageQueueTrigger(queue=UNKNOWN_QUEUE)

            with pytest.raises(ValueError, match=NOT_RECOGNIZED_ERROR):
                _ = trigger.trigger

    def test_queue_recognized_by_multiple_providers(self):
        """Test error when queue is recognized by multiple providers (collision)."""
        # Create mock providers that both match the same queue pattern
        provider1 = MockProvider(PROVIDER_1_NAME, PROVIDER_1_PATTERN)
        provider2 = MockProvider(PROVIDER_2_NAME, PROVIDER_1_PATTERN)

        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, [provider1, provider2]):
            trigger = MessageQueueTrigger(queue=PROVIDER_1_QUEUE)

            with pytest.raises(ValueError, match=MULTIPLE_PROVIDERS_ERROR.format(queue=PROVIDER_1_QUEUE)):
                _ = trigger.trigger

    def test_successful_provider_matching(self):
        """Test successful provider matching and trigger creation."""
        provider1 = MockProvider(PROVIDER_1_NAME, PROVIDER_1_PATTERN)
        provider2 = MockProvider(PROVIDER_2_NAME, PROVIDER_2_PATTERN)

        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, [provider1, provider2]):
            trigger = MessageQueueTrigger(queue=PROVIDER_1_QUEUE, extra_param="value")

            result_trigger = trigger.trigger

            assert result_trigger is not None

    def test_provider_class_names_in_logging(self):
        """Test that provider class names (not objects) are logged in error messages."""
        provider1 = MockProvider(PROVIDER_1_NAME, PROVIDER_1_PATTERN)
        provider2 = MockProvider(PROVIDER_2_NAME, PROVIDER_2_PATTERN)

        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, [provider1, provider2]):
            trigger = MessageQueueTrigger(queue=UNSUPPORTED_QUEUE)

            with pytest.raises(ValueError):
                _ = trigger.trigger

    def test_trigger_kwargs_passed_correctly(self):
        """Test that kwargs are passed correctly to the selected provider."""
        provider = MockProvider(PROVIDER_1_NAME, PROVIDER_1_PATTERN)

        mock_trigger_class = MagicMock()
        mock_trigger_instance = MagicMock(spec=BaseEventTrigger)
        mock_trigger_class.return_value = mock_trigger_instance

        provider.trigger_class = MagicMock(return_value=mock_trigger_class)
        provider.trigger_kwargs = MagicMock(return_value={"processed_queue": "test://processed"})

        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, [provider]):
            trigger = MessageQueueTrigger(queue=PROVIDER_1_QUEUE, param1="value1", param2="value2")

            result = trigger.trigger

            provider.trigger_kwargs.assert_called_once_with(
                PROVIDER_1_QUEUE, param1="value1", param2="value2"
            )

            # Verify trigger class was instantiated with combined kwargs
            mock_trigger_class.assert_called_once_with(
                processed_queue="test://processed", param1="value1", param2="value2"
            )

            assert result == mock_trigger_instance

    def test_serialize_delegates_to_underlying_trigger(self):
        """Test that serialize method delegates to the underlying trigger."""
        provider = MockProvider(PROVIDER_1_NAME, PROVIDER_1_PATTERN)

        mock_trigger_instance = MagicMock(spec=BaseEventTrigger)
        mock_trigger_instance.serialize.return_value = ("test.module.TestTrigger", {"param": "value"})

        mock_trigger_class = MagicMock(return_value=mock_trigger_instance)
        provider.trigger_class = MagicMock(return_value=mock_trigger_class)
        provider.trigger_kwargs = MagicMock(return_value={})

        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, [provider]):
            trigger = MessageQueueTrigger(queue=PROVIDER_1_QUEUE)

            result = trigger.serialize()

            mock_trigger_instance.serialize.assert_called_once()
            assert result == ("test.module.TestTrigger", {"param": "value"})

    @pytest.mark.asyncio
    async def test_run_delegates_to_underlying_trigger(self):
        """Test that run method delegates to the underlying trigger."""
        provider = MockProvider(PROVIDER_1_NAME, PROVIDER_1_PATTERN)

        mock_trigger_instance = MagicMock(spec=BaseEventTrigger)

        async def mock_run():
            yield MagicMock()

        mock_trigger_instance.run.return_value = mock_run()

        mock_trigger_class = MagicMock(return_value=mock_trigger_instance)
        provider.trigger_class = MagicMock(return_value=mock_trigger_class)
        provider.trigger_kwargs = MagicMock(return_value={})

        with mock.patch(MESSAGE_QUEUE_PROVIDERS_PATH, [provider]):
            trigger = MessageQueueTrigger(queue=PROVIDER_1_QUEUE)

            async_gen = trigger.run()
            event = await async_gen.__anext__()
            mock_trigger_instance.run.assert_called_once()
            assert event is not None


@mock.patch(
    "airflow.providers.common.messaging.triggers.msg_queue.MESSAGE_QUEUE_PROVIDERS",
    new_callable=mock.PropertyMock,
)
def test_provider_integrations(_):
    trigger = MessageQueueTrigger(queue="any queue")
    assert trigger is not None
