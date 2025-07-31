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

import asyncio
from unittest.mock import Mock, patch

import pytest

from airflow.providers.microsoft.azure.triggers.message_bus import (
    AzureServiceBusQueueTrigger,
    AzureServiceBusSubscriptionTrigger,
    BaseAzureServiceBusTrigger,
)
from airflow.triggers.base import TriggerEvent
from airflow.utils.context import Context


class TestBaseAzureServiceBusTrigger:
    """Test the base trigger functionality."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        trigger = BaseAzureServiceBusTrigger()

        assert trigger.max_message_count == 1
        assert trigger.max_wait_time is None
        assert trigger.poll_interval == 60
        assert trigger._queue is not None

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        context = Context()
        trigger = BaseAzureServiceBusTrigger(
            context=context,
            poll_interval=30,
            azure_service_bus_conn_id="custom_conn",
            max_message_count=5,
            max_wait_time=120,
        )

        assert trigger.context == context
        assert trigger.poll_interval == 30
        assert trigger.max_message_count == 5
        assert trigger.max_wait_time == 120

    @patch("airflow.providers.microsoft.azure.hooks.asb.ServiceBusMessage")
    def test_create_message_callback(self, mock_message):
        """Test message callback creation and functionality."""
        trigger = BaseAzureServiceBusTrigger()
        callback = trigger.create_message_callback()

        # Mock message
        mock_message.body = "test message body"
        context = Context()

        # This should not raise an exception
        callback(mock_message, context)

        assert callable(callback)

    @pytest.mark.asyncio
    async def test_callback_listener_timeout(self):
        """Test callback listener with timeout."""
        trigger = BaseAzureServiceBusTrigger(max_wait_time=0.1)

        with pytest.raises(asyncio.TimeoutError):
            async for _ in trigger.callback_listener():
                pass


class TestAzureServiceBusQueueTrigger:
    """Test the queue trigger functionality."""

    def test_init(self):
        """Test queue trigger initialization."""
        queues = ["queue1", "queue2"]
        trigger = AzureServiceBusQueueTrigger(
            queues=queues,
            azure_service_bus_conn_id="test_conn",
            max_message_count=10,
        )

        assert trigger.queues == queues
        assert trigger.max_message_count == 10

    def test_serialize(self):
        """Test serialization of queue trigger."""
        queues = ["queue1", "queue2"]
        trigger = AzureServiceBusQueueTrigger(
            queues=queues,
            azure_service_bus_conn_id="test_conn",
        )

        serialized = trigger.serialize()

        assert serialized["class_name"] == "AzureServiceBusQueueTrigger"
        assert serialized["config"]["queues"] == queues
        assert "azure_service_bus_conn_id" in serialized["config"]

    @patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook")
    def test_trigger_queue_listening(self, mock_hook):
        """Test queue listening task creation."""
        trigger = AzureServiceBusQueueTrigger(queues=["test_queue"])
        mock_receive_func = Mock()

        task = trigger.trigger_queue_listening("test_queue", mock_receive_func)

        assert asyncio.iscoroutine(task)
        task.close()  # Clean up the coroutine

    @pytest.mark.asyncio
    @patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook")
    @patch("asgiref.sync.sync_to_async")
    async def test_run_single_queue(self, mock_sync_to_async, mock_hook):
        """Test running single queue monitoring."""
        trigger = AzureServiceBusQueueTrigger(
            queues=["test_queue"],
            poll_interval=0.01,  # Very short for testing
        )

        # Mock the async receive function
        mock_receive_async = Mock()
        mock_sync_to_async.return_value = mock_receive_async

        # Create a task and cancel it quickly to avoid infinite loop
        task = asyncio.create_task(trigger.run_single("test_queue"))
        await asyncio.sleep(0.02)  # Let it run briefly
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass  # Expected since we cancelled the task

    @pytest.mark.asyncio
    @patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook")
    async def test_run_with_message(self, mock_hook):
        """Test the main run method with a mock message."""
        trigger = AzureServiceBusQueueTrigger(queues=["test_queue"])

        # Put a test message in the queue
        await trigger._queue.put("test message")

        # Get one event from the generator
        events = []
        async for event in trigger.run():
            events.append(event)
            if len(events) >= 1:
                break

        assert len(events) == 1
        assert isinstance(events[0], TriggerEvent)
        assert events[0].payload == "test message"


class TestAzureServiceBusSubscriptionTrigger:
    """Test the subscription trigger functionality."""

    def test_init(self):
        """Test subscription trigger initialization."""
        topics = ["topic1", "topic2"]
        subscription = "test-subscription"
        trigger = AzureServiceBusSubscriptionTrigger(
            topics=topics,
            subscription_name=subscription,
            azure_service_bus_conn_id="test_conn",
        )

        assert trigger.topics == topics
        assert trigger.subscription_name == subscription

    def test_serialize(self):
        """Test serialization of subscription trigger."""
        topics = ["topic1", "topic2"]
        subscription = "test-subscription"
        trigger = AzureServiceBusSubscriptionTrigger(
            topics=topics,
            subscription_name=subscription,
            azure_service_bus_conn_id="test_conn",
        )

        serialized = trigger.serialize()

        assert serialized["class_name"] == "AzureServiceBusSubscriptionTrigger"
        assert serialized["config"]["topics"] == topics
        assert serialized["config"]["subscription_name"] == subscription

    @patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook")
    def test_trigger_topic_listening(self, mock_hook):
        """Test topic listening task creation."""
        trigger = AzureServiceBusSubscriptionTrigger(topics=["test_topic"], subscription_name="test-sub")
        mock_receive_func = Mock()

        task = trigger.trigger_topic_listening("test_topic", mock_receive_func)

        assert asyncio.iscoroutine(task)
        task.close()  # Clean up the coroutine

    @pytest.mark.asyncio
    @patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook")
    @patch("asgiref.sync.sync_to_async")
    async def test_run_single_topic(self, mock_sync_to_async, mock_hook):
        """Test running single topic monitoring."""
        trigger = AzureServiceBusSubscriptionTrigger(
            topics=["test_topic"],
            subscription_name="test-sub",
            poll_interval=0.01,  # Very short for testing
        )

        # Mock the async receive function
        mock_receive_async = Mock()
        mock_sync_to_async.return_value = mock_receive_async

        # Create a task and cancel it quickly to avoid infinite loop
        task = asyncio.create_task(trigger.run_single("test_topic"))
        await asyncio.sleep(0.02)  # Let it run briefly
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass  # Expected since we cancelled the task

    @pytest.mark.asyncio
    @patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook")
    async def test_run_subscription_with_message(self, mock_hook):
        """Test the main run method with a mock message."""
        trigger = AzureServiceBusSubscriptionTrigger(topics=["test_topic"], subscription_name="test-sub")

        # Put a test message in the queue
        await trigger._queue.put("subscription test message")

        # Get one event from the generator
        events = []
        async for event in trigger.run():
            events.append(event)
            if len(events) >= 1:
                break

        assert len(events) == 1
        assert isinstance(events[0], TriggerEvent)
        assert events[0].payload == "subscription test message"


class TestIntegrationScenarios:
    """Test integration scenarios and edge cases."""

    @pytest.mark.asyncio
    async def test_multiple_messages_processing(self):
        """Test processing multiple messages in sequence."""
        trigger = AzureServiceBusQueueTrigger(queues=["test_queue"])

        # Add multiple messages
        messages = ["msg1", "msg2", "msg3"]
        for msg in messages:
            await trigger._queue.put(msg)

        # Collect events
        events = []
        async for event in trigger.run():
            events.append(event)
            if len(events) >= 3:
                break

        assert len(events) == 3
        received_messages = [event.payload for event in events]
        assert received_messages == messages

    def test_queue_trigger_with_empty_queues_list(self):
        """Test queue trigger with empty queues list."""
        trigger = AzureServiceBusQueueTrigger(queues=[])
        assert trigger.queues == []

    def test_subscription_trigger_with_empty_topics_list(self):
        """Test subscription trigger with empty topics list."""
        trigger = AzureServiceBusSubscriptionTrigger(topics=[], subscription_name="test-sub")
        assert trigger.topics == []

    @patch("airflow.providers.microsoft.azure.hooks.asb.MessageHook")
    def test_message_hook_initialization(self, mock_hook_class):
        """Test that MessageHook is properly initialized."""
        conn_id = "test_connection"
        trigger = AzureServiceBusQueueTrigger(queues=["test"], azure_service_bus_conn_id=conn_id)

        mock_hook_class.assert_called_with(azure_service_bus_conn_id=conn_id)

    def test_callback_creation_returns_callable(self):
        """Test that callback creation returns a callable function."""
        trigger = BaseAzureServiceBusTrigger()
        callback = trigger.create_message_callback()

        assert callable(callback)
        assert callable(callback)
