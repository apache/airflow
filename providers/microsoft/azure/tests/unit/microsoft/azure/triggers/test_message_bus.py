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

from unittest.mock import Mock, patch

import pytest

from airflow.providers.microsoft.azure.triggers.message_bus import (
    AzureServiceBusQueueTrigger,
    AzureServiceBusSubscriptionTrigger,
)
from airflow.triggers.base import TriggerEvent


class TestBaseAzureServiceBusTrigger:
    """Test the base trigger functionality."""

    def test_init_with_defaults(self):
        """Test initialization with default values using queue trigger."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(queues=["test_queue"])

            assert trigger.max_wait_time is None
            assert trigger.poll_interval == 60
            assert hasattr(trigger, "message_hook")

    def test_init_with_custom_values(self):
        """Test initialization with custom values using queue trigger."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(
                queues=["test_queue"],
                poll_interval=30,
                azure_service_bus_conn_id="custom_conn",
                max_wait_time=120,
            )

            assert trigger.poll_interval == 30
            assert trigger.max_wait_time == 120
            assert trigger.connection_id == "custom_conn"


class TestAzureServiceBusQueueTrigger:
    """Test the queue trigger functionality."""

    def test_init(self):
        """Test queue trigger initialization."""
        queues = ["queue1", "queue2"]
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(
                queues=queues,
                azure_service_bus_conn_id="test_conn",
            )

            assert trigger.queues == queues

    def test_serialize(self):
        """Test serialization of queue trigger."""
        queues = ["queue1", "queue2"]
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(
                queues=queues,
                azure_service_bus_conn_id="test_conn",
            )

            class_path, config = trigger.serialize()

            assert "AzureServiceBusQueueTrigger" in class_path
            assert config["queues"] == queues
            assert "azure_service_bus_conn_id" in config

    @pytest.mark.asyncio
    async def test_run_with_message(self):
        """Test the main run method with a mock message as bytes."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(
                queues=["test_queue"],
                poll_interval=0.01,  # Very short for testing
            )

            mock_message = Mock(body=b"test message")
            trigger.message_hook.read_message = Mock(return_value=mock_message)

            # Get one event from the generator
            events = []
            async for event in trigger.run():
                events.append(event)
                if len(events) >= 1:
                    break

            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["message"] == "test message"
            assert events[0].payload["queue"] == "test_queue"

    @pytest.mark.asyncio
    async def test_run_with_iterator_message(self):
        """Test the main run method with a mock message as an iterator."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(
                queues=["test_queue"],
                poll_interval=0.01,  # Very short for testing
            )

            mock_message = Mock(body=iter([b"test", b" ", b"iterator", b" ", b"message"]))
            trigger.message_hook.read_message = Mock(return_value=mock_message)

            # Get one event from the generator
            events = []
            async for event in trigger.run():
                events.append(event)
                if len(events) >= 1:
                    break

            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["message"] == "test iterator message"
            assert events[0].payload["queue"] == "test_queue"


class TestAzureServiceBusSubscriptionTrigger:
    """Test the subscription trigger functionality."""

    def test_init(self):
        """Test subscription trigger initialization."""
        topics = ["topic1", "topic2"]
        subscription = "test-subscription"
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
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
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusSubscriptionTrigger(
                topics=topics,
                subscription_name=subscription,
                azure_service_bus_conn_id="test_conn",
            )

            class_path, config = trigger.serialize()

            assert "AzureServiceBusSubscriptionTrigger" in class_path
            assert config["topics"] == topics
            assert config["subscription_name"] == subscription

    @pytest.mark.asyncio
    async def test_run_subscription_with_message(self):
        """Test the main run method with a mock message as bytes."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusSubscriptionTrigger(
                topics=["test_topic"],
                subscription_name="test-sub",
                poll_interval=0.01,  # Very short for testing
                azure_service_bus_conn_id="test_conn",
            )

            mock_message = Mock(body=b"subscription test message")
            trigger.message_hook.read_subscription_message = Mock(return_value=mock_message)

            # Get one event from the generator
            events = []
            async for event in trigger.run():
                events.append(event)
                if len(events) >= 1:
                    break

            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["message"] == "subscription test message"
            assert events[0].payload["topic"] == "test_topic"
            assert events[0].payload["subscription"] == "test-sub"

    @pytest.mark.asyncio
    async def test_run_subscription_with_iterator_message(self):
        """Test the main run method with a mock message as an iterator."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusSubscriptionTrigger(
                topics=["test_topic"],
                subscription_name="test-sub",
                poll_interval=0.01,  # Very short for testing
                azure_service_bus_conn_id="test_conn",
            )

            mock_message = Mock(body=iter([b"iterator", b" ", b"subscription"]))
            trigger.message_hook.read_subscription_message = Mock(return_value=mock_message)

            # Get one event from the generator
            events = []
            async for event in trigger.run():
                events.append(event)
                if len(events) >= 1:
                    break

            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["message"] == "iterator subscription"
            assert events[0].payload["topic"] == "test_topic"
            assert events[0].payload["subscription"] == "test-sub"


class TestIntegrationScenarios:
    """Test integration scenarios and edge cases."""

    @pytest.mark.asyncio
    async def test_multiple_messages_processing(self):
        """Test processing multiple messages in sequence."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(
                queues=["test_queue"],
                poll_interval=0.01,  # Very short for testing
            )

            messages_as_str = ["msg1", "msg2", "msg3"]
            mock_messages = [Mock(body=msg.encode("utf-8")) for msg in messages_as_str]
            trigger.message_hook.read_message = Mock(side_effect=mock_messages + [None])

            # Collect events
            events = []
            async for event in trigger.run():
                events.append(event)
                if len(events) >= 3:
                    break

            assert len(events) == 3
            received_messages = [event.payload["message"] for event in events]
            assert received_messages == messages_as_str

    def test_queue_trigger_with_empty_queues_list(self):
        """Test queue trigger with empty queues list."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusQueueTrigger(queues=[])
            assert trigger.queues == []

    def test_subscription_trigger_with_empty_topics_list(self):
        """Test subscription trigger with empty topics list."""
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = AzureServiceBusSubscriptionTrigger(
                topics=[], subscription_name="test-sub", azure_service_bus_conn_id="test_conn"
            )
            assert trigger.topics == []

    def test_message_hook_initialization(self):
        """Test that MessageHook is properly initialized."""
        conn_id = "test_connection"
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook") as mock_hook_class:
            trigger = AzureServiceBusQueueTrigger(queues=["test"], azure_service_bus_conn_id=conn_id)

            # Verify the hook was initialized with the correct connection ID
            mock_hook_class.assert_called_once_with(azure_service_bus_conn_id=conn_id)
            # Also verify the trigger has the message_hook attribute
            assert hasattr(trigger, "message_hook")

    def test_message_hook_properly_configured(self):
        """Test that MessageHook is properly configured with connection."""
        conn_id = "test_connection"
        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook") as mock_hook_class:
            trigger = AzureServiceBusQueueTrigger(queues=["test"], azure_service_bus_conn_id=conn_id)

            # Verify the hook was called with the correct parameters
            mock_hook_class.assert_called_once_with(azure_service_bus_conn_id=conn_id)
            assert hasattr(trigger, "message_hook")
            # Verify the connection_id is set correctly
            assert trigger.connection_id == conn_id
