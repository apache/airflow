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
from abc import abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from asgiref.sync import sync_to_async

from airflow.providers.microsoft.azure.hooks.asb import MessageHook

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusReceivedMessage


class BaseAzureServiceBusTrigger(BaseEventTrigger):
    """
    Base trigger for Azure Service Bus message processing.

    This trigger provides common functionality for listening to Azure Service Bus
    queues and topics/subscriptions. It handles connection management and
    async message processing.

    :param poll_interval: Time interval between polling operations (seconds)
    :param azure_service_bus_conn_id: Connection ID for Azure Service Bus
    :param max_wait_time: Maximum time to wait for messages (seconds)
    """

    default_conn_name = "azure_service_bus_default"
    default_max_wait_time = None
    default_poll_interval = 60

    def __init__(
        self,
        poll_interval: float | None = None,
        azure_service_bus_conn_id: str | None = None,
        max_wait_time: float | None = None,
    ) -> None:
        self.connection_id = (
            azure_service_bus_conn_id
            if azure_service_bus_conn_id
            else BaseAzureServiceBusTrigger.default_conn_name
        )
        self.max_wait_time = (
            max_wait_time if max_wait_time else BaseAzureServiceBusTrigger.default_max_wait_time
        )
        self.poll_interval = (
            poll_interval if poll_interval else BaseAzureServiceBusTrigger.default_poll_interval
        )
        self.message_hook = MessageHook(azure_service_bus_conn_id=self.connection_id)

    @abstractmethod
    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger instance."""

    @abstractmethod
    def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the trigger logic."""

    @classmethod
    def _get_message_body(cls, message: ServiceBusReceivedMessage) -> str:
        message_body = message.body
        if isinstance(message_body, bytes):
            return message_body.decode("utf-8")
        try:
            return "".join(chunk.decode("utf-8") for chunk in message_body)
        except Exception:
            raise TypeError(f"Expected bytes or an iterator of bytes, but got {type(message_body).__name__}")


class AzureServiceBusQueueTrigger(BaseAzureServiceBusTrigger):
    """
    Trigger for Azure Service Bus Queue message processing.

    This trigger monitors one or more Azure Service Bus queues for incoming messages.
    When messages arrive, they are processed and yielded as trigger events that can
    be consumed by downstream tasks.

    Example:
        >>> trigger = AzureServiceBusQueueTrigger(
        ...     queues=["queue1", "queue2"],
        ...     azure_service_bus_conn_id="my_asb_conn",
        ...     poll_interval=30,
        ... )

    :param queues: List of queue names to monitor
    :param poll_interval: Time interval between polling operations (seconds)
    :param azure_service_bus_conn_id: Connection ID for Azure Service Bus
    :param max_wait_time: Maximum time to wait for messages (seconds)
    """

    def __init__(
        self,
        queues: list[str],
        poll_interval: float | None = None,
        azure_service_bus_conn_id: str | None = None,
        max_wait_time: float | None = None,
    ) -> None:
        super().__init__(poll_interval, azure_service_bus_conn_id, max_wait_time)
        self.queues = queues

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "azure_service_bus_conn_id": self.connection_id,
                "queues": self.queues,
                "poll_interval": self.poll_interval,
                "max_wait_time": self.max_wait_time,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        read_queue_message_async = sync_to_async(self.message_hook.read_message)

        while True:
            for queue_name in self.queues:
                message = await read_queue_message_async(
                    queue_name=queue_name, max_wait_time=self.max_wait_time
                )
                if message:
                    yield TriggerEvent(
                        {
                            "message": BaseAzureServiceBusTrigger._get_message_body(message),
                            "queue": queue_name,
                        }
                    )
                    break
            await asyncio.sleep(self.poll_interval)


class AzureServiceBusSubscriptionTrigger(BaseAzureServiceBusTrigger):
    """
    Trigger for Azure Service Bus Topic Subscription message processing.

    This trigger monitors topic subscriptions for incoming messages. It can handle
    multiple topics with a single subscription name, processing messages as they
    arrive and yielding them as trigger events.

    Example:
        >>> trigger = AzureServiceBusSubscriptionTrigger(
        ...     topics=["topic1", "topic2"],
        ...     subscription_name="my-subscription",
        ...     azure_service_bus_conn_id="my_asb_conn",
        ... )

    :param topics: List of topic names to monitor
    :param subscription_name: Name of the subscription to use
    :param poll_interval: Time interval between polling operations (seconds)
    :param azure_service_bus_conn_id: Connection ID for Azure Service Bus
    :param max_wait_time: Maximum time to wait for messages (seconds)
    """

    def __init__(
        self,
        topics: list[str],
        subscription_name: str,
        poll_interval: float | None = None,
        azure_service_bus_conn_id: str | None = None,
        max_wait_time: float | None = None,
    ) -> None:
        super().__init__(poll_interval, azure_service_bus_conn_id, max_wait_time)
        self.topics = topics
        self.subscription_name = subscription_name

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "azure_service_bus_conn_id": self.connection_id,
                "topics": self.topics,
                "subscription_name": self.subscription_name,
                "poll_interval": self.poll_interval,
                "max_wait_time": self.max_wait_time,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        read_subscription_message_async = sync_to_async(self.message_hook.read_subscription_message)

        while True:
            for topic_name in self.topics:
                message = await read_subscription_message_async(
                    topic_name=topic_name,
                    subscription_name=self.subscription_name,
                    max_wait_time=self.max_wait_time,
                )
                if message:
                    yield TriggerEvent(
                        {
                            "message": BaseAzureServiceBusTrigger._get_message_body(message),
                            "topic": topic_name,
                            "subscription": self.subscription_name,
                        }
                    )
                    break
            await asyncio.sleep(self.poll_interval)
