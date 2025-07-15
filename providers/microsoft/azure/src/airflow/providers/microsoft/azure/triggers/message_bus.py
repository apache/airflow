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
from collections.abc import TYPE_CHECKING, AsyncGenerator, Callable

from asgiref.sync import sync_to_async

from airflow.providers.microsoft.azure.hooks.asb import MessageCallback, MessageHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context

if TYPE_CHECKING:
    from azure.servicebus import (
        ServiceBusMessage,
    )


class BaseAzureServiceBusTrigger(BaseTrigger):
    """
    Base trigger for Azure Service Bus message processing.

    This trigger provides common functionality for listening to Azure Service Bus
    queues and topics/subscriptions. It handles connection management, message
    callback creation, and async message processing.

    :param context: Airflow context for task execution
    :param poll_interval: Time interval between polling operations (seconds)
    :param azure_service_bus_conn_id: Connection ID for Azure Service Bus
    :param max_message_count: Maximum number of messages to retrieve per batch
    :param max_wait_time: Maximum time to wait for messages (seconds)
    """

    default_conn_name = "azure_service_bus_default"
    default_max_message_count = 1
    default_max_wait_time = None
    default_context = Context()
    default_poll_interval = 60

    def __init__(
        self,
        context: Context | None = None,
        poll_interval: float | None = None,
        azure_service_bus_conn_id: str | None = None,
        max_message_count: int | None = None,
        max_wait_time: float | None = None,
    ) -> None:
        self.connection_id = (
            azure_service_bus_conn_id
            if azure_service_bus_conn_id
            else BaseAzureServiceBusTrigger.default_azure_service_bus_conn_id
        )
        self.context = context if context else BaseAzureServiceBusTrigger.default_context
        self.max_message_count = (
            max_message_count if max_message_count else BaseAzureServiceBusTrigger.default_max_message_count
        )
        self.max_wait_time = (
            max_wait_time if max_wait_time else BaseAzureServiceBusTrigger.default_max_wait_time
        )
        self.poll_interval = (
            poll_interval if poll_interval else BaseAzureServiceBusTrigger.default_poll_interval
        )
        self._queue = asyncio.Queue()
        self.message_hook = MessageHook(azure_service_bus_conn_id=azure_service_bus_conn_id)

    def create_message_callback(self) -> MessageCallback:
        """
        Create a message callback function that adds messages to the internal queue.

        This callback is used by the Service Bus receiver to process incoming messages.
        The callback extracts the message body and puts it into an async queue for
        further processing by the trigger.

        :return: MessageCallback function that processes Service Bus messages
        """

        def message_callback(message: ServiceBusMessage, context: Context) -> None:
            asyncio.run(self._queue.put(message.body))

        return message_callback

    async def callback_listener(self) -> AsyncGenerator[str]:
        """
        Async generator that yields messages from the internal queue.

        This method continuously listens for messages that have been added to the
        internal queue by the message callback. It yields each message as it becomes
        available, with optional timeout handling.

        :yield: Message body strings from the Service Bus
        :raises asyncio.TimeoutError: If max_wait_time is exceeded without receiving a message
        """
        while True:
            message = await asyncio.wait_for(self._queue.get(), timeout=self.max_wait_time)
            yield message


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
        ...     max_message_count=10,
        ...     poll_interval=30,
        ... )

    :param queues: List of queue names to monitor
    :param context: Airflow context for task execution
    :param poll_interval: Time interval between polling operations (seconds)
    :param azure_service_bus_conn_id: Connection ID for Azure Service Bus
    :param max_message_count: Maximum number of messages to retrieve per batch
    :param max_wait_time: Maximum time to wait for messages (seconds)
    """

    def __init__(
        self,
        queues: list[str],
        context: Context | None = None,
        poll_interval: float | None = None,
        azure_service_bus_conn_id: str | None = None,
        max_message_count: int | None = None,
        max_wait_time: float | None = None,
    ) -> None:
        super().__init__(context, poll_interval, azure_service_bus_conn_id, max_message_count, max_wait_time)
        self.queues = queues

    def serialize(self) -> tuple[str, dict[str, any]]:
        return {
            "class_name": self.__class__.__name__,
            "config": {
                "azure_service_bus_conn_id": self.connection_id,
                "queues": self.queues,
                "poll_interval": self.poll_interval,
                "context": self.context,
                "max_message_count": self.max_message_count,
                "max_wait_time": self.max_wait_time,
            },
        }

    def trigger_queue_listening(self, queue: str, receive_message_async: Callable) -> asyncio.Task:
        return asyncio.create_task(
            receive_message_async(
                queue,
                self.context,
                self.max_message_count,
                self.max_wait_time,
                message_callback=self.create_message_callback(),
            )
        )

    async def run_single(self, queue: str) -> None:
        receive_message_async = sync_to_async(self.message_hook.receive_message)
        while True:
            tasks = []
            task = self.trigger_queue_listening(queue, receive_message_async)
            tasks.append(task)
            await asyncio.gather(*tasks)
            asyncio.sleep(self.poll_interval)

    async def run(self) -> AsyncGenerator[TriggerEvent, None]:
        queue_tasks = []
        for queue in self.queues:
            task = asyncio.create_task(self.run_single(queue))
            queue_tasks.append(task)
        async for msg in self.callback_listener():
            yield TriggerEvent(msg)


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
        ...     max_message_count=5,
        ... )

    :param topics: List of topic names to monitor
    :param subscription_name: Name of the subscription to use
    :param context: Airflow context for task execution
    :param poll_interval: Time interval between polling operations (seconds)
    :param azure_service_bus_conn_id: Connection ID for Azure Service Bus
    :param max_message_count: Maximum number of messages to retrieve per batch
    :param max_wait_time: Maximum time to wait for messages (seconds)
    """

    def __init__(
        self,
        topics: list[str],
        subscription_name: str,
        context: Context | None = None,
        poll_interval: float | None = None,
        azure_service_bus_conn_id: str | None = None,
        max_message_count: int | None = None,
        max_wait_time: float | None = None,
    ) -> None:
        super().__init__(context, poll_interval, azure_service_bus_conn_id, max_message_count, max_wait_time)
        self.topics = topics
        self.subscription_name = subscription_name

    def serialize(self) -> tuple[str, dict[str, any]]:
        return {
            "class_name": self.__class__.__name__,
            "config": {
                "azure_service_bus_conn_id": self.connection_id,
                "topics": self.topics,
                "subscription_name": self.subscription_name,
                "poll_interval": self.poll_interval,
                "context": self.context,
                "max_message_count": self.max_message_count,
                "max_wait_time": self.max_wait_time,
            },
        }

    def trigger_topic_listening(self, topic: str, receive_message_async: Callable) -> asyncio.Task:
        return asyncio.create_task(
            receive_message_async(
                topic,
                self.subscription_name,
                self.context,
                self.max_message_count,
                self.max_wait_time,
                message_callback=self.create_message_callback(),
            )
        )

    async def run_single(self, topic: str) -> None:
        receive_message_async = sync_to_async(self.message_hook.receive_subscription_message)
        while True:
            tasks = []
            task = self.trigger_topic_listening(topic, receive_message_async)
            tasks.append(task)
            await asyncio.gather(*tasks)
            asyncio.sleep(self.poll_interval)

    async def run(self) -> AsyncGenerator[TriggerEvent, None]:
        topic_tasks = []
        for topic in self.topics:
            task = asyncio.create_task(self.run_single(topic))
            topic_tasks.append(task)
        async for msg in self.callback_listener():
            yield TriggerEvent(msg)
