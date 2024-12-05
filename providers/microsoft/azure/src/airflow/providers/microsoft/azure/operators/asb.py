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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.servicebus import ServiceBusMessage
from azure.servicebus.management import (
    AuthorizationRule,
    CorrelationRuleFilter,
    ServiceBusAdministrationClient,
    SqlRuleFilter,
)

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.asb import AdminClientHook, MessageHook

if TYPE_CHECKING:
    import datetime

    from airflow.utils.context import Context
    from azure.servicebus import ServiceBusMessage
    from azure.servicebus.management import AuthorizationRule, CorrelationRuleFilter, SqlRuleFilter

    MessageCallback = Callable[[ServiceBusMessage, Context], None]


class AzureServiceBusCreateQueueOperator(BaseOperator):
    """
    Create a Azure Service Bus queue under a Service Bus Namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusCreateQueueOperator`

    :param queue_name: The name of the queue. should be unique.
    :param max_delivery_count: The maximum delivery count. A message is automatically
            dead lettered after this number of deliveries. Default value is 10..
    :param dead_lettering_on_message_expiration: A value that indicates whether this subscription has
        dead letter support when a message expires.
    :param enable_batched_operations: Value that indicates whether server-side batched
        operations are enabled.
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        max_delivery_count: int = 10,
        dead_lettering_on_message_expiration: bool = True,
        enable_batched_operations: bool = True,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.max_delivery_count = max_delivery_count
        self.dead_lettering_on_message_expiration = dead_lettering_on_message_expiration
        self.enable_batched_operations = enable_batched_operations
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: Context) -> None:
        """Create Queue in Azure Service Bus namespace, by connecting to Service Bus Admin client in hook."""
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # create queue with name
        queue = hook.create_queue(
            self.queue_name,
            self.max_delivery_count,
            self.dead_lettering_on_message_expiration,
            self.enable_batched_operations,
        )
        self.log.info("Created Queue %s", queue.name)


class AzureServiceBusSendMessageOperator(BaseOperator):
    """
    Send Message or batch message to the Service Bus queue.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusSendMessageOperator`

    :param queue_name: The name of the queue. should be unique.
    :param message: Message which needs to be sent to the queue. It can be string or list of string.
    :param batch: Its boolean flag by default it is set to False, if the message needs to be sent
        as batch message it can be set to True.
    :param azure_service_bus_conn_id: Reference to the
        :ref: `Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        message: str | list[str],
        batch: bool = False,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.batch = batch
        self.message = message
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: Context) -> None:
        """Send Message to the specific queue in Service Bus namespace."""
        # Create the hook
        hook = MessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # send message
        hook.send_message(self.queue_name, self.message, self.batch)


class AzureServiceBusReceiveMessageOperator(BaseOperator):
    """
    Receive a batch of messages at once in a specified Queue name.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusReceiveMessageOperator`

    :param queue_name: The name of the queue name or a QueueProperties with name.
    :param max_message_count: Maximum number of messages in the batch.
    :param max_wait_time: Maximum time to wait in seconds for the first message to arrive.
    :param azure_service_bus_conn_id: Reference to the
        :ref: `Azure Service Bus connection <howto/connection:azure_service_bus>`.
    :param message_callback: Optional callback to process each message. If not provided, then
        the message will be logged and completed. If provided, and throws an exception, the
        message will be abandoned for future redelivery.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        max_message_count: int = 10,
        max_wait_time: float = 5,
        message_callback: MessageCallback | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id
        self.max_message_count = max_message_count
        self.max_wait_time = max_wait_time
        self.message_callback = message_callback

    def execute(self, context: Context) -> None:
        """Receive Message in specific queue in Service Bus namespace by connecting to Service Bus client."""
        # Create the hook
        hook = MessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # Receive message
        hook.receive_message(
            self.queue_name,
            context,
            max_message_count=self.max_message_count,
            max_wait_time=self.max_wait_time,
            message_callback=self.message_callback,
        )


class AzureServiceBusDeleteQueueOperator(BaseOperator):
    """
    Delete the Queue in the Azure Service Bus namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusDeleteQueueOperator`

    :param queue_name: The name of the queue in Service Bus namespace.
    :param azure_service_bus_conn_id: Reference to the
        :ref: `Azure Service Bus connection <howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: Context) -> None:
        """Delete Queue in Service Bus namespace, by connecting to Service Bus Admin client."""
        # Create the hook
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # delete queue with name
        hook.delete_queue(self.queue_name)


class AzureServiceBusTopicCreateOperator(BaseOperator):
    """
    Create an Azure Service Bus Topic under a Service Bus Namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusTopicCreateOperator`

    :param topic_name: Name of the topic.
    :param default_message_time_to_live: ISO 8601 default message time span to live value. This is
     the duration after which the message expires, starting from when the message is sent to Service
     Bus. This is the default value used when TimeToLive is not set on a message itself.
     Input value of either type ~datetime.timedelta or string in ISO 8601 duration format
     like "PT300S" is accepted.
    :param max_size_in_megabytes: The maximum size of the topic in megabytes, which is the size of
     memory allocated for the topic.
    :param requires_duplicate_detection: A value indicating if this topic requires duplicate
     detection.
    :param duplicate_detection_history_time_window: ISO 8601 time span structure that defines the
     duration of the duplicate detection history. The default value is 10 minutes.
     Input value of either type ~datetime.timedelta or string in ISO 8601 duration format
     like "PT300S" is accepted.
    :param enable_batched_operations: Value that indicates whether server-side batched operations
     are enabled.
    :param size_in_bytes: The size of the topic, in bytes.
    :param filtering_messages_before_publishing: Filter messages before publishing.
    :param authorization_rules: List of Authorization rules for resource.
    :param support_ordering: A value that indicates whether the topic supports ordering.
    :param auto_delete_on_idle: ISO 8601 time span idle interval after which the topic is
     automatically deleted. The minimum duration is 5 minutes.
     Input value of either type ~datetime.timedelta or string in ISO 8601 duration format
     like "PT300S" is accepted.
    :param enable_partitioning: A value that indicates whether the topic is to be partitioned
     across multiple message brokers.
    :param enable_express: A value that indicates whether Express Entities are enabled. An express
     queue holds a message in memory temporarily before writing it to persistent storage.
    :param user_metadata: Metadata associated with the topic.
    :param max_message_size_in_kilobytes: The maximum size in kilobytes of message payload that
     can be accepted by the queue. This feature is only available when using a Premium namespace
     and Service Bus API version "2021-05" or higher.
     The minimum allowed value is 1024 while the maximum allowed value is 102400. Default value is 1024.
    """

    template_fields: Sequence[str] = ("topic_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        default_message_time_to_live: datetime.timedelta | str | None = None,
        max_size_in_megabytes: int | None = None,
        requires_duplicate_detection: bool | None = None,
        duplicate_detection_history_time_window: datetime.timedelta | str | None = None,
        enable_batched_operations: bool | None = None,
        size_in_bytes: int | None = None,
        filtering_messages_before_publishing: bool | None = None,
        authorization_rules: list[AuthorizationRule] | None = None,
        support_ordering: bool | None = None,
        auto_delete_on_idle: datetime.timedelta | str | None = None,
        enable_partitioning: bool | None = None,
        enable_express: bool | None = None,
        user_metadata: str | None = None,
        max_message_size_in_kilobytes: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id
        self.default_message_time_to_live = default_message_time_to_live
        self.max_size_in_megabytes = max_size_in_megabytes
        self.requires_duplicate_detection = requires_duplicate_detection
        self.duplicate_detection_history_time_window = duplicate_detection_history_time_window
        self.enable_batched_operations = enable_batched_operations
        self.size_in_bytes = size_in_bytes
        self.filtering_messages_before_publishing = filtering_messages_before_publishing
        self.authorization_rules = authorization_rules
        self.support_ordering = support_ordering
        self.auto_delete_on_idle = auto_delete_on_idle
        self.enable_partitioning = enable_partitioning
        self.enable_express = enable_express
        self.user_metadata = user_metadata
        self.max_message_size_in_kilobytes = max_message_size_in_kilobytes

    def execute(self, context: Context) -> str:
        """Create Topic in Service Bus namespace, by connecting to Service Bus Admin client."""
        if self.topic_name is None:
            raise TypeError("Topic name cannot be None.")

        # Create the hook
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        return hook.create_topic(
            topic_name=self.topic_name,
            default_message_time_to_live=self.default_message_time_to_live,
            max_size_in_megabytes=self.max_size_in_megabytes,
            requires_duplicate_detection=self.requires_duplicate_detection,
            duplicate_detection_history_time_window=self.duplicate_detection_history_time_window,
            enable_batched_operations=self.enable_batched_operations,
            size_in_bytes=self.size_in_bytes,
            filtering_messages_before_publishing=self.filtering_messages_before_publishing,
            authorization_rules=self.authorization_rules,
            support_ordering=self.support_ordering,
            auto_delete_on_idle=self.auto_delete_on_idle,
            enable_partitioning=self.enable_partitioning,
            enable_express=self.enable_express,
            user_metadata=self.user_metadata,
            max_message_size_in_kilobytes=self.max_message_size_in_kilobytes,
        )


class AzureServiceBusSubscriptionCreateOperator(BaseOperator):
    """
    Create an Azure Service Bus Topic Subscription under a Service Bus Namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusSubscriptionCreateOperator`

    :param topic_name: The topic that will own the to-be-created subscription.
    :param subscription_name: Name of the subscription that need to be created
    :param lock_duration: ISO 8601 time span duration of a peek-lock; that is, the amount of time that
        the message is locked for other receivers. The maximum value for LockDuration is 5 minutes; the
        default value is 1 minute. Input value of either type ~datetime.timedelta or string in ISO 8601
        duration format like "PT300S" is accepted.
    :param requires_session: A value that indicates whether the queue supports the concept of sessions.
    :param default_message_time_to_live: ISO 8601 default message time span to live value. This is the
        duration after which the message expires, starting from when the message is sent to
        Service Bus. This is the default value used when TimeToLive is not set on a message itself.
        Input value of either type ~datetime.timedelta or string in ISO 8601 duration
        format like "PT300S" is accepted.
    :param dead_lettering_on_message_expiration: A value that indicates whether this subscription has
        dead letter support when a message expires.
    :param dead_lettering_on_filter_evaluation_exceptions: A value that indicates whether this
        subscription has dead letter support when a message expires.
    :param max_delivery_count: The maximum delivery count. A message is automatically dead lettered
        after this number of deliveries. Default value is 10.
    :param enable_batched_operations: Value that indicates whether server-side batched
        operations are enabled.
    :param forward_to: The name of the recipient entity to which all the messages sent to the
        subscription are forwarded to.
    :param user_metadata: Metadata associated with the subscription. Maximum number of characters is 1024.
    :param forward_dead_lettered_messages_to: The name of the recipient entity to which all the
        messages sent to the subscription are forwarded to.
    :param auto_delete_on_idle: ISO 8601 time Span idle interval after which the subscription is
        automatically deleted. The minimum duration is 5 minutes. Input value of either
        type ~datetime.timedelta or string in ISO 8601 duration format like "PT300S" is accepted.
    :param filter_rule: Optional correlation or SQL rule filter to apply on the messages.
    :param filter_rule_name: Optional rule name to use applying the rule filter to the subscription
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    :param filter: The filter expression used to match messages. For SQLFilter, the expression
    """

    template_fields: Sequence[str] = ("topic_name", "subscription_name")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        subscription_name: str,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        lock_duration: datetime.timedelta | str | None = None,
        requires_session: bool | None = None,
        default_message_time_to_live: datetime.timedelta | str | None = None,
        dead_lettering_on_message_expiration: bool | None = True,
        dead_lettering_on_filter_evaluation_exceptions: bool | None = None,
        max_delivery_count: int | None = 10,
        enable_batched_operations: bool | None = True,
        forward_to: str | None = None,
        user_metadata: str | None = None,
        forward_dead_lettered_messages_to: str | None = None,
        auto_delete_on_idle: datetime.timedelta | str | None = None,
        filter_rule: CorrelationRuleFilter | SqlRuleFilter | None = None,
        filter_rule_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.lock_duration = lock_duration
        self.requires_session = requires_session
        self.default_message_time_to_live = default_message_time_to_live
        self.dl_on_message_expiration = dead_lettering_on_message_expiration
        self.dl_on_filter_evaluation_exceptions = dead_lettering_on_filter_evaluation_exceptions
        self.max_delivery_count = max_delivery_count
        self.enable_batched_operations = enable_batched_operations
        self.forward_to = forward_to
        self.user_metadata = user_metadata
        self.forward_dead_lettered_messages_to = forward_dead_lettered_messages_to
        self.auto_delete_on_idle = auto_delete_on_idle
        self.azure_service_bus_conn_id = azure_service_bus_conn_id
        self.filter_rule = filter_rule
        self.filter_rule_name = filter_rule_name

    def execute(self, context: Context) -> None:
        """Create Subscription in Service Bus namespace, by connecting to Service Bus Admin client."""
        if self.subscription_name is None:
            raise TypeError("Subscription name cannot be None.")
        if self.topic_name is None:
            raise TypeError("Topic name cannot be None.")
        # Create the hook
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        subscription = hook.create_subscription(
            topic_name=self.topic_name,
            subscription_name=self.subscription_name,
            lock_duration=self.lock_duration,
            requires_session=self.requires_session,
            default_message_time_to_live=self.default_message_time_to_live,
            dead_lettering_on_message_expiration=self.dl_on_message_expiration,
            dead_lettering_on_filter_evaluation_exceptions=self.dl_on_filter_evaluation_exceptions,
            max_delivery_count=self.max_delivery_count,
            enable_batched_operations=self.enable_batched_operations,
            forward_to=self.forward_to,
            user_metadata=self.user_metadata,
            forward_dead_lettered_messages_to=self.forward_dead_lettered_messages_to,
            auto_delete_on_idle=self.auto_delete_on_idle,
            filter_rule=self.filter_rule,
            filter_rule_name=self.filter_rule_name,
        )
        self.log.info("Created subscription %s", subscription.name)


class AzureServiceBusUpdateSubscriptionOperator(BaseOperator):
    """
    Update an Azure ServiceBus Topic Subscription under a ServiceBus Namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusUpdateSubscriptionOperator`

    :param topic_name: The topic that will own the to-be-created subscription.
    :param subscription_name: Name of the subscription that need to be created.
    :param max_delivery_count: The maximum delivery count. A message is automatically dead lettered
        after this number of deliveries. Default value is 10.
    :param dead_lettering_on_message_expiration: A value that indicates whether this subscription
        has dead letter support when a message expires.
    :param enable_batched_operations: Value that indicates whether server-side batched
        operations are enabled.
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("topic_name", "subscription_name")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        subscription_name: str,
        max_delivery_count: int | None = None,
        dead_lettering_on_message_expiration: bool | None = None,
        enable_batched_operations: bool | None = None,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.max_delivery_count = max_delivery_count
        self.dl_on_message_expiration = dead_lettering_on_message_expiration
        self.enable_batched_operations = enable_batched_operations
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: Context) -> None:
        """Update Subscription properties, by connecting to Service Bus Admin client."""
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        hook.update_subscription(
            topic_name=self.topic_name,
            subscription_name=self.subscription_name,
            max_delivery_count=self.max_delivery_count,
            dead_lettering_on_message_expiration=self.dl_on_message_expiration,
            enable_batched_operations=self.enable_batched_operations,
        )


class ASBReceiveSubscriptionMessageOperator(BaseOperator):
    """
    Receive a Batch messages from a Service Bus Subscription under specific Topic.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ASBReceiveSubscriptionMessageOperator`

    :param subscription_name: The subscription name that will own the rule in topic
    :param topic_name: The topic that will own the subscription rule.
    :param max_message_count: Maximum number of messages in the batch.
        Actual number returned will depend on prefetch_count and incoming stream rate.
        Setting to None will fully depend on the prefetch config. The default value is 1.
    :param max_wait_time: Maximum time to wait in seconds for the first message to arrive. If no
        messages arrive, and no timeout is specified, this call will not return until the
        connection is closed. If specified, an no messages arrive within the timeout period,
        an empty list will be returned.
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection <howto/connection:azure_service_bus>`.
    :param message_callback: Optional callback to process each message. If not provided, then
        the message will be logged and completed. If provided, and throws an exception, the
        message will be abandoned for future redelivery.
    """

    template_fields: Sequence[str] = ("topic_name", "subscription_name")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        subscription_name: str,
        max_message_count: int | None = 1,
        max_wait_time: float | None = 5,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        message_callback: MessageCallback | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.max_message_count = max_message_count
        self.max_wait_time = max_wait_time
        self.azure_service_bus_conn_id = azure_service_bus_conn_id
        self.message_callback = message_callback

    def execute(self, context: Context) -> None:
        """Receive Message in specific queue in Service Bus namespace by connecting to Service Bus client."""
        # Create the hook
        hook = MessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # Receive message
        hook.receive_subscription_message(
            self.topic_name,
            self.subscription_name,
            context,
            self.max_message_count,
            self.max_wait_time,
            message_callback=self.message_callback,
        )


class AzureServiceBusSubscriptionDeleteOperator(BaseOperator):
    """
    Delete the topic subscription in the Azure ServiceBus namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusSubscriptionDeleteOperator`

    :param topic_name: The topic that will own the to-be-created subscription.
    :param subscription_name: Name of the subscription that need to be created
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection <howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("topic_name", "subscription_name")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        subscription_name: str,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: Context) -> None:
        """Delete topic subscription in Service Bus namespace, by connecting to Service Bus Admin client."""
        # Create the hook
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # delete subscription with name
        hook.delete_subscription(self.subscription_name, self.topic_name)


class AzureServiceBusTopicDeleteOperator(BaseOperator):
    """
    Delete the topic in the Azure Service Bus namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusTopicDeleteOperator`

    :param topic_name: Name of the topic to be deleted.
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection <howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("topic_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: Context) -> None:
        """Delete topic in Service Bus namespace, by connecting to Service Bus Admin client."""
        if self.topic_name is None:
            raise TypeError("Topic name cannot be None.")
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        with hook.get_conn() as service_mgmt_conn:
            topic_properties = service_mgmt_conn.get_topic(self.topic_name)
            if topic_properties and topic_properties.name == self.topic_name:
                service_mgmt_conn.delete_topic(self.topic_name)
                self.log.info("Topic %s deleted.", self.topic_name)
            else:
                self.log.info("Topic %s does not exist.", self.topic_name)


class AzureServiceBusRequestReplyOperator(BaseOperator):
    """
    Implement request-reply pattern using Azure Service Bus.

    Send a message to an Azure Service Bus Queue and receive a reply by correlation id from an Azure Service
    Bus Topic. This implements the Request-Reply pattern from Enterprise Integration Patterns, Hohpe, Woolf,
    Addison-Wesley, 2003: https://www.enterpriseintegrationpatterns.com/patterns/messaging/RequestReply.html

    Steps are:
        1. Generate a unique ID for the message. The subscription needs to exist before the request message is
            sent or there will be a race condition where the reply message could be sent before the
            subscription is created. By default, a UUID is used for the unique ID and this should be
            reasonably unique over the life of the universe.
        2. Create a subscription to the reply topic for messages where the correlation ID equals the unique ID
            created in #1.
        3. Send the message to the request queue with (a) the reply-to property set to the reply topic,
            (b) the reply type property set to topic, and (c) the message ID set to the unique ID.
        4. Wait for a reply message on the reply topic with the correlation ID set to the unique ID.
        5. Remove the subscription on the reply topic.

    The caller must pass in a generator function to create the request message body (request_body_generator)
    from the context and an optional callback can  process the reply message (reply_message_callback). This
    callback could either detect errors and abort processing by throwing an exception or could process the
    message body and add information into XComs for downstream tasks to use.

    :param request_queue_name: Name of the queue to send the request to. This queue must be reachable from
        a connection created from the connection name specified in the param `azure_service_bus_conn_id`.
    :param request_body_generator: A method to generate the request message body from the context.
    :param reply_topic_name: Name of the topic to send the reply to. This topic must be reachable from
        a connection created from the connection name specified in the param "azure_service_bus_conn_id".
    :param reply_correlation_id: a string to use to correlate the request and reply. This will also be the
        message ID of the request message. If not specified, a UUID will be generated and used. Generally,
        the default behavior should be used.
    :param max_wait_time: maximum wait for a reply in seconds. This should be set to some small multiple of
        the expected processing time. Perhaps 3x the expected processing time. Default is 60 seconds.
    :param reply_message_callback: An optional callback to handle the response message. This takes the service
        bus message and the context as parameters and can raise an exception to abort processing or insert
        values into the XCOM for downstream tasks to use.
    :param azure_service_bus_conn_id: ID of the airflow connection to the Azure Service Bus. It defaults to
        `azure_service_bus_default`,
    """

    REPLY_SUBSCRIPTION_PREFIX = "reply-"
    REPLY_RULE_SUFFIX = "-rule"

    def __init__(
        self,
        *,
        request_queue_name: str,
        request_body_generator: Callable[[Context], str],
        reply_topic_name: str,
        reply_correlation_id: str | None = None,
        max_wait_time: float = 60,
        reply_message_callback: MessageCallback | None = None,
        azure_service_bus_conn_id: str = "azure_service_bus_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.request_queue_name = request_queue_name
        self.request_body_generator = request_body_generator
        self.reply_topic_name = reply_topic_name
        self.reply_correlation_id = reply_correlation_id
        self.max_wait_time = max_wait_time
        self.reply_message_callback = reply_message_callback
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

        if not self.reply_correlation_id:
            self.reply_correlation_id = str(uuid4())
        self.subscription_name = self.REPLY_SUBSCRIPTION_PREFIX + self.reply_correlation_id

    def execute(self, context: Context) -> None:
        """Implement the request-reply pattern using existing hooks."""
        self._validate_params()
        admin_hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)
        self._create_reply_subscription_for_correlation_id(admin_hook, context)

        message_hook = MessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)
        try:
            # send the request message
            self._send_request_message(message_hook, context)

            # Wait for and receive the reply message
            message_hook.receive_subscription_message(
                self.reply_topic_name,
                self.subscription_name,
                context,
                max_message_count=1,
                max_wait_time=self.max_wait_time,
                message_callback=self.reply_message_callback,
            )
        finally:
            # Remove the subscription on the reply topic
            self._remove_reply_subscription(admin_hook)

    def _send_request_message(self, message_hook: MessageHook, context: Context) -> None:
        with message_hook.get_conn() as service_bus_client:
            message = ServiceBusMessage(
                self.request_body_generator(context),
                application_properties={"reply_type": "topic"},
                message_id=self.reply_correlation_id,
                reply_to=self.reply_topic_name,
            )
            with service_bus_client.get_queue_sender(queue_name=self.request_queue_name) as sender:
                sender.send_messages(message, timeout=60)
                self.log.info(
                    "Sent request with id %s to queue %s", self.reply_correlation_id, self.request_queue_name
                )

    def _create_reply_subscription_for_correlation_id(
        self, admin_hook: AdminClientHook, context: Context
    ) -> None:
        """Create subscription on the reply topic for the correlation ID."""
        with admin_hook.get_conn() as admin_asb_conn:
            self.log.info(
                "Creating subscription %s on topic %s", self.subscription_name, self.reply_topic_name
            )
            try:
                self._create_subscription(admin_asb_conn, context)
            except ResourceExistsError:
                # subscription already created, so return
                self.log.info(
                    "Subscription %s on topic %s already existed.",
                    self.subscription_name,
                    self.reply_topic_name,
                )
                return

            # remove default rule (which accepts all messages)
            try:
                admin_asb_conn.delete_rule(self.reply_topic_name, self.subscription_name, "$Default")
            except ResourceNotFoundError:
                # as long as it is gone :)
                self.log.debug("Could not find default rule '$Default' to delete; ignoring")

            # add a rule to filter on the correlation ID
            rule_name = self.subscription_name + self.REPLY_RULE_SUFFIX
            filter = CorrelationRuleFilter(correlation_id=self.reply_correlation_id)
            admin_asb_conn.create_rule(
                self.reply_topic_name, self.subscription_name, rule_name, filter=filter
            )
            self.log.info(
                "Created subscription %s on topic %s", self.subscription_name, self.reply_topic_name
            )

    def _create_subscription(self, admin_asb_conn: ServiceBusAdministrationClient, context: Context):
        return admin_asb_conn.create_subscription(
            topic_name=self.reply_topic_name,
            subscription_name=self.subscription_name,
            default_message_time_to_live="PT1H",  # 1 hour
            dead_lettering_on_message_expiration=True,
            dead_lettering_on_filter_evaluation_exceptions=True,
            enable_batched_operations=False,
            user_metadata=f"Subscription for reply to {self.reply_correlation_id} for task ID {context['task'].task_id}",
            auto_delete_on_idle="PT6H",  # 6 hours
        )

    def _remove_reply_subscription(self, admin_hook: AdminClientHook) -> None:
        try:
            admin_hook.delete_subscription(self.subscription_name, self.reply_topic_name)
            self.log.debug("Subscription removed!")
        except ResourceNotFoundError:
            # already deleted, ignore.
            self.log.debug("Subscription already removed!")
        self.log.info("Removed subscription %s", self.subscription_name)

    def _validate_params(self):
        error_message: str = ""

        if not self.request_queue_name:
            error_message += "Request queue name is required. "
        if not self.request_body_generator:
            error_message += "Request body creator is required. "
        if not self.reply_topic_name:
            error_message += "Reply topic name is required. "

        if error_message:
            self.log.error(error_message)
            raise TypeError(error_message)
