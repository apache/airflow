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

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from azure.core.exceptions import ResourceNotFoundError
from azure.servicebus import (
    ServiceBusClient,
    ServiceBusMessage,
    ServiceBusReceivedMessage,
    ServiceBusReceiver,
    ServiceBusSender,
)
from azure.servicebus.management import (
    AuthorizationRule,
    CorrelationRuleFilter,
    QueueProperties,
    ServiceBusAdministrationClient,
    SqlRuleFilter,
    SubscriptionProperties,
)

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    import datetime

    from azure.identity import DefaultAzureCredential

    from airflow.sdk import Context

    MessageCallback = Callable[[ServiceBusMessage, Context], None]


class BaseAzureServiceBusHook(BaseHook):
    """
    BaseAzureServiceBusHook class to create session and create connection using connection string.

    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    conn_name_attr = "azure_service_bus_conn_id"
    default_conn_name = "azure_service_bus_default"
    conn_type = "azure_service_bus"
    hook_name = "Azure Service Bus"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "fully_qualified_namespace": StringField(
                lazy_gettext("Fully Qualified Namespace"), widget=BS3TextFieldWidget()
            ),
            "credential": PasswordField(lazy_gettext("Credential"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["port", "host", "extra", "login", "password"],
            "relabeling": {"schema": "Connection String"},
            "placeholders": {
                "fully_qualified_namespace": (
                    "<Resource group>.servicebus.windows.net (for Azure AD authenticaltion)"
                ),
                "credential": "credential",
                "schema": "Endpoint=sb://<Resource group>.servicebus.windows.net/;SharedAccessKeyName=<AccessKeyName>;SharedAccessKey=<SharedAccessKey>",
            },
        }

    def __init__(self, azure_service_bus_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_service_bus_conn_id

    def get_conn(self):
        raise NotImplementedError

    def _get_field(self, extras: dict, field_name: str) -> str:
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=field_name,
        )


class AdminClientHook(BaseAzureServiceBusHook):
    """
    Interact with the ServiceBusAdministrationClient.

    This can create, update, list, and delete resources of a Service Bus
    namespace. This hook uses the same Azure Service Bus client connection
    inherited from the base class.
    """

    def get_conn(self) -> ServiceBusAdministrationClient:
        """
        Create a ServiceBusAdministrationClient instance.

        This uses the connection string in connection details.
        """
        conn = self.get_connection(self.conn_id)
        connection_string: str = str(conn.schema)
        if connection_string:
            client = ServiceBusAdministrationClient.from_connection_string(connection_string)
        else:
            extras = conn.extra_dejson
            credential: str | DefaultAzureCredential = self._get_field(extras=extras, field_name="credential")
            fully_qualified_namespace = self._get_field(extras=extras, field_name="fully_qualified_namespace")
            if not credential:
                managed_identity_client_id = self._get_field(
                    extras=extras, field_name="managed_identity_client_id"
                )
                workload_identity_tenant_id = self._get_field(
                    extras=extras, field_name="workload_identity_tenant_id"
                )
                credential = get_sync_default_azure_credential(
                    managed_identity_client_id=managed_identity_client_id,
                    workload_identity_tenant_id=workload_identity_tenant_id,
                )
            client = ServiceBusAdministrationClient(
                fully_qualified_namespace=fully_qualified_namespace,
                credential=credential,  # type: ignore[arg-type]
            )
        self.log.info("Create and returns ServiceBusAdministrationClient")
        return client

    def create_queue(
        self,
        queue_name: str,
        max_delivery_count: int = 10,
        dead_lettering_on_message_expiration: bool = True,
        enable_batched_operations: bool = True,
    ) -> QueueProperties:
        """
        Create Queue by connecting to service Bus Admin client return the QueueProperties.

        :param queue_name: The name of the queue or a QueueProperties with name.
        :param max_delivery_count: The maximum delivery count. A message is automatically
            dead lettered after this number of deliveries. Default value is 10..
        :param dead_lettering_on_message_expiration: A value that indicates whether this subscription has
            dead letter support when a message expires.
        :param enable_batched_operations: Value that indicates whether server-side batched
            operations are enabled.
        """
        if queue_name is None:
            raise TypeError("Queue name cannot be None.")

        with self.get_conn() as service_mgmt_conn:
            queue = service_mgmt_conn.create_queue(
                queue_name,
                max_delivery_count=max_delivery_count,
                dead_lettering_on_message_expiration=dead_lettering_on_message_expiration,
                enable_batched_operations=enable_batched_operations,
            )
            return queue

    def delete_queue(self, queue_name: str) -> None:
        """
        Delete the queue by queue_name in service bus namespace.

        :param queue_name: The name of the queue or a QueueProperties with name.
        """
        if queue_name is None:
            raise TypeError("Queue name cannot be None.")

        with self.get_conn() as service_mgmt_conn:
            service_mgmt_conn.delete_queue(queue_name)

    def create_topic(
        self,
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
    ) -> str:
        """
        Create a topic by connecting to service Bus Admin client.

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
        if topic_name is None:
            raise TypeError("Topic name cannot be None.")

        with self.get_conn() as service_mgmt_conn:
            try:
                topic_properties = service_mgmt_conn.get_topic(topic_name)
            except ResourceNotFoundError:
                topic_properties = None
            if topic_properties and topic_properties.name == topic_name:
                self.log.info("Topic name already exists")
                return topic_properties.name
            topic = service_mgmt_conn.create_topic(
                topic_name=topic_name,
                default_message_time_to_live=default_message_time_to_live,
                max_size_in_megabytes=max_size_in_megabytes,
                requires_duplicate_detection=requires_duplicate_detection,
                duplicate_detection_history_time_window=duplicate_detection_history_time_window,
                enable_batched_operations=enable_batched_operations,
                size_in_bytes=size_in_bytes,
                filtering_messages_before_publishing=filtering_messages_before_publishing,
                authorization_rules=authorization_rules,
                support_ordering=support_ordering,
                auto_delete_on_idle=auto_delete_on_idle,
                enable_partitioning=enable_partitioning,
                enable_express=enable_express,
                user_metadata=user_metadata,
                max_message_size_in_kilobytes=max_message_size_in_kilobytes,
            )
            self.log.info("Created Topic %s", topic.name)
            return topic.name

    def create_subscription(
        self,
        topic_name: str,
        subscription_name: str,
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
    ) -> SubscriptionProperties:
        """
        Create a subscription with specified name on a topic and return the SubscriptionProperties for it.

        An optional filter_rule can be provided to filter messages based on their properties. In particular,
        the correlation ID filter can be used to pair up replies to requests.

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
        """
        if subscription_name is None:
            raise TypeError("Subscription name cannot be None.")
        if topic_name is None:
            raise TypeError("Topic name cannot be None.")

        with self.get_conn() as connection:
            # create subscription with name
            subscription = connection.create_subscription(
                topic_name=topic_name,
                subscription_name=subscription_name,
                lock_duration=lock_duration,
                requires_session=requires_session,
                default_message_time_to_live=default_message_time_to_live,
                dead_lettering_on_message_expiration=dead_lettering_on_message_expiration,
                dead_lettering_on_filter_evaluation_exceptions=dead_lettering_on_filter_evaluation_exceptions,
                max_delivery_count=max_delivery_count,
                enable_batched_operations=enable_batched_operations,
                forward_to=forward_to,
                user_metadata=user_metadata,
                forward_dead_lettered_messages_to=forward_dead_lettered_messages_to,
                auto_delete_on_idle=auto_delete_on_idle,
            )

            if filter_rule:
                # remove default rule (which accepts all messages)
                try:
                    connection.delete_rule(topic_name, subscription_name, "$Default")
                except ResourceNotFoundError:
                    # as long as it is gone :)
                    self.log.debug("Could not find default rule '$Default' to delete; ignoring error.")

                # add a rule to filter with the filter rule passed in
                rule_name = filter_rule_name if filter_rule_name else "rule" + str(uuid4())
                connection.create_rule(topic_name, subscription_name, rule_name, filter=filter_rule)
                self.log.debug(
                    "Created rule %s for subscription %s on topic %s",
                    rule_name,
                    subscription_name,
                    topic_name,
                )

            return subscription

    def update_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        max_delivery_count: int | None = None,
        dead_lettering_on_message_expiration: bool | None = None,
        enable_batched_operations: bool | None = None,
    ) -> None:
        """
        Update an Azure ServiceBus Topic Subscription under a ServiceBus Namespace.

        :param topic_name: The topic that will own the to-be-created subscription.
        :param subscription_name: Name of the subscription that need to be created.
        :param max_delivery_count: The maximum delivery count. A message is automatically dead lettered
            after this number of deliveries. Default value is 10.
        :param dead_lettering_on_message_expiration: A value that indicates whether this subscription
            has dead letter support when a message expires.
        :param enable_batched_operations: Value that indicates whether server-side batched
            operations are enabled.
        """
        with self.get_conn() as service_mgmt_conn:
            subscription_prop = service_mgmt_conn.get_subscription(topic_name, subscription_name)
            if max_delivery_count:
                subscription_prop.max_delivery_count = max_delivery_count
            if dead_lettering_on_message_expiration is not None:
                subscription_prop.dead_lettering_on_message_expiration = dead_lettering_on_message_expiration
            if enable_batched_operations is not None:
                subscription_prop.enable_batched_operations = enable_batched_operations
            # update by updating the properties in the model
            service_mgmt_conn.update_subscription(topic_name, subscription_prop)
            updated_subscription = service_mgmt_conn.get_subscription(topic_name, subscription_name)
            self.log.info("Subscription Updated successfully %s", updated_subscription.name)

    def delete_subscription(self, subscription_name: str, topic_name: str) -> None:
        """
        Delete a topic subscription entities under a ServiceBus Namespace.

        :param subscription_name: The subscription name that will own the rule in topic
        :param topic_name: The topic that will own the subscription rule.
        """
        if subscription_name is None:
            raise TypeError("Subscription name cannot be None.")
        if topic_name is None:
            raise TypeError("Topic name cannot be None.")

        with self.get_conn() as service_mgmt_conn:
            self.log.info("Deleting Subscription %s", subscription_name)
            service_mgmt_conn.delete_subscription(topic_name, subscription_name)


class MessageHook(BaseAzureServiceBusHook):
    """
    Interact with ServiceBusClient.

    This acts as a high level interface for getting ServiceBusSender and ServiceBusReceiver.
    """

    def get_conn(self) -> ServiceBusClient:
        """Create and returns ServiceBusClient by using the connection string in connection details."""
        conn = self.get_connection(self.conn_id)
        connection_string: str = str(conn.schema)
        if connection_string:
            client = ServiceBusClient.from_connection_string(connection_string, logging_enable=True)
        else:
            extras = conn.extra_dejson
            credential: str | DefaultAzureCredential = self._get_field(extras=extras, field_name="credential")
            fully_qualified_namespace = self._get_field(extras=extras, field_name="fully_qualified_namespace")
            if not credential:
                managed_identity_client_id = self._get_field(
                    extras=extras, field_name="managed_identity_client_id"
                )
                workload_identity_tenant_id = self._get_field(
                    extras=extras, field_name="workload_identity_tenant_id"
                )
                credential = get_sync_default_azure_credential(
                    managed_identity_client_id=managed_identity_client_id,
                    workload_identity_tenant_id=workload_identity_tenant_id,
                )
            client = ServiceBusClient(
                fully_qualified_namespace=fully_qualified_namespace,
                credential=credential,  # type: ignore[arg-type]
            )

        self.log.info("Create and returns ServiceBusClient")
        return client

    def send_message(
        self,
        queue_name: str,
        messages: str | list[str],
        batch_message_flag: bool = False,
        message_id: str | None = None,
        reply_to: str | None = None,
        message_headers: dict[str | bytes, int | float | bytes | bool | str | UUID] | None = None,
    ):
        """
        Use ServiceBusClient Send to send message(s) to a Service Bus Queue.

        By using ``batch_message_flag``, it enables and send message as batch message.

        :param queue_name: The name of the queue or a QueueProperties with name.
        :param messages: Message which needs to be sent to the queue. It can be string or list of string.
        :param batch_message_flag: bool flag, can be set to True if message needs to be
            sent as batch message.
        :param message_id: Message ID to set on message being sent to the queue. Please note, message_id may only be
            set when a single message is sent.
        :param reply_to: Reply to which needs to be sent to the queue.
        :param message_headers: Headers to add to the message's application_properties field for Azure Service Bus.
        """
        if queue_name is None:
            raise TypeError("Queue name cannot be None.")
        if not messages:
            raise ValueError("Messages list cannot be empty.")
        if message_id and isinstance(messages, list) and len(messages) != 1:
            raise TypeError("Message ID can only be set if a single message is sent.")
        with (
            self.get_conn() as service_bus_client,
            service_bus_client.get_queue_sender(queue_name=queue_name) as sender,
            sender,
        ):
            message_creator = lambda msg_body: ServiceBusMessage(
                msg_body, message_id=message_id, reply_to=reply_to, application_properties=message_headers
            )
            message_list = [messages] if isinstance(messages, str) else messages
            if not batch_message_flag:
                self.send_list_messages(sender, message_list, message_creator)
            else:
                self.send_batch_message(sender, message_list, message_creator)

    @staticmethod
    def send_list_messages(
        sender: ServiceBusSender,
        messages: list[str],
        message_creator: Callable[[str], ServiceBusMessage],
    ):
        list_messages = [message_creator(body) for body in messages]
        sender.send_messages(list_messages)

    @staticmethod
    def send_batch_message(
        sender: ServiceBusSender,
        messages: list[str],
        message_creator: Callable[[str], ServiceBusMessage],
    ):
        batch_message = sender.create_message_batch()
        for message in messages:
            batch_message.add_message(message_creator(message))
        sender.send_messages(batch_message)

    def receive_message(
        self,
        queue_name: str,
        context: Context,
        max_message_count: int | None = 1,
        max_wait_time: float | None = None,
        message_callback: MessageCallback | None = None,
    ):
        """
        Receive a batch of messages at once in a specified Queue name.

        :param queue_name: The name of the queue name or a QueueProperties with name.
        :param max_message_count: Maximum number of messages in the batch.
        :param max_wait_time: Maximum time to wait in seconds for the first message to arrive.
        :param message_callback: Optional callback to process each message. If not provided, then
            the message will be logged and completed. If provided, and throws an exception, the
            message will be abandoned for future redelivery.
        """
        if queue_name is None:
            raise TypeError("Queue name cannot be None.")

        with (
            self.get_conn() as service_bus_client,
            service_bus_client.get_queue_receiver(queue_name=queue_name) as receiver,
            receiver,
        ):
            received_msgs = receiver.receive_messages(
                max_message_count=max_message_count, max_wait_time=max_wait_time
            )
            for msg in received_msgs:
                self._process_message(msg, context, message_callback, receiver)

    def receive_subscription_message(
        self,
        topic_name: str,
        subscription_name: str,
        context: Context,
        max_message_count: int | None,
        max_wait_time: float | None,
        message_callback: MessageCallback | None = None,
    ):
        """
        Receive a batch of subscription message at once.

        This approach is optimal if you wish to process multiple messages
        simultaneously, or perform an ad-hoc receive as a single call.

        :param subscription_name: The subscription name that will own the rule in topic
        :param topic_name: The topic that will own the subscription rule.
        :param max_message_count: Maximum number of messages in the batch.
            Actual number returned will depend on prefetch_count and incoming stream rate.
            Setting to None will fully depend on the prefetch config. The default value is 1.
        :param max_wait_time: Maximum time to wait in seconds for the first message to arrive. If no
            messages arrive, and no timeout is specified, this call will not return until the
            connection is closed. If specified, an no messages arrive within the timeout period,
            an empty list will be returned.
        """
        if subscription_name is None:
            raise TypeError("Subscription name cannot be None.")
        if topic_name is None:
            raise TypeError("Topic name cannot be None.")
        with (
            self.get_conn() as service_bus_client,
            service_bus_client.get_subscription_receiver(
                topic_name, subscription_name
            ) as subscription_receiver,
            subscription_receiver,
        ):
            received_msgs = subscription_receiver.receive_messages(
                max_message_count=max_message_count, max_wait_time=max_wait_time
            )
            for msg in received_msgs:
                self._process_message(msg, context, message_callback, subscription_receiver)

    def read_message(
        self,
        queue_name: str,
        max_wait_time: float | None = None,
    ) -> ServiceBusReceivedMessage | None:
        """
        Read a single message from a Service Bus queue without callback processing.

        :param queue_name: The name of the queue to read from.
        :param max_wait_time: Maximum time to wait for messages (seconds).
        :return: The received message or None if no message is available.
        """
        with (
            self.get_conn() as service_bus_client,
            service_bus_client.get_queue_receiver(queue_name=queue_name) as receiver,
            receiver,
        ):
            received_msgs = receiver.receive_messages(max_message_count=1, max_wait_time=max_wait_time)
            if received_msgs:
                msg = received_msgs[0]
                receiver.complete_message(msg)
                return msg
            return None

    def read_subscription_message(
        self,
        topic_name: str,
        subscription_name: str,
        max_wait_time: float | None = None,
    ) -> ServiceBusReceivedMessage | None:
        """
        Read a single message from a Service Bus topic subscription without callback processing.

        :param topic_name: The name of the topic.
        :param subscription_name: The name of the subscription.
        :param max_wait_time: Maximum time to wait for messages (seconds).
        :return: The received message or None if no message is available.
        """
        with (
            self.get_conn() as service_bus_client,
            service_bus_client.get_subscription_receiver(
                topic_name, subscription_name
            ) as subscription_receiver,
            subscription_receiver,
        ):
            received_msgs = subscription_receiver.receive_messages(
                max_message_count=1, max_wait_time=max_wait_time
            )
            if received_msgs:
                msg = received_msgs[0]
                subscription_receiver.complete_message(msg)
                return msg
            return None

    def _process_message(
        self,
        msg: ServiceBusReceivedMessage,
        context: Context,
        message_callback: MessageCallback | None,
        receiver: ServiceBusReceiver,
    ):
        """
        Process the message by calling the message_callback or logging the message.

        :param msg: The message to process.
        :param message_callback: Optional callback to process each message. If not provided, then
            the message will be logged and completed. If provided, and throws an exception, the
            message will be abandoned for future redelivery.
        :param receiver: The receiver that received the message.
        """
        if message_callback is None:
            self.log.info(msg)
            receiver.complete_message(msg)
        else:
            try:
                message_callback(msg, context)
            except Exception as e:
                self.log.error("Error processing message: %s", e)
                receiver.abandon_message(msg)
                raise e
            else:
                receiver.complete_message(msg)
