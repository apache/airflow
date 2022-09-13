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

from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender
from azure.servicebus.management import QueueProperties, ServiceBusAdministrationClient

from airflow.hooks.base import BaseHook


class BaseAzureServiceBusHook(BaseHook):
    """
    BaseAzureServiceBusHook class to create session and create connection using connection string

    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    conn_name_attr = 'azure_service_bus_conn_id'
    default_conn_name = 'azure_service_bus_default'
    conn_type = 'azure_service_bus'
    hook_name = 'Azure Service Bus'

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'host', 'extra', 'login', 'password'],
            "relabeling": {'schema': 'Connection String'},
            "placeholders": {
                'schema': 'Endpoint=sb://<Resource group>.servicebus.windows.net/;SharedAccessKeyName=<AccessKeyName>;SharedAccessKey=<SharedAccessKey>',  # noqa
            },
        }

    def __init__(self, azure_service_bus_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_service_bus_conn_id

    def get_conn(self):
        raise NotImplementedError


class AdminClientHook(BaseAzureServiceBusHook):
    """
    Interacts with ServiceBusAdministrationClient client
    to create, update, list, and delete resources of a
    Service Bus namespace.  This hook uses the same Azure Service Bus client connection inherited
    from the base class
    """

    def get_conn(self) -> ServiceBusAdministrationClient:
        """
        Create and returns ServiceBusAdministrationClient by using the connection
        string in connection details
        """
        conn = self.get_connection(self.conn_id)

        connection_string: str = str(conn.schema)
        return ServiceBusAdministrationClient.from_connection_string(connection_string)

    def create_queue(
        self,
        queue_name: str,
        max_delivery_count: int = 10,
        dead_lettering_on_message_expiration: bool = True,
        enable_batched_operations: bool = True,
    ) -> QueueProperties:
        """
        Create Queue by connecting to service Bus Admin client return the QueueProperties

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
        Delete the queue by queue_name in service bus namespace

        :param queue_name: The name of the queue or a QueueProperties with name.
        """
        if queue_name is None:
            raise TypeError("Queue name cannot be None.")

        with self.get_conn() as service_mgmt_conn:
            service_mgmt_conn.delete_queue(queue_name)

    def delete_subscription(self, subscription_name: str, topic_name: str) -> None:
        """
        Delete a topic subscription entities under a ServiceBus Namespace

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
    Interacts with ServiceBusClient and acts as a high level interface
    for getting ServiceBusSender and ServiceBusReceiver.
    """

    def get_conn(self) -> ServiceBusClient:
        """Create and returns ServiceBusClient by using the connection string in connection details"""
        conn = self.get_connection(self.conn_id)
        connection_string: str = str(conn.schema)

        self.log.info("Create and returns ServiceBusClient")
        return ServiceBusClient.from_connection_string(conn_str=connection_string, logging_enable=True)

    def send_message(self, queue_name: str, messages: str | list[str], batch_message_flag: bool = False):
        """
        By using ServiceBusClient Send message(s) to a Service Bus Queue. By using
        batch_message_flag it enables and send message as batch message

        :param queue_name: The name of the queue or a QueueProperties with name.
        :param messages: Message which needs to be sent to the queue. It can be string or list of string.
        :param batch_message_flag: bool flag, can be set to True if message needs to be
            sent as batch message.
        """
        if queue_name is None:
            raise TypeError("Queue name cannot be None.")
        if not messages:
            raise ValueError("Messages list cannot be empty.")
        with self.get_conn() as service_bus_client, service_bus_client.get_queue_sender(
            queue_name=queue_name
        ) as sender:
            with sender:
                if isinstance(messages, str):
                    if not batch_message_flag:
                        msg = ServiceBusMessage(messages)
                        sender.send_messages(msg)
                    else:
                        self.send_batch_message(sender, [messages])
                else:
                    if not batch_message_flag:
                        self.send_list_messages(sender, messages)
                    else:
                        self.send_batch_message(sender, messages)

    @staticmethod
    def send_list_messages(sender: ServiceBusSender, messages: list[str]):
        list_messages = [ServiceBusMessage(message) for message in messages]
        sender.send_messages(list_messages)  # type: ignore[arg-type]

    @staticmethod
    def send_batch_message(sender: ServiceBusSender, messages: list[str]):
        batch_message = sender.create_message_batch()
        for message in messages:
            batch_message.add_message(ServiceBusMessage(message))
        sender.send_messages(batch_message)

    def receive_message(
        self, queue_name, max_message_count: int | None = 1, max_wait_time: float | None = None
    ):
        """
        Receive a batch of messages at once in a specified Queue name

        :param queue_name: The name of the queue name or a QueueProperties with name.
        :param max_message_count: Maximum number of messages in the batch.
        :param max_wait_time: Maximum time to wait in seconds for the first message to arrive.
        """
        if queue_name is None:
            raise TypeError("Queue name cannot be None.")

        with self.get_conn() as service_bus_client, service_bus_client.get_queue_receiver(
            queue_name=queue_name
        ) as receiver:
            with receiver:
                received_msgs = receiver.receive_messages(
                    max_message_count=max_message_count, max_wait_time=max_wait_time
                )
                for msg in received_msgs:
                    self.log.info(msg)
                    receiver.complete_message(msg)

    def receive_subscription_message(
        self,
        topic_name: str,
        subscription_name: str,
        max_message_count: int | None,
        max_wait_time: float | None,
    ):
        """
        Receive a batch of subscription message at once. This approach is optimal if you wish
        to process multiple messages simultaneously, or perform an ad-hoc receive as a single call.

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
        with self.get_conn() as service_bus_client, service_bus_client.get_subscription_receiver(
            topic_name, subscription_name
        ) as subscription_receiver:
            with subscription_receiver:
                received_msgs = subscription_receiver.receive_messages(
                    max_message_count=max_message_count, max_wait_time=max_wait_time
                )
                for msg in received_msgs:
                    self.log.info(msg)
                    subscription_receiver.complete_message(msg)
