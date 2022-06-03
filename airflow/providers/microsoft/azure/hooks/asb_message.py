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
from typing import List, Optional, Union

from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender

from airflow.exceptions import AirflowBadRequest, AirflowException
from airflow.providers.microsoft.azure.hooks.base_asb import BaseAzureServiceBusHook


class ServiceBusMessageHook(BaseAzureServiceBusHook):
    """
    Interacts with ServiceBusClient and acts as a high level interface
    for getting ServiceBusSender and ServiceBusReceiver.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def get_conn(self) -> ServiceBusClient:
        """Create and returns ServiceBusClient by using the connection string in connection details"""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson

        self.connection_string = str(
            extras.get('connection_string') or extras.get('extra__azure_service_bus__connection_string')
        )

        return ServiceBusClient.from_connection_string(conn_str=self.connection_string, logging_enable=True)

    def send_message(
        self, queue_name: str, messages: Union[str, List[str]], batch_message_flag: bool = False
    ):
        """
        By using ServiceBusClient Send message(s) to a Service Bus Queue. By using
        batch_message_flag it enables and send message as batch message

        :param queue_name: The name of the queue or a QueueProperties with name.
        :param messages: Message which needs to be sent to the queue. It can be string or list of string.
        :param batch_message_flag: bool flag, can be set to True if message needs to be sent as batch message.
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
    def send_list_messages(sender: ServiceBusSender, messages: List[str]):
        list_messages = [ServiceBusMessage(message) for message in messages]
        sender.send_messages(list_messages)  # type: ignore[arg-type]

    @staticmethod
    def send_batch_message(sender: ServiceBusSender, messages: List[str]):
        batch_message = sender.create_message_batch()
        for message in messages:
            batch_message.add_message(ServiceBusMessage(message))
        sender.send_messages(batch_message)

    def receive_message(
        self, queue_name, max_message_count: Optional[int] = 1, max_wait_time: Optional[float] = None
    ):
        """
        Receive a batch of messages at once in a specified Queue name

        :param queue_name: The name of the queue name or a QueueProperties with name.
        :param max_message_count: Maximum number of messages in the batch.
        :param max_wait_time: Maximum time to wait in seconds for the first message to arrive.
        """
        if queue_name is None:
            raise ValueError("Queue name cannot be None.")

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
        max_message_count: Optional[int],
        max_wait_time: Optional[float],
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
            raise AirflowBadRequest("Subscription name cannot be None.")
        if topic_name is None:
            raise AirflowBadRequest("Topic name cannot be None.")
        try:
            with self.get_conn() as service_bus_client:
                subscription_receiver = service_bus_client.get_subscription_receiver(
                    topic_name, subscription_name
                )
                with subscription_receiver:
                    received_msgs = subscription_receiver.receive_messages(
                        max_message_count=max_message_count, max_wait_time=max_wait_time
                    )
                    for msg in received_msgs:
                        self.log.info(msg)
                        subscription_receiver.complete_message(msg)
        except Exception as e:
            raise AirflowException(e)
