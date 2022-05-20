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
from typing import Union

from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender
from azure.servicebus.exceptions import MessageSizeExceededError

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

        self.connection_string: str = extras.get('connection_string') or extras.get(
            'extra__azure_service_bus__connection_string'
        )

        return ServiceBusClient.from_connection_string(conn_str=self.connection_string, logging_enable=True)

    def send_message(self, queue_name: str, messages: Union[str, list], batch_message_flag: bool = False):
        """
        By using ServiceBusClient Send message(s) to a Service Bus Queue. By using
        batch_message_flag it enables and send message as batch message

        :param queue_name: The name of the queue or a QueueProperties with name.
        :param messages: Message which needs to be sent to the queue. It can be string or list of string.
        :param batch_message_flag: bool flag, can be set to True if message needs to be sent as batch message.
        """
        if queue_name is None:
            raise AirflowBadRequest("Queue name cannot be None.")
        if not messages:
            raise AirflowException("Message is empty.")
        service_bus_client = self.get_conn()
        try:
            with service_bus_client:
                sender = service_bus_client.get_queue_sender(queue_name=queue_name)
                with sender:
                    if isinstance(messages, str):
                        msg = ServiceBusMessage(messages)
                        sender.send_messages(msg)
                    else:
                        if not batch_message_flag:
                            self.send_list_messages(sender, messages)
                        else:
                            self.send_batch_message(sender, messages)
        except Exception as e:
            raise AirflowException(e)

    @staticmethod
    def send_list_messages(sender: ServiceBusSender, messages: list[str]):
        list_messages = [ServiceBusMessage(message) for message in messages]
        sender.send_messages(list_messages)

    @staticmethod
    def send_batch_message(sender: ServiceBusSender, messages: list[str]):
        batch_message = sender.create_message_batch()
        for message in messages:
            try:
                batch_message.add_message(
                    ServiceBusMessage(message))
            except MessageSizeExceededError as e:
                # ServiceBusMessageBatch object reaches max_size.
                # New ServiceBusMessageBatch object can be created here to send more data.
                raise AirflowException(e)
        sender.send_messages(batch_message)

    @staticmethod
    def send_service_bus_messages(sender: ServiceBusSender,
                                  message: Union[ServiceBusMessage, list(ServiceBusMessage)]):
        sender.send_messages(message)

    def receive_message(self, queue_name):
        """
        Receive a batch of messages at once in a specified Queue name

        :param queue_name: The name of the queue name or a QueueProperties with name.
        """
        if queue_name is None:
            raise AirflowBadRequest("Queue name cannot be None.")

        service_bus_client = self.get_conn()
        try:
            with service_bus_client:
                receiver = service_bus_client.get_queue_receiver(queue_name=queue_name)
                with receiver:
                    received_msgs = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                    for msg in received_msgs:
                        self.log.info(msg)
                        receiver.complete_message(msg)
        except Exception as e:
            raise AirflowException(e)
