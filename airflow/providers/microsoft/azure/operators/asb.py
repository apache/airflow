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

from typing import TYPE_CHECKING, List, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.asb import AdminClientHook, MessageHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureServiceBusCreateQueueOperator(BaseOperator):
    """
    Creates a Azure Service Bus queue under a Service Bus Namespace by using ServiceBusAdministrationClient

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
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.max_delivery_count = max_delivery_count
        self.dead_lettering_on_message_expiration = dead_lettering_on_message_expiration
        self.enable_batched_operations = enable_batched_operations
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """Creates Queue in Azure Service Bus namespace, by connecting to Service Bus Admin client in hook"""
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
    Send Message or batch message to the Service Bus queue

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
        message: Union[str, List[str]],
        batch: bool = False,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.batch = batch
        self.message = message
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """
        Sends Message to the specific queue in Service Bus namespace, by
        connecting to Service Bus  client
        """
        # Create the hook
        hook = MessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # send message
        hook.send_message(self.queue_name, self.message, self.batch)


class AzureServiceBusReceiveMessageOperator(BaseOperator):
    """
    Receive a batch of messages at once in a specified Queue name

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureServiceBusReceiveMessageOperator`

    :param queue_name: The name of the queue name or a QueueProperties with name.
    :param max_message_count: Maximum number of messages in the batch.
    :param max_wait_time: Maximum time to wait in seconds for the first message to arrive.
    :param azure_service_bus_conn_id: Reference to the
        :ref: `Azure Service Bus connection <howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        max_message_count: int = 10,
        max_wait_time: float = 5,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id
        self.max_message_count = max_message_count
        self.max_wait_time = max_wait_time

    def execute(self, context: "Context") -> None:
        """
        Receive Message in specific queue in Service Bus namespace,
        by connecting to Service Bus client
        """
        # Create the hook
        hook = MessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # Receive message
        hook.receive_message(
            self.queue_name, max_message_count=self.max_message_count, max_wait_time=self.max_wait_time
        )


class AzureServiceBusDeleteQueueOperator(BaseOperator):
    """
    Deletes the Queue in the Azure Service Bus namespace

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
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """Delete Queue in Service Bus namespace, by connecting to Service Bus Admin client"""
        # Create the hook
        hook = AdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # delete queue with name
        hook.delete_queue(self.queue_name)
