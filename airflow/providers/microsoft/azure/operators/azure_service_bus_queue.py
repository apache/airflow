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

from typing import TYPE_CHECKING, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.asb_admin_client import AzureServiceBusAdminClientHook
from airflow.providers.microsoft.azure.hooks.asb_message import ServiceBusMessageHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureServiceBusCreateQueueOperator(BaseOperator):
    """
    Creates a Azure ServiceBus queue under a ServiceBus Namespace by using ServiceBusAdministrationClient

    :param queue_name: The name of the queue. should be unique.
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
        """Creates Queue in Service Bus namespace, by connecting to Service Bus Admin client"""
        # Create the hook
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # create queue with name
        queue = hook.create_queue(
            self.queue_name,
            self.max_delivery_count,
            self.dead_lettering_on_message_expiration,
            self.enable_batched_operations,
        )
        self.log.info("Created Queue %s", queue.name)
        self.log.info(queue)


class AzureServiceBusSendMessageOperator(BaseOperator):
    """
    Send Message or batch message to the service bus queue

    :param message: Message which needs to be sent to the queue. It can be string or list of string.
    :param batch: Its boolean flag by default it is set to False, if the message needs to be sent
        as batch message it can be set to True.
    :param azure_service_bus_conn_id: Reference to the
        :ref: `Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name", "message")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        message: Union[str, list],
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
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # send message
        hook.send_message(self.queue_name, self.message, self.batch)


class AzureServiceBusReceiveMessageOperator(BaseOperator):
    """
    Receive a batch of messages at once in a specified Queue name

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
        """
        Receive Message in specific queue in Service Bus namespace,
        by connecting to Service Bus client
        """
        # Create the hook
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # Receive message
        hook.receive_message(self.queue_name)


class AzureServiceBusDeleteQueueOperator(BaseOperator):
    """
    Deletes the Queue in the Azure ServiceBus namespace

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
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # delete queue with name
        hook.delete_queue(self.queue_name)
