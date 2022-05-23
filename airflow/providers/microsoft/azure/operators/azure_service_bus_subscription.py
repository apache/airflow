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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.asb_admin_client import AzureServiceBusAdminClientHook
from airflow.providers.microsoft.azure.hooks.asb_message import ServiceBusMessageHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ASBCreateSubscriptionOperator(BaseOperator):
    """
    Creates an Azure ServiceBus Topic Subscription under a ServiceBus Namespace
    by using ServiceBusAdministrationClient

    :param topic_name: The topic that will own the to-be-created subscription.
    :param subscription_name: Name of the subscription that need to be created
    :param max_delivery_count: The maximum delivery count. A message is automatically dead lettered
        after this number of deliveries. Default value is 10
    :param dead_lettering_on_message_expiration: A value that indicates whether this subscription
        has dead letter support when a message expires.
    :param enable_batched_operations: Value that indicates whether server-side batched operations are enabled.
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
        max_delivery_count: int = 10,
        dead_lettering_on_message_expiration: bool = True,
        enable_batched_operations: bool = True,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.max_delivery_count = max_delivery_count
        self.dead_lettering_on_message_expiration = dead_lettering_on_message_expiration
        self.enable_batched_operations = enable_batched_operations
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """Creates Subscription in Service Bus namespace, by connecting to Service Bus Admin client"""
        # Create the hook
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # create subscription with name
        subscription = hook.create_subscription(
            self.subscription_name,
            self.topic_name,
            self.max_delivery_count,
            self.dead_lettering_on_message_expiration,
            self.enable_batched_operations
        )
        self.log.info("Created Queue %s", self.cluster_identifier)
        self.log.info(subscription)


class ASBUpdateSubscriptionOperator(BaseOperator):
    """
    Update an Azure ServiceBus Topic Subscription under a ServiceBus Namespace
    by using ServiceBusAdministrationClient

    :param topic_name: The topic that will own the to-be-created subscription.
    :param subscription_name: Name of the subscription that need to be created.
    :param max_delivery_count: The maximum delivery count. A message is automatically dead lettered
        after this number of deliveries. Default value is 10.
    :param dead_lettering_on_message_expiration: A value that indicates whether this subscription
        has dead letter support when a message expires.
    :param enable_batched_operations: Value that indicates whether server-side batched operations are enabled.
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("topic_name","subscription_name")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        subscription_name: str,
        max_delivery_count: int = None,
        dead_lettering_on_message_expiration: bool = None,
        enable_batched_operations: bool = None,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.max_delivery_count = max_delivery_count
        self.dead_lettering_on_message_expiration = dead_lettering_on_message_expiration
        self.enable_batched_operations = enable_batched_operations
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """Updates Subscription properties, by connecting to Service Bus Admin client"""
        # Create the hook
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # Update subscription with name
        hook.update_subscription(
            self.subscription_name,
            self.topic_name,
            self.max_delivery_count,
            self.dead_lettering_on_message_expiration,
            self.enable_batched_operations
        )


class ASBReceiveSubscriptionMessageOperator(BaseOperator):
    """
    Receive a messages for the specific subscription under the topic.

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
    """

    template_fields: Sequence[str] = ("topic_name", "subscription_name")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        topic_name: str,
        subscription_name: str,
        max_message_count: int = 1,
        max_wait_time: float = 5,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name,
        self.subscription_name = subscription_name,
        self.max_message_count = max_message_count,
        self.max_wait_time = max_wait_time,
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """
        Receive Message in specific queue in Service Bus namespace,
        by connecting to Service Bus client
        """
        # Create the hook
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # Receive message
        hook.receive_subscription_message(
            self.topic_name,
            self.subscription_name,
            self.max_message_count,
            self.max_wait_time
        )


class ASBDeleteSubscriptionOperator(BaseOperator):
    """
    Deletes the topic subscription in the Azure ServiceBus namespace

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
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """Delete topic subscription in Service Bus namespace, by connecting to Service Bus Admin client"""
        # Create the hook
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # delete subscription with name
        hook.delete_subscription(self.subscription_name, self.topic_name)
