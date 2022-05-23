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

from azure.servicebus.management import (
    QueueProperties, ServiceBusAdministrationClient, SubscriptionProperties)

from airflow.exceptions import AirflowBadRequest, AirflowException
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from airflow.providers.microsoft.azure.hooks.base_asb import BaseAzureServiceBusHook


class AzureServiceBusAdminClientHook(BaseAzureServiceBusHook):
    """
    Interacts with Azure ServiceBus management client
    and Use this client to create, update, list, and delete resources of a ServiceBus namespace.
    it uses the same azure service bus client connection inherits from the base class
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def get_conn(self) -> ServiceBusAdministrationClient:
        """Create and returns ServiceBusAdministration by using the connection string in connection details"""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson

        self.connection_string: str = extras.get('connection_string') or extras.get(
            'extra__azure_service_bus__connection_string'
        )
        return ServiceBusAdministrationClient.from_connection_string(self.connection_string)

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
            raise AirflowBadRequest("Queue name cannot be None.")

        try:
            with self.get_conn() as service_mgmt_conn:
                queue = service_mgmt_conn.create_queue(
                    queue_name,
                    max_delivery_count=max_delivery_count,
                    dead_lettering_on_message_expiration=dead_lettering_on_message_expiration,
                    enable_batched_operations=enable_batched_operations,
                )
                return queue
        except Exception as e:
            raise AirflowException(e)

    def delete_queue(self, queue_name: str) -> None:
        """
        Delete the queue by queue_name in service bus namespace

        :param queue_name: The name of the queue or a QueueProperties with name.
        """
        if queue_name is None:
            raise AirflowBadRequest("Queue name cannot be None.")

        try:
            with self.get_conn() as service_mgmt_conn:
                service_mgmt_conn.delete_queue(queue_name)
        except Exception as e:
            raise AirflowException(e)

    def create_subscription(self,
                            subscription_name: str,
                            topic_name: str,
                            max_delivery_count: int = 10,
                            dead_lettering_on_message_expiration: bool = True,
                            enable_batched_operations: bool = True) -> SubscriptionProperties:
        """
        Create a topic subscription entities under a ServiceBus Namespace.

        :param subscription_name: The subscription that will own the to-be-created rule.
        :param topic_name: The topic that will own the to-be-created subscription rule.
        :param max_delivery_count: The maximum delivery count. A message is automatically dead lettered
            after this number of deliveries. Default value is 10
        :param dead_lettering_on_message_expiration: A value that indicates whether this subscription
            has dead letter support when a message expires.
        :param enable_batched_operations: Value that indicates whether server-side batched operations are enabled.
        """
        if subscription_name is None:
            raise AirflowBadRequest("Subscription name cannot be None.")
        if topic_name is None:
            raise AirflowBadRequest("Topic name cannot be None.")
        try:
            with self.get_conn() as service_mgmt_conn:
                subscription = service_mgmt_conn.create_subscription(
                    topic_name,
                    subscription_name,
                    max_delivery_count=max_delivery_count,
                    dead_lettering_on_message_expiration=dead_lettering_on_message_expiration,
                    enable_batched_operations=enable_batched_operations
                )
                return subscription
        except ResourceExistsError as e:
            raise e

    def delete_subscription(self, subscription_name: str, topic_name: str) -> None:
        """
        Delete a topic subscription entities under a ServiceBus Namespace

        :param subscription_name: The subscription name that will own the rule in topic
        :param topic_name: The topic that will own the subscription rule.
        """
        if subscription_name is None:
            raise AirflowBadRequest("Subscription name cannot be None.")
        if topic_name is None:
            raise AirflowBadRequest("Topic name cannot be None.")
        try:
            with self.get_conn() as service_mgmt_conn:
                service_mgmt_conn.delete_subscription(topic_name, subscription_name)
        except ResourceNotFoundError as e:
            raise e

    def update_subscription(self,
                            subscription_name: str,
                            topic_name: str,
                            max_delivery_count: int,
                            dead_lettering_on_message_expiration: bool,
                            enable_batched_operations: bool
                            ) -> None:
        with self.get_conn() as service_mgmt_conn:
            try:
                subscription_prop = service_mgmt_conn.get_subscription(topic_name, subscription_name)
                if max_delivery_count:
                    subscription_prop.max_delivery_count = max_delivery_count
                if dead_lettering_on_message_expiration is not None:
                    subscription_prop.dead_lettering_on_message_expiration = dead_lettering_on_message_expiration
                if enable_batched_operations is not None:
                    subscription_prop.enable_batched_operations = enable_batched_operations
                # update by updating the properties in the model
                service_mgmt_conn.update_subscription(topic_name, subscription_prop)
            except ResourceNotFoundError as e:
                raise e
