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

from azure.servicebus.management import QueueProperties, ServiceBusAdministrationClient

from airflow.exceptions import AirflowBadRequest, AirflowException
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

        self.connection_string = extras.get('connection_string') or extras.get(
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
