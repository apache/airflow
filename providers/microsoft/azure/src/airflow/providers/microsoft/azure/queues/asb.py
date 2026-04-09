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

from typing import TYPE_CHECKING

from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
from airflow.providers.microsoft.azure.triggers.message_bus import AzureServiceBusQueueTrigger

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger


class AzureServiceBusMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for Azure Service Bus integration with common-messaging.

    [START azure_servicebus_message_queue_provider_description]

    * It uses ``azure+servicebus`` as the scheme for identifying the provider.
    * For parameter definitions, take a look at
      :class:`~airflow.providers.microsoft.azure.triggers.message_bus.AzureServiceBusQueueTrigger`.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        trigger = MessageQueueTrigger(
            scheme="azure+servicebus",
            # AzureServiceBusQueueTrigger parameters
            queues=["my-queue"],
            azure_service_bus_conn_id="azure_service_bus_default",
            poll_interval=60,
        )

        asset = Asset(
            "asb_queue_asset",
            watchers=[AssetWatcher(name="asb_watcher", trigger=trigger)],
        )

    [END azure_servicebus_message_queue_provider_description]
    """

    scheme = "azure+servicebus"

    def trigger_class(self) -> type[BaseEventTrigger]:
        return AzureServiceBusQueueTrigger
