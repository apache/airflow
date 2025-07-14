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

from asgiref.sync import sync_to_async

from airflow.providers.microsoft.azure.hooks.asb import MessageHook
from airflow.triggers.base import BaseTrigger
from airflow.utils.context import Context


class AzureServiceBusQueueTrigger(BaseTrigger):
    default_conn_name = "azure_service_bus_default"
    default_max_message_count = 1
    default_max_wait_time = None
    default_context = Context()

    def __init__(
        self,
        queues: list[str],
        context: Context = default_context,
        azure_service_bus_conn_id: str = default_conn_name,
        max_message_count: int | None = default_max_message_count,
        max_wait_time: float | None = default_max_wait_time,
    ) -> None:
        self.connection_id = azure_service_bus_conn_id
        self.queues = queues
        self.context = context
        self.max_message_count = max_message_count
        self.max_wait_time = max_wait_time
        self.message_hook = MessageHook(azure_service_bus_conn_id=azure_service_bus_conn_id)

    def serialize(self) -> tuple[str, dict[str, any]]:
        return {
            "class_name": self.__class__.__name__,
            "config": {
                "azure_service_bus_conn_id": self.connection_id,
                "queries": self.queries,
                "context": self.context,
                "max_message_count": self.max_message_count,
                "max_wait_time": self.max_wait_time,
            },
        }

    async def run(self):
        receive_message_async = sync_to_async(self.message_hook.receive_message)
        for queue in self.queues:
            pass


class AzureServiceBusSubscriptionTrigger(BaseTrigger):
    default_conn_name = "azure_service_bus_default"
    default_max_message_count = 1
    default_max_wait_time = None
    default_context = Context()

    def __init__(
        self,
        queues: list[str],
        context: Context = default_context,
        azure_service_bus_conn_id: str = default_conn_name,
        max_message_count: int | None = default_max_message_count,
        max_wait_time: float | None = default_max_wait_time,
    ) -> None:
        self.connection_id = azure_service_bus_conn_id
        self.queues = queues
        self.context = context
        self.max_message_count = max_message_count
        self.max_wait_time = max_wait_time
        self.message_hook = MessageHook(azure_service_bus_conn_id=azure_service_bus_conn_id)

    def serialize(self) -> tuple[str, dict[str, any]]:
        return {
            "class_name": self.__class__.__name__,
            "config": {
                "azure_service_bus_conn_id": self.connection_id,
                "queries": self.queries,
                "context": self.context,
                "max_message_count": self.max_message_count,
                "max_wait_time": self.max_wait_time,
            },
        }
