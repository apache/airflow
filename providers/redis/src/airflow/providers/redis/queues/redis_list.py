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
from airflow.providers.redis.triggers.redis_list_await_message import AwaitMessageFromListTrigger

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger


class RedisListMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for Redis Lists integration with common-messaging.

    [START redis_list_message_queue_provider_description]

    * It uses ``redis+list`` as scheme for identifying Redis list-based queues.
    * For parameter definitions take a look at :class:`~airflow.providers.redis.triggers.redis_list_await_message.AwaitMessageFromListTrigger`.

    Unlike ``redis+pubsub``, list-based queues provide:

    * **Durability**: Messages persist in the list until consumed (no message loss).
    * **Exactly-once delivery**: BRPOP atomically removes and returns the message.
    * **Priority queues**: Multiple lists are checked in order.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        trigger = MessageQueueTrigger(
            scheme="redis+list",
            lists=["my_queue"],
            redis_conn_id="redis_default",
        )

        asset = Asset("redis_list_asset", watchers=[AssetWatcher(name="redis_watcher", trigger=trigger)])

    [END redis_list_message_queue_provider_description]
    """

    scheme = "redis+list"

    def trigger_class(self) -> type[BaseEventTrigger]:
        return AwaitMessageFromListTrigger  # type: ignore[return-value]
