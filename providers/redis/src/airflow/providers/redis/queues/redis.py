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

import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
from airflow.providers.redis.triggers.redis_await_message import AwaitMessageTrigger

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger

# [START queue_regexp]
QUEUE_REGEXP = r"^redis\+pubsub://"
# [END queue_regexp]


class RedisPubSubMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for Redis integration with common-messaging.

    It uses the ``redis+pubsub://`` URI scheme for identifying Redis queues.

    **URI Format**:

    .. code-block:: text

        redis+pubsub://<host>:<port>/<channel_list>

    Where:

    * ``host``: Redis server hostname
    * ``port``: Redis server port
    * ``channel_list``: Comma-separated list of Redis channels to subscribe to

    **Examples**:

    .. code-block:: text

        redis+pubsub://localhost:6379/my_channel

    You can also provide ``channels`` directly in kwargs instead of in the URI.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        trigger = MessageQueueTrigger(queue="redis+pubsub://localhost:6379/test")

    For a complete example, see:
    :mod:`tests.system.redis.example_dag_message_queue_trigger`
    """

    def queue_matches(self, queue: str) -> bool:
        return bool(re.match(QUEUE_REGEXP, queue))

    def trigger_class(self) -> type[BaseEventTrigger]:
        return AwaitMessageTrigger  # type: ignore[return-value]

    def trigger_kwargs(self, queue: str, **kwargs) -> dict:
        # [START extract_channels]
        # Parse the queue URI
        parsed = urlparse(queue)
        # Extract channels (after host and port)
        # parsed.path starts with a '/', so strip it
        raw_channels = parsed.path.lstrip("/")
        channels = raw_channels.split(",") if raw_channels else []
        # [END extract_channels]

        if not channels and "channels" not in kwargs:
            raise ValueError(
                "channels is required in RedisPubSubMessageQueueProvider kwargs or provide them in the queue URI"
            )

        return {} if "channels" in kwargs else {"channels": channels}
