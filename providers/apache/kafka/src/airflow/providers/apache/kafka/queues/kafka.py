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

from airflow.providers.apache.kafka.triggers.await_message import AwaitMessageTrigger
from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger

# [START queue_regexp]
QUEUE_REGEXP = r"^kafka://"
# [END queue_regexp]


class KafkaMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for Apache Kafka integration with common-messaging.

    [START kafka_message_queue_provider_description]

    **Scheme-based Usage (Recommended)**:

    * It uses the ``kafka`` as scheme for identifying Kafka queues.
    * For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.triggers.await_message.AwaitMessageTrigger`.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        # New scheme-based approach
        trigger = MessageQueueTrigger(
            scheme="kafka",
            # Additional Kafka AwaitMessageTrigger parameters as needed
            topics=["my_topic"],
            apply_function="module.apply_function",
            bootstrap_servers="localhost:9092",
        )

        asset = Asset("kafka_queue_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)])

    **URI Format (Deprecated)**:

    .. warning::

        * The ``queue`` parameter is deprecated and will be removed in future versions.
        * Use the ``scheme`` parameter with appropriate keyword arguments instead.

    .. code-block:: text

        kafka://<broker>/<topic_list>

    Where:

    * ``broker``: Kafka brokers (hostname:port)
    * ``topic_list``: Comma-separated list of Kafka topics to consume messages from

    **Examples (Deprecated)**:

    * ``queue``: The Kafka queue URI, ``kafka://localhost:9092/my_topic`` in this case, which means the topic is passed by URI instead of the ``topics`` kwarg.
    * ``apply_function``: Function to process each Kafka message

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        # Deprecated queue URI approach - will be removed in future versions
        trigger = MessageQueueTrigger(
            queue="kafka://localhost:9092/test",
            # Additional Kafka AwaitMessageTrigger parameters as needed
            apply_function="module.apply_function",
        )

    For a complete example, see:
    :mod:`tests.system.common.messaging.kafka_message_queue_trigger`

    [END kafka_message_queue_provider_description]
    """

    scheme = "kafka"

    def queue_matches(self, queue: str) -> bool:
        return bool(re.match(QUEUE_REGEXP, queue))

    def trigger_class(self) -> type[BaseEventTrigger]:
        return AwaitMessageTrigger  # type: ignore[return-value]

    def trigger_kwargs(self, queue: str, **kwargs) -> dict:
        if "apply_function" not in kwargs:
            raise ValueError("apply_function is required in KafkaMessageQueueProvider kwargs")

        # [START extract_topics]
        # Parse the queue URI
        parsed = urlparse(queue)
        # Extract topics (after host list)
        # parsed.path starts with a '/', so strip it
        raw_topics = parsed.path.lstrip("/")
        topics = raw_topics.split(",") if raw_topics else []
        # [END extract_topics]

        if not topics and "topics" not in kwargs:
            raise ValueError(
                "topics is required in KafkaMessageQueueProvider kwargs or provide them in the queue URI"
            )

        return {} if "topics" in kwargs else {"topics": topics}
