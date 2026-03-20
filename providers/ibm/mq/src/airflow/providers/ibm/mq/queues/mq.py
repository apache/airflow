#
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
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
from airflow.providers.ibm.mq.triggers.mq import AwaitMessageTrigger

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger

# [START queue_regexp]
QUEUE_REGEXP = r"^mq://"
# [END queue_regexp]


class IBMMQMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for IBM MQ integration with common-messaging.

    [START ibmmq_message_queue_provider_description]

    * It uses ``mq`` as scheme for identifying IBM MQ queues.
    * For parameter definitions take a look at
      :class:`~airflow.providers.ibm.mq.triggers.mq.AwaitMessageTrigger`.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        trigger = MessageQueueTrigger(
            queue="mq://mq_default/MY.QUEUE.NAME",
        )

        asset = Asset("mq_topic_asset", watchers=[AssetWatcher(name="mq_watcher", trigger=trigger)])

    [END ibmmq_message_queue_provider_description]
    """

    scheme = "mq"

    def queue_matches(self, queue: str) -> bool:
        return bool(re.match(QUEUE_REGEXP, queue))

    def trigger_class(self) -> type[BaseEventTrigger]:
        return AwaitMessageTrigger  # type: ignore[return-value]

    def trigger_kwargs(self, queue: str, **kwargs) -> dict[str, Any]:
        """
        Parse URI of format:
            mq://<conn_id>/<queue_name>
        """
        parsed = urlparse(queue)

        if not parsed.netloc:
            raise ValueError(
                "MQ URI must contain connection id. Expected format: mq://<conn_id>/<queue_name>"
            )

        conn_id = parsed.netloc

        queue_name = parsed.path.lstrip("/")
        if not queue_name:
            raise ValueError("MQ URI must contain queue name. Expected format: mq://<conn_id>/<queue_name>")

        return {
            "mq_conn_id": conn_id,
            "queue_name": queue_name,
            "poll_interval": kwargs.get("poll_interval", 5),
        }
