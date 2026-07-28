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
from urllib.parse import parse_qs, urlparse

from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
from airflow.providers.ibm.mq.triggers.mq import AwaitMessageTrigger

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger

# [START queue_regexp]
# Matches ibmmq://<conn_id>/<queue_name> (conn_id required, queue_name required)
QUEUE_REGEXP = r"^ibmmq://[^/]+/.+"
# [END queue_regexp]


class IBMMQMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for IBM MQ integration with common-messaging.

    [START ibmmq_message_queue_provider_description]

    * It uses ``ibmmq`` as scheme for identifying IBM MQ queues.
    * For parameter definitions take a look at
      :class:`~airflow.providers.ibm.mq.triggers.mq.AwaitMessageTrigger`.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        trigger = MessageQueueTrigger(
            queue="ibmmq://mq_default/MY.QUEUE.NAME",
        )

        asset = Asset("mq_topic_asset", watchers=[AssetWatcher(name="mq_watcher", trigger=trigger)])

    [END ibmmq_message_queue_provider_description]
    """

    scheme = "ibmmq"

    def queue_matches(self, queue: str) -> bool:
        return bool(re.match(QUEUE_REGEXP, queue))

    def trigger_class(self) -> type[BaseEventTrigger]:
        return AwaitMessageTrigger  # type: ignore[return-value]

    def trigger_kwargs(self, queue: str, **kwargs) -> dict[str, Any]:
        # Parse the queue URI
        parsed = urlparse(queue)
        query_params = parse_qs(parsed.query, keep_blank_values=True)

        if not parsed.netloc:
            raise ValueError(
                "MQ URI must contain connection id. Expected format: ibmmq://<conn_id>/<queue_name>"
            )

        conn_id = parsed.netloc

        queue_name = parsed.path.lstrip("/")
        if not queue_name:
            raise ValueError(
                "MQ URI must contain queue name. Expected format: ibmmq://<conn_id>/<queue_name>"
            )

        open_options: int | None = None
        raw_open_options = query_params.get("open_options", [])
        if raw_open_options:
            import ibmmq

            open_options = 0
            for value in raw_open_options:
                if not value:
                    raise ValueError(
                        "MQ URI open_options query parameter values must be numeric or CMQC constant names"
                    )
                found_token = False
                for token in (part.strip() for part in value.replace("|", ",").split(",")):
                    if not token:
                        continue
                    found_token = True
                    try:
                        option_value = int(token, 0)
                    except ValueError:
                        if not token.startswith("MQOO_"):
                            raise ValueError(
                                f"Invalid MQ URI open_options value '{token}'. "
                                "Use numeric values or MQOO_* CMQC constant names "
                                "(for example MQOO_INPUT_SHARED)."
                            ) from None
                        option_value = getattr(ibmmq.CMQC, token, None)  # type: ignore[assignment]
                        if not isinstance(option_value, int):
                            raise ValueError(
                                f"Invalid MQ URI open_options value '{token}'. "
                                "Use numeric values or MQOO_* CMQC constant names "
                                "(for example MQOO_INPUT_SHARED)."
                            ) from None
                    open_options |= option_value
                if not found_token:
                    raise ValueError(
                        "MQ URI open_options query parameter values must be numeric or MQOO_* CMQC constant names"
                    )

            if open_options == 0:
                open_options = None
        trigger_kwargs: dict[str, Any] = {
            "mq_conn_id": conn_id,
            "queue_name": queue_name,
        }
        if "poll_interval" not in kwargs:
            trigger_kwargs["poll_interval"] = 5

        # MessageQueueTrigger(queue=..., **kwargs) passes both provider kwargs and raw kwargs.
        # Avoid duplicate keyword errors by not re-emitting keys already present in raw kwargs.
        if open_options is not None and "open_options" not in kwargs:
            trigger_kwargs["open_options"] = open_options
        return trigger_kwargs
