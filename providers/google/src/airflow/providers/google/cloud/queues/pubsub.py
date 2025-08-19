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

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger

try:
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'common.messaging' provider to be installed in version >= 1.0.1."
    )

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger

# [START queue_regexp]
QUEUE_REGEXP = (
    r"^projects/(?P<project_id>[^/]+)/subscriptions/"
    r"(?P<subscription>(?!goog)[A-Za-z][A-Za-z0-9\-_.~+%]{2,254})$"
)
# [END queue_regexp]


class PubsubMessageQueueProvider(BaseMessageQueueProvider):
    """Configuration for PubSub integration with common-messaging."""

    def queue_matches(self, queue: str) -> bool:
        return bool(re.match(QUEUE_REGEXP, queue))

    def trigger_class(self) -> type[BaseEventTrigger]:
        return PubsubPullTrigger  # type: ignore[return-value]

    def trigger_kwargs(self, queue: str, **kwargs) -> dict:
        pattern = re.compile(QUEUE_REGEXP)
        match = pattern.match(queue)

        if match is None:
            raise ValueError(f"Queue '{queue}' does not match the expected PubSub format")

        project_id = match.group("project_id")
        subscription = match.group("subscription")

        if "project_id" in kwargs or "subscription" in kwargs:
            raise ValueError(
                "project_id or subscription cannot be provided in kwargs, use the queue param instead"
            )

        return_kwargs = {
            "project_id": project_id,
            "subscription": subscription,
            "ack_messages": True,
        }
        if "max_messages" not in kwargs:
            return_kwargs["max_messages"] = 1
        if "gcp_conn_id" not in kwargs:
            return_kwargs["gcp_conn_id"] = "google_cloud_default"

        return return_kwargs
