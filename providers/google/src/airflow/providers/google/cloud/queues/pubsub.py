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


class PubsubMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for PubSub integration with common-messaging.

    [START pubsub_message_queue_provider_description]
    * It uses ``google+pubsub`` as the scheme for identifying the provider.
    * For parameter definitions, take a look at :class:`~airflow.providers.google.cloud.triggers.pubsub.PubsubPullTrigger`.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        trigger = MessageQueueTrigger(
            scheme="google+pubsub",
            # Additional PubsubPullTrigger parameters as needed
            project_id="my_project",
            subscription="my_subscription",
            ack_messages=True,
            max_messages=1,
            gcp_conn_id="google_cloud_default",
            poke_interval=60.0,
        )

        asset = Asset("pubsub_queue_asset", watchers=[AssetWatcher(name="pubsub_watcher", trigger=trigger)])

    [END pubsub_message_queue_provider_description]

    """

    scheme = "google+pubsub"

    def trigger_class(self) -> type[BaseEventTrigger]:
        return PubsubPullTrigger  # type: ignore[return-value]
