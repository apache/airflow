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

from airflow.providers.amazon.aws.triggers.kinesis import KinesisTrigger
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'common.messaging' provider to be installed in version >= 2.0.0."
    )

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger


class KinesisMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for Amazon Kinesis Data Streams integration with common-messaging.

    [START kinesis_message_queue_provider_description]

    * It uses ``kinesis`` as scheme for identifying Kinesis Data Streams.
    * For parameter definitions take a look at :class:`~airflow.providers.amazon.aws.triggers.kinesis.KinesisTrigger`.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        trigger = MessageQueueTrigger(
            scheme="kinesis",
            stream_name="my-kinesis-stream",
            aws_conn_id="aws_default",
        )

        watcher = AssetWatcher(name="kinesis_watcher", trigger=trigger)
        asset = Asset("kinesis_stream_asset", watchers=[watcher])

    For a complete example, see:
    :mod:`tests.system.amazon.aws.example_kinesis_message_queue`

    [END kinesis_message_queue_provider_description]
    """

    scheme = "kinesis"

    def trigger_class(self) -> type[BaseEventTrigger]:
        return KinesisTrigger
