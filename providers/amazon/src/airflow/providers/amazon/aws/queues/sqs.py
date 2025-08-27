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
from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger

try:
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'common.messaging' provider to be installed in version >= 1.0.1."
    )

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger

# [START queue_regexp]
QUEUE_REGEXP = r"^https://sqs\.[^.]+\.amazonaws\.com/[0-9]+/.+"
# [END queue_regexp]


class SqsMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for SQS integration with common-messaging.

    [START sqs_message_queue_provider_description]

    * It uses ``sqs`` as scheme for identifying SQS queues.
    * For parameter definitions take a look at :class:`~airflow.providers.amazon.aws.triggers.sqs.SqsSensorTrigger`.

    .. code-block:: python

        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.sdk import Asset, AssetWatcher

        trigger = MessageQueueTrigger(
            scheme="sqs",
            # Additional AWS SqsSensorTrigger parameters as needed
            sqs_queue="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            aws_conn_id="aws_default",
        )

        asset = Asset("sqs_queue_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

    For a complete example, see:
    :mod:`tests.system.amazon.aws.example_dag_sqs_message_queue_trigger`

    [END sqs_message_queue_provider_description]
    """

    scheme = "sqs"

    def queue_matches(self, queue: str) -> bool:
        return bool(re.match(QUEUE_REGEXP, queue))

    def trigger_class(self) -> type[BaseEventTrigger]:
        return SqsSensorTrigger

    def trigger_kwargs(self, queue: str, **kwargs) -> dict:
        return {
            "sqs_queue": queue,
        }
