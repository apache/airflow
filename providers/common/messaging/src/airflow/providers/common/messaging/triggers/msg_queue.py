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
from collections.abc import AsyncIterator
from functools import cached_property
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
from airflow.triggers.base import BaseEventTrigger, TriggerEvent


def is_sqs_queue(queue: str) -> bool:
    return bool(re.match(r"^https://sqs\.[^.]+\.amazonaws\.com/[0-9]+/.+", queue))


# This list defines the supported providers. Each item in the list is a tuple containing:
#   1. A function (Callable) that checks if a given queue (string) matches a specific provider's pattern.
#   2. The corresponding trigger to use when the function returns True.
#
# The function that checks whether a queue matches a provider's pattern must be as specific as possible to
# avoid collision. Functions in this list should NOT overlap with each other in their matching criteria.
# To add support for a new provider in `MessageQueueTrigger`, add a new entry to this list.
MESSAGE_QUEUE_PROVIDERS = [(is_sqs_queue, SqsSensorTrigger)]


class MessageQueueTrigger(BaseEventTrigger):
    """
    ``MessageQueueTrigger`` serves as a unified trigger for monitoring message queues from different providers.

    It abstracts away provider-specific details, allowing users to monitor a queue with a single trigger,
    regardless of the underlying provider.

    This makes it easy to switch providers without modifying the trigger.

    :param queue: The queue identifier
    """

    def __init__(self, *, queue: str, **kwargs: Any) -> None:
        self.queue = queue
        self.kwargs = kwargs

    @cached_property
    def trigger(self) -> BaseEventTrigger:
        triggers = [trigger[1] for trigger in MESSAGE_QUEUE_PROVIDERS if trigger[0](self.queue)]
        if len(triggers) == 0:
            raise ValueError(f"The queue '{self.queue}' is not recognized by ``MessageQueueTrigger``.")
        if len(triggers) > 1:
            self.log.error(
                "The queue '%s' is recognized by more than one trigger. "
                "At least two functions in ``MESSAGE_QUEUE_PROVIDERS`` are colliding with each "
                "other.",
                self.queue,
            )
            raise AirflowException(f"The queue '{self.queue}' is recognized by more than one trigger.")
        return triggers[1](**self.kwargs)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return self.trigger.serialize()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        return self.trigger.run()
