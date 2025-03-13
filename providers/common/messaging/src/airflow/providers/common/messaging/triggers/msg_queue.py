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

from collections.abc import AsyncIterator
from functools import cached_property
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.common.messaging.providers import MESSAGE_QUEUE_PROVIDERS
from airflow.triggers.base import BaseEventTrigger, TriggerEvent


class MessageQueueTrigger(BaseEventTrigger):
    """
    ``MessageQueueTrigger`` serves as a unified trigger for monitoring message queues from different providers.

    It abstracts away provider-specific details, allowing users to monitor a queue with a single trigger,
    regardless of the underlying provider.

    This makes it easy to switch providers without modifying the trigger.

    :param queue: The queue identifier

    .. seealso::
        For more information on how to use this trigger, take a look at the guide:
        :ref:`howto/trigger:MessageQueueTrigger`
    """

    def __init__(self, *, queue: str, **kwargs: Any) -> None:
        self.queue = queue
        self.kwargs = kwargs

    @cached_property
    def trigger(self) -> BaseEventTrigger:
        providers = [provider for provider in MESSAGE_QUEUE_PROVIDERS if provider.queue_matches(self.queue)]
        if len(providers) == 0:
            raise ValueError(f"The queue '{self.queue}' is not recognized by ``MessageQueueTrigger``.")
        if len(providers) > 1:
            self.log.error(
                "The queue '%s' is recognized by more than one provider. "
                "At least two providers in ``MESSAGE_QUEUE_PROVIDERS`` are colliding with each "
                "other.",
                self.queue,
            )
            raise AirflowException(f"The queue '{self.queue}' is recognized by more than one provider.")
        return providers[0].trigger_class()(
            **providers[0].trigger_kwargs(self.queue, **self.kwargs), **self.kwargs
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return self.trigger.serialize()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async for event in self.trigger.run():
            yield event
