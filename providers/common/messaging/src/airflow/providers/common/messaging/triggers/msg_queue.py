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

import importlib
import warnings
from collections.abc import AsyncIterator
from functools import cached_property
from typing import Any

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers_manager import ProvidersManager
from airflow.triggers.base import BaseEventTrigger, TriggerEvent

providers_manager = ProvidersManager()
providers_manager.initialize_providers_queues()


def create_class_by_name(name: str):
    module_name, class_name = name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


MESSAGE_QUEUE_PROVIDERS = [create_class_by_name(name)() for name in providers_manager.queue_class_names]


class MessageQueueTrigger(BaseEventTrigger):
    """
    ``MessageQueueTrigger`` serves as a unified trigger for monitoring message queues from different providers.

    It abstracts away provider-specific details, allowing users to monitor a queue with a single trigger,
    regardless of the underlying provider.

    This makes it easy to switch providers without modifying the trigger.

    :param scheme: The queue scheme (e.g., 'kafka', 'redis+pubsub', 'sqs'). Used for provider matching.
    :param queue: **Deprecated** The queue identifier (URI format). If provided, this takes precedence over scheme parameter.
        This parameter is deprecated and will be removed in future versions. Use the 'scheme' parameter instead.

    .. seealso::
        For more information on how to use this trigger, take a look at the guide:
        :ref:`howto/trigger:MessageQueueTrigger`
    """

    queue: str | None = None
    scheme: str | None = None

    def __init__(self, *, queue: str | None = None, scheme: str | None = None, **kwargs: Any) -> None:
        if queue is None and scheme is None:
            raise ValueError("Either `queue` or `scheme` parameter must be provided.")

        # For backward compatibility, queue takes precedence
        if queue is not None:
            warnings.warn(
                "The `queue` parameter is deprecated and will be removed in future versions. "
                "Use the `scheme` parameter instead and pass configuration as keyword arguments to `MessageQueueTrigger`.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            self.queue = queue
            self.scheme = None
        else:
            self.queue = None
            self.scheme = scheme

        self.kwargs = kwargs

    @cached_property
    def trigger(self) -> BaseEventTrigger:
        if len(MESSAGE_QUEUE_PROVIDERS) == 0:
            self.log.error(
                "No message queue providers are available. "
                "Please ensure that you have the necessary providers installed."
            )
            raise ValueError("No message queue providers are available. ")

        # Find matching providers based on queue URI or scheme
        if self.queue is not None:
            # Use existing queue-based matching for backward compatibility
            providers = [
                provider for provider in MESSAGE_QUEUE_PROVIDERS if provider.queue_matches(self.queue)
            ]
            identifier = self.queue
            match_by = "queue"
        elif self.scheme is not None:
            # Use new scheme-based matching
            providers = [
                provider for provider in MESSAGE_QUEUE_PROVIDERS if provider.scheme_matches(self.scheme)
            ]
            identifier = self.scheme
            match_by = "scheme"

        if len(providers) == 0:
            self.log.error(
                "The %s '%s' is not recognized by any of the registered providers. "
                "The available providers are: '%s'.",
                match_by,
                identifier,
                ", ".join([type(provider).__name__ for provider in MESSAGE_QUEUE_PROVIDERS]),
            )
            raise ValueError(
                f"The {match_by} '{identifier}' is not recognized by any of the registered providers."
            )

        if len(providers) > 1:
            self.log.error(
                "The %s '%s' is recognized by more than one provider. "
                "At least two providers in ``MESSAGE_QUEUE_PROVIDERS`` are colliding with each "
                "other: '%s'",
                match_by,
                identifier,
                ", ".join([type(provider).__name__ for provider in providers]),
            )
            raise ValueError(f"The {match_by} '{identifier}' is recognized by more than one provider.")

        # Create trigger instance
        selected_provider = providers[0]
        if self.queue is not None:
            # Pass queue to trigger_kwargs for backward compatibility
            trigger_kwargs = selected_provider.trigger_kwargs(self.queue, **self.kwargs)
            return selected_provider.trigger_class()(**trigger_kwargs, **self.kwargs)
        # For scheme-based matching, we need to pass all current kwargs to the trigger
        return selected_provider.trigger_class()(**self.kwargs)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return self.trigger.serialize()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async for event in self.trigger.run():
            yield event
