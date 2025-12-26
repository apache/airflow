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

from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger


class BaseMessageQueueProvider:
    """
    Base class defining a provider supported by operators/triggers of common-messaging provider.

    To add a new provider supported by the provider, create a new class extending this base class and add it
    to ``MESSAGE_QUEUE_PROVIDERS``.
    """

    scheme: str | None = None

    def scheme_matches(self, scheme: str) -> bool:
        """
        Return whether a given scheme (string) matches a specific provider's pattern.

        This function must be as specific as possible to avoid collision with other providers.
        Functions in this provider should NOT overlap with each other in their matching criteria.

        :param scheme: The scheme identifier
        """
        return self.scheme == scheme

    @abstractmethod
    def queue_matches(self, queue: str) -> bool:
        """
        Return whether a given queue (string) matches a specific provider's pattern.

        This function must be as specific as possible to avoid collision with other providers.
        Functions in this provider should NOT overlap with each other in their matching criteria.

        :param queue: The queue identifier
        """

    @abstractmethod
    def trigger_class(self) -> type[BaseEventTrigger]:
        """Trigger class to use when ``queue_matches`` returns True."""

    @abstractmethod
    def trigger_kwargs(self, queue: str, **kwargs) -> dict:
        """
        Parameters passed to the instance of ``trigger_class``.

        :param queue: The queue identifier
        """
