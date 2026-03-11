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
"""Abstract base hook for AI/LLM framework integrations (AIP-99 Phase 2.5b)."""

from __future__ import annotations

from abc import abstractmethod
from typing import Any

from airflow.providers.common.compat.sdk import BaseHook


class BaseAIHook(BaseHook):
    """
    Abstract base hook that defines the contract for AI framework integrations.

    Every AI hook (pydantic-ai, ADK, future frameworks) must implement this
    interface so that :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`
    can work with any backend transparently — the user picks the backend by
    choosing a connection type, not a different operator class.

    Subclasses must implement:

    * :meth:`get_conn` — resolve and return the underlying model/connection object.
    * :meth:`create_agent` — build a framework-specific agent with the given
      configuration, optionally bridging ``toolsets`` to the native tool format.
    * :meth:`run_agent` — execute the agent synchronously and return the output.
    * :meth:`test_connection` — validate that the model can be resolved.
    """

    model_id: str | None = None

    @abstractmethod
    def get_conn(self) -> Any:
        """
        Return the underlying model or connection object.

        The concrete type depends on the framework (e.g. a pydantic-ai
        ``Model`` or an ADK model identifier string).  The result should be
        cached for the lifetime of the hook instance.
        """

    @abstractmethod
    def create_agent(
        self,
        *,
        output_type: type = str,
        instructions: str = "",
        toolsets: list | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Create a framework-specific agent configured with this hook's model.

        :param output_type: Expected output type (default ``str``).
        :param instructions: System-level instructions for the agent.
        :param toolsets: List of pydantic-ai ``AbstractToolset`` instances.
            Hooks that do not natively support pydantic-ai toolsets must
            bridge them to their native tool format.
        :param kwargs: Additional framework-specific keyword arguments passed
            to the underlying agent constructor.
        """

    @abstractmethod
    def run_agent(self, *, agent: Any, prompt: str) -> Any:
        """
        Run the agent synchronously and return the output.

        Logging of run metadata (token usage, model name, tool call sequence)
        should be handled internally by the hook using ``self.log``.

        :param agent: A framework-specific agent instance created by
            :meth:`create_agent`.
        :param prompt: The user prompt to send to the agent.
        :return: The agent's output — a string, a dict (from ``BaseModel.model_dump()``),
            or any serializable value.
        """

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """
        Test that the model can be resolved without making an LLM API call.

        :return: ``(True, message)`` on success, ``(False, error)`` on failure.
        """
