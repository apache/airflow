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
"""Shared contract for agent-framework hooks used by :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`."""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar

from airflow.providers.common.compat.sdk import BaseHook


@dataclass
class AgentUsage:
    """Token and request usage from an agent run, when the backend exposes it."""

    requests: int = 0
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


@dataclass
class AgentRunResult:
    """
    Backend-neutral result from :meth:`BaseAIHook.run_agent`.

    :param output: Final agent output (``str``, Pydantic model instance, etc.).
    :param message_history: Opaque conversation state for HITL regeneration; only pass back to the
        same hook implementation that produced it.
    :param model_name: Resolved model identifier, when available.
    :param usage: Usage counters when the backend exposes them.
    :param tool_names: Ordered tool names invoked during the run, when known.
    """

    output: Any
    message_history: Any = None
    model_name: str | None = None
    usage: AgentUsage | None = None
    tool_names: list[str] | None = None


class BaseAIHook(BaseHook, metaclass=ABCMeta):
    """
    Abstract hook for multi-turn LLM agents.

    :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` resolves the concrete hook
    from the Airflow connection ``conn_type`` (for example ``pydanticai`` or ``pydanticai-bedrock``).

    Subclasses implement :meth:`get_conn`, :meth:`create_agent`, and :meth:`run_agent` for their agent
    runtime.
    """

    conn_name_attr = "llm_conn_id"

    supports_toolsets: ClassVar[bool] = False
    supports_durable: ClassVar[bool] = False
    supports_usage_limits: ClassVar[bool] = False

    @classmethod
    def get_agent_hook(cls, conn_id: str, *, hook_params: dict[str, Any] | None = None) -> BaseAIHook:
        """
        Return an agent hook for *conn_id*, verifying it implements this contract.

        Uses the connection's ``conn_type`` to select the hook class registered in
        ``provider.yaml``.
        """
        hook = cls.get_hook(conn_id, hook_params=hook_params)
        if not isinstance(hook, BaseAIHook):
            raise TypeError(
                f"Connection {conn_id!r} resolved to {type(hook).__name__}, which is not a BaseAIHook. "
                "Use a connection type registered for agent frameworks (e.g. pydanticai, pydanticai-bedrock)."
            )
        return hook

    @abstractmethod
    def get_conn(self) -> Any:
        """Return the backend model/client used to construct agents."""

    @abstractmethod
    def create_agent(
        self,
        *,
        output_type: type[Any] = str,
        instructions: str = "",
        **agent_kwargs: Any,
    ) -> Any:
        """
        Build an agent instance for this backend.

        :param output_type: Expected structured output type (``str`` or a Pydantic ``BaseModel`` subclass).
        :param instructions: System-level instructions for the agent.
        :param agent_kwargs: Backend-specific options (e.g. pydantic-ai ``toolsets``).
        """

    @abstractmethod
    def run_agent(
        self,
        agent: Any,
        *,
        prompt: str,
        usage_limits: Any = None,
        message_history: Any = None,
    ) -> AgentRunResult:
        """
        Run *agent* with *prompt* and return a normalized :class:`AgentRunResult`.

        :param agent: Object returned by :meth:`create_agent`.
        :param prompt: User prompt for this invocation.
        :param usage_limits: Backend-specific usage limits (pydantic-ai ``UsageLimits`` only today).
        :param message_history: Prior conversation state from a previous :class:`AgentRunResult`.
        """
