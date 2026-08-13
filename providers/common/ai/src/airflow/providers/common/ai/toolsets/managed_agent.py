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

import logging
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    return_schema_kwargs,
    serialize_for_llm,
)

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

log = logging.getLogger(__name__)

_PROMPT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "prompt": {
            "type": "string",
            "description": "The question or instruction to send to this agent.",
        }
    },
    "required": ["prompt"],
}


class BaseManagedAgentToolset(AbstractToolset[Any]):
    """
    Base class exposing a vendor-managed agent as a single pydantic-ai tool.

    A managed agent runs its own reasoning loop on the vendor's infrastructure
    (Snowflake Cortex Agents, Amazon Bedrock AgentCore, Azure AI Foundry hosted
    agents, Vertex AI Agent Engine). Airflow submits one request and reads one
    answer, so the Airflow-side agent features -- toolsets, human-in-the-loop
    review, durable step replay -- apply to the *calling* agent and never reach
    inside the managed agent.

    Subclasses implement :meth:`agent_ref` and :meth:`invoke`. Tool naming,
    argument validation, result serialisation and logging are handled here so
    every provider's implementation presents the same surface to the model.

    :param tool_name: Name the calling model sees. A verb phrase naming the
        specialist reads best, e.g. ``ask_bookings_analyst``.
    :param description: What this agent knows and when to consult it. Required:
        a remote agent's competence cannot be introspected, and this is the only
        basis the calling model has for choosing between specialists.
    :param timeout: Seconds to wait for a single invocation. ``None`` defers to
        the platform default, which subclasses supply -- a number chosen here
        would silently disagree with the vendor operator's documented timeout
        for the same service.
    """

    #: Whether a completed invocation may be replayed from the durable cache
    #: instead of re-invoked. Off by default because a managed agent may act on
    #: systems Airflow cannot observe, so replaying a cached answer could skip a
    #: side effect. Read-only agents should opt in.
    replayable: bool = False

    def __init__(
        self,
        *,
        tool_name: str,
        description: str,
        timeout: float | None = None,
    ) -> None:
        if not tool_name:
            raise ValueError("tool_name must be a non-empty string.")
        if not description or not description.strip():
            raise ValueError(
                "description is required: the calling model uses it to decide which "
                "specialist to consult, and it cannot be derived from the agent's identifier."
            )
        self._tool_name = tool_name
        self._description = description
        self._timeout = timeout

    @property
    @abstractmethod
    def agent_ref(self) -> dict[str, str]:
        """
        Normalised identity of the remote agent.

        Must contain ``platform`` and ``name``, e.g.
        ``{"platform": "snowflake.cortex", "name": "ANALYTICS.REVENUE.BOOKINGS_ANALYST"}``.
        Logged on every invocation so a run can be audited for which agents were
        consulted.
        """

    @abstractmethod
    async def invoke(self, prompt: str) -> Any:
        """
        Send ``prompt`` to the remote agent and return the agent's answer.

        Return the answer, not the transport envelope -- whatever the calling
        model should actually read. Unwrapping is the implementation's job.

        Failures sort into three buckets, and conflating them is the most common
        way an implementation goes wrong:

        * ``pydantic_ai.exceptions.ModelRetry`` -- the remote agent rejected the
          request in a way rephrasing could fix. The calling model sees the
          message and tries again, bounded by its ``usage_limits``.
        * :class:`~airflow.providers.common.ai.exceptions.ManagedAgentInvocationError`
          -- terminal. Bad credentials, missing agent, revoked quota. Neither a
          rephrase nor a task retry helps, so fail fast.
        * Anything transient (429, 5xx, connection reset, read timeout) -- let it
          propagate unchanged. Airflow's task-level retry is the right layer; a
          rephrase does nothing for a 503.

        :param prompt: The question or instruction to send to the remote agent.
        """

    @property
    def id(self) -> str:
        return f"managed-agent-{self._tool_name}"

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tool_def = ToolDefinition(
            name=self._tool_name,
            description=self._description,
            parameters_json_schema=_PROMPT_SCHEMA,
            # Each invocation is an independent request to a remote service, so
            # consulting several specialists concurrently is both safe and the point.
            sequential=False,
            **return_schema_kwargs({"type": "string"}),
        )
        return {
            self._tool_name: ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=build_args_validator(_PROMPT_SCHEMA),
            )
        }

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        ref = self.agent_ref
        log.info("Consulting managed agent %s on %s", ref.get("name"), ref.get("platform"))
        result = await self.invoke(tool_args["prompt"])
        return serialize_for_llm(result)
