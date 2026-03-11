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
"""Operator for running AI agents with tools and multi-turn reasoning."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.base import BaseAIHook
from airflow.providers.common.ai.utils.logging import wrap_toolsets_for_logging
from airflow.providers.common.compat.sdk import BaseHook, BaseOperator

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.sdk import Context


class AgentOperator(BaseOperator):
    """
    Run an AI agent with tools and multi-turn reasoning.

    Works with **any** AI framework that implements
    :class:`~airflow.providers.common.ai.hooks.base.BaseAIHook` — the backend
    is selected by the **connection type**, not by choosing a different operator.

    Supported backends:

    * **pydantic-ai** (connection type ``pydanticai``) — default.  Uses
      `pydantic-ai <https://ai.pydantic.dev/>`__ agents.  All toolsets
      (``SQLToolset``, ``HookToolset``, ``MCPToolset``) work natively.
    * **Google ADK** (connection type ``adk``) — uses Google's
      `Agent Development Kit <https://google.github.io/adk-docs/>`__ with
      Gemini models.  Toolsets are automatically bridged to ADK-compatible
      callable tools.

    :param prompt: The prompt to send to the agent.
    :param llm_conn_id: Connection ID for the LLM provider.  The connection
        type determines which AI framework is used.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"`` for pydantic-ai
        or ``"gemini-2.5-flash"`` for ADK).  Overrides the model stored in
        the connection's extra field.
    :param system_prompt: System-level instructions for the agent.
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output.
    :param toolsets: List of pydantic-ai toolsets the agent can use
        (e.g. ``SQLToolset``, ``HookToolset``).  When using ADK, toolsets are
        bridged automatically to ADK-compatible callable tools.
    :param enable_tool_logging: When ``True`` (default), wraps each toolset in a
        ``LoggingToolset`` that logs tool calls with timing at INFO level and
        arguments at DEBUG level.  Currently applies to pydantic-ai backend only.
    :param agent_params: Additional keyword arguments passed to the underlying
        agent constructor (e.g. ``retries``, ``model_settings`` for pydantic-ai,
        or ``description``, ``tools`` for ADK).
    """

    template_fields: Sequence[str] = (
        "prompt",
        "llm_conn_id",
        "model_id",
        "system_prompt",
        "agent_params",
    )

    def __init__(
        self,
        *,
        prompt: str,
        llm_conn_id: str,
        model_id: str | None = None,
        system_prompt: str = "",
        output_type: type = str,
        toolsets: list[AbstractToolset] | None = None,
        enable_tool_logging: bool = True,
        agent_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.toolsets = toolsets
        self.enable_tool_logging = enable_tool_logging
        self.agent_params = agent_params or {}

    @cached_property
    def llm_hook(self) -> BaseAIHook:
        """
        Resolve the AI hook from the connection type.

        Checks the connection's ``conn_type`` to pick the right hook:

        * ``adk`` → :class:`~airflow.providers.common.ai.hooks.adk.AdkHook`
        * everything else → :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`

        This allows users to switch backends by changing the connection type,
        not the operator class.
        """
        conn_type = self._resolve_conn_type()

        if conn_type == "adk":
            from airflow.providers.common.ai.hooks.adk import AdkHook

            return AdkHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

        from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook

        return PydanticAIHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

    def _resolve_conn_type(self) -> str:
        """Look up the connection type for ``llm_conn_id``."""
        try:
            conn = BaseHook.get_connection(self.llm_conn_id)
            return conn.conn_type
        except Exception:
            return "pydanticai"

    def execute(self, context: Context) -> Any:
        extra_kwargs = dict(self.agent_params)

        # Prepare toolsets — wrap for logging when using pydantic-ai.
        resolved_toolsets = self.toolsets
        if self.toolsets and self.enable_tool_logging:
            hook_type = self._resolve_conn_type()
            if hook_type != "adk":
                resolved_toolsets = wrap_toolsets_for_logging(self.toolsets, self.log)

        agent = self.llm_hook.create_agent(
            output_type=self.output_type,
            instructions=self.system_prompt,
            toolsets=resolved_toolsets,
            **extra_kwargs,
        )

        output = self.llm_hook.run_agent(agent=agent, prompt=self.prompt)

        if isinstance(output, BaseModel):
            return output.model_dump()
        return output
