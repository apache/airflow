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
"""
Operator for running AI agents with tools and multi-turn reasoning.

Supports two agent frameworks selectable via ``agent_framework``:

* ``"pydantic_ai"`` (default) — uses pydantic-ai's ``Agent.run_sync()``.
* ``"adk"`` — uses Google's Agent Development Kit (``google-adk``).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.ai.utils.logging import log_run_summary, wrap_toolsets_for_logging
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.providers.common.ai.hooks.adk import AdkHook
    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.sdk import Context

SUPPORTED_FRAMEWORKS = ("pydantic_ai", "adk")


class AgentOperator(BaseOperator):
    """
    Run an AI Agent with tools and multi-turn reasoning.

    Provide ``llm_conn_id`` and optional ``toolsets`` / ``tools`` to let the
    operator build and run the agent.  The agent reasons about the prompt,
    calls tools in a multi-turn loop, and returns a final answer.

    Two agent frameworks are supported, selectable via ``agent_framework``:

    * ``"pydantic_ai"`` (default) — built on the **pydantic-ai** library.
      Tools are supplied as ``toolsets`` (instances of
      ``pydantic_ai.toolsets.abstract.AbstractToolset``).
    * ``"adk"`` — built on Google's **Agent Development Kit**
      (``google-adk``).  Tools are supplied as ``tools`` (plain callables
      whose docstrings are exposed to the LLM).

    :param prompt: The prompt to send to the agent.
    :param llm_conn_id: Connection ID for the LLM provider.
        Used by the ``pydantic_ai`` framework only. For ADK, set
        ``model_id`` directly or configure environment variables
        (``GOOGLE_API_KEY``, ``GOOGLE_APPLICATION_CREDENTIALS``, etc.).
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"`` for
        pydantic-ai or ``"gemini-2.5-flash"`` for ADK).  Overrides the
        model stored in the connection's extra field when using pydantic-ai.
    :param system_prompt: System-level instructions for the agent.
    :param output_type: Expected output type. Default ``str``.  Set to a
        Pydantic ``BaseModel`` subclass for structured output.
    :param toolsets: List of pydantic-ai toolsets the agent can use
        (e.g. ``SQLToolset``, ``HookToolset``).
        Applies only when ``agent_framework="pydantic_ai"``.
    :param tools: List of plain callables exposed as tools to the agent.
        Applies only when ``agent_framework="adk"``.  Each callable's
        docstring is sent to the LLM so it knows when and how to call it.
    :param enable_tool_logging: When ``True`` (default), wraps each
        pydantic-ai toolset in a ``LoggingToolset`` that logs calls at
        INFO/DEBUG level.  Set to ``False`` to disable.
        Applies only when ``agent_framework="pydantic_ai"``.
    :param agent_framework: Which framework to use.  ``"pydantic_ai"``
        (default) or ``"adk"``.
    :param agent_params: Additional keyword arguments passed to the
        underlying agent constructor (pydantic-ai ``Agent`` *or* ADK
        ``Agent``).
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
        llm_conn_id: str = "",
        model_id: str | None = None,
        system_prompt: str = "",
        output_type: type = str,
        toolsets: list[AbstractToolset] | None = None,
        tools: list[Callable] | None = None,
        enable_tool_logging: bool = True,
        agent_framework: str = "pydantic_ai",
        agent_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        if agent_framework not in SUPPORTED_FRAMEWORKS:
            raise ValueError(
                f"Unsupported agent_framework={agent_framework!r}. Choose from {SUPPORTED_FRAMEWORKS}."
            )

        if agent_framework == "pydantic_ai" and not llm_conn_id:
            raise ValueError("llm_conn_id is required when agent_framework='pydantic_ai'.")

        if agent_framework == "adk" and not model_id:
            raise ValueError("model_id is required when agent_framework='adk'.")

        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.toolsets = toolsets
        self.tools = tools
        self.enable_tool_logging = enable_tool_logging
        self.agent_framework = agent_framework
        self.agent_params = agent_params or {}

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """Return PydanticAIHook for the configured LLM connection."""
        from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook

        return PydanticAIHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

    @cached_property
    def adk_hook(self) -> AdkHook:
        """Return AdkHook for the configured ADK connection."""
        from airflow.providers.common.ai.hooks.adk import AdkHook

        return AdkHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

    # ------------------------------------------------------------------
    # pydantic-ai backend
    # ------------------------------------------------------------------
    def _execute_pydantic_ai(self) -> Any:
        extra_kwargs = dict(self.agent_params)
        if self.toolsets:
            if self.enable_tool_logging:
                extra_kwargs["toolsets"] = wrap_toolsets_for_logging(self.toolsets, self.log)
            else:
                extra_kwargs["toolsets"] = self.toolsets
        agent: Agent[None, Any] = self.llm_hook.create_agent(
            output_type=self.output_type,
            instructions=self.system_prompt,
            **extra_kwargs,
        )
        result = agent.run_sync(self.prompt)
        log_run_summary(self.log, result)
        output = result.output

        if isinstance(output, BaseModel):
            return output.model_dump()
        return output

    # ------------------------------------------------------------------
    # Google ADK backend
    # ------------------------------------------------------------------
    def _execute_adk(self) -> Any:
        extra_kwargs = dict(self.agent_params)
        adk_tools = list(self.tools or [])
        if adk_tools:
            extra_kwargs["tools"] = adk_tools

        agent = self.adk_hook.create_agent(
            instruction=self.system_prompt,
            description=extra_kwargs.pop("description", "Airflow AgentOperator ADK agent"),
            **extra_kwargs,
        )

        output = self.adk_hook.run_agent_sync(agent=agent, prompt=self.prompt)

        self.log.info("ADK agent run complete for model=%s", self.model_id)

        if isinstance(output, BaseModel):
            return output.model_dump()
        return output

    # ------------------------------------------------------------------
    # execute dispatch
    # ------------------------------------------------------------------
    def execute(self, context: Context) -> Any:
        if self.agent_framework == "adk":
            result = self._execute_adk()
        else:
            result = self._execute_pydantic_ai()
        self.xcom_push(context, key="result_time", value=result)
        return result
