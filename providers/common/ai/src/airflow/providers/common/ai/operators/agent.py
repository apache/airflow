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
"""Operator for running pydantic-ai agents with tools and multi-turn reasoning."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.sdk import Context


class AgentOperator(BaseOperator):
    """
    Run a pydantic-ai Agent with tools and multi-turn reasoning.

    Provide ``llm_conn_id`` and optional ``toolsets`` to let the operator build
    and run the agent. The agent reasons about the prompt, calls tools in a
    multi-turn loop, and returns a final answer.

    :param prompt: The prompt to send to the agent.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
        Overrides the model stored in the connection's extra field.
    :param system_prompt: System-level instructions for the agent.
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output.
    :param toolsets: List of pydantic-ai toolsets the agent can use
        (e.g. ``SQLToolset``, ``HookToolset``).
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``).
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
        self.agent_params = agent_params or {}

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """Return PydanticAIHook for the configured LLM connection."""
        return PydanticAIHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

    def execute(self, context: Context) -> Any:
        extra_kwargs = dict(self.agent_params)
        if self.toolsets:
            extra_kwargs["toolsets"] = self.toolsets
        agent: Agent[None, Any] = self.llm_hook.create_agent(
            output_type=self.output_type,
            instructions=self.system_prompt,
            **extra_kwargs,
        )

        result = agent.run_sync(self.prompt)
        output = result.output

        if isinstance(output, BaseModel):
            return output.model_dump()
        return output
