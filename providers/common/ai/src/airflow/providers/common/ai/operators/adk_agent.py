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
"""Operator for running Google ADK agents with tools and multi-turn reasoning."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.ai.hooks.adk import AdkHook
    from airflow.sdk import Context


class AdkAgentOperator(BaseOperator):
    """
    Run a Google ADK Agent with tools and multi-turn reasoning.

    Uses Google's Agent Development Kit (``google-adk``) to orchestrate
    multi-turn agent interactions with Gemini models. Tools are supplied as
    plain Python callables whose docstrings are exposed to the LLM.

    :param prompt: The prompt to send to the agent.
    :param model_id: Model identifier (e.g. ``"gemini-2.5-flash"``).
    :param llm_conn_id: Airflow connection ID for the LLM provider.
        When provided, API key and model can be read from the connection.
    :param system_prompt: System-level instructions for the agent.
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output.
    :param tools: List of plain callables exposed as tools to the agent.
        Each callable's docstring is sent to the LLM so it knows when and
        how to call it.
    :param agent_params: Additional keyword arguments passed to the ADK
        ``Agent`` constructor.
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
        model_id: str,
        llm_conn_id: str = "",
        system_prompt: str = "",
        output_type: type = str,
        tools: list[Callable] | None = None,
        agent_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.prompt = prompt
        self.model_id = model_id
        self.llm_conn_id = llm_conn_id
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.tools = tools
        self.agent_params = agent_params or {}

    @cached_property
    def hook(self) -> AdkHook:
        """Return AdkHook for the configured connection."""
        from airflow.providers.common.ai.hooks.adk import AdkHook

        return AdkHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

    def execute(self, context: Context) -> Any:
        extra_kwargs = dict(self.agent_params)
        adk_tools = list(self.tools or [])
        if adk_tools:
            extra_kwargs["tools"] = adk_tools

        agent = self.hook.create_agent(
            instruction=self.system_prompt,
            description=extra_kwargs.pop("description", "Airflow AdkAgentOperator agent"),
            **extra_kwargs,
        )

        output = self.hook.run_agent_sync(agent=agent, prompt=self.prompt)

        self.log.info("ADK agent run complete for model=%s", self.model_id)

        if isinstance(output, BaseModel):
            return output.model_dump()
        return output
