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

import json
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.utils.logging import log_run_summary, wrap_toolsets_for_logging
from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
    BaseOperatorLink,
)
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.providers.common.compat.sdk import TaskInstanceKey
    from airflow.sdk import Context


class HITLReviewLink(BaseOperatorLink):
    """
    Link that opens the live chat window for a running feedback session.

    The URL is constructed directly from the task instance key so that the
    link is available immediately — even while the task is still running —
    without waiting for an XCom value to be committed.
    """

    name = "HITL Review"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        if not getattr(operator, "enable_hitl_review", False):
            return ""
        from urllib.parse import urlparse

        from airflow.configuration import conf

        base_url = conf.get("api", "base_url", fallback="/")
        if base_url.startswith(("http://", "https://")):
            base_path = urlparse(base_url).path.rstrip("/")
        else:
            base_path = base_url.rstrip("/")
        mapped = f"/mapped/{ti_key.map_index}" if ti_key.map_index >= 0 else ""
        return (
            f"{base_path}/dags/{ti_key.dag_id}/runs/{ti_key.run_id}"
            f"/tasks/{ti_key.task_id}{mapped}/plugin/hitl-review"
        )


class AgentOperator(BaseOperator, HITLReviewMixin):
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
    :param enable_tool_logging: When ``True`` (default), wraps each toolset in a
        ``LoggingToolset`` that logs tool calls with timing at INFO level and
        arguments at DEBUG level. Set to ``False`` to disable.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``).

    **HITL Review parameters** (requires the ``hitl_review`` plugin):

    :param enable_hitl_review: When ``True``, the operator enters an
        iterative review loop after the first generation.  A human reviewer
        can approve, reject, or request changes via the plugin's REST API
        at ``/hitl-review`` or through the **HITL Review** extra link
        on the task instance.  Default ``False``.
    :param max_hitl_iterations: Maximum outputs shown to the reviewer (1 =
        initial output). When the reviewer requests changes at
        iteration >= this limit, the task fails with ``HITLMaxIterationsError``
        without calling the LLM. E.g. 5 allows changes at iterations 1–4.
        Default ``5``.
    :param hitl_timeout: Maximum wall-clock time to wait for
        all review rounds combined.  ``None`` means no timeout (the
        operator blocks until a terminal action).
    :param hitl_poll_interval: Seconds between XCom polls
        while waiting for a human response.  Default ``10``.
    """

    template_fields: Sequence[str] = (
        "prompt",
        "llm_conn_id",
        "model_id",
        "system_prompt",
        "agent_params",
    )

    operator_extra_links = (HITLReviewLink(),)

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
        # Agent feedback parameters
        enable_hitl_review: bool = False,
        max_hitl_iterations: int = 5,
        hitl_timeout: timedelta | None = None,
        hitl_poll_interval: float = 10.0,
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

        self.enable_hitl_review = enable_hitl_review
        self.max_hitl_iterations = max_hitl_iterations
        self.hitl_timeout = hitl_timeout
        self.hitl_poll_interval = hitl_poll_interval

        if self.enable_hitl_review and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "Human in the loop functionality needs Airflow 3.1+."
            )

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """Return PydanticAIHook for the configured LLM connection."""
        hook_params = {
            "model_id": self.model_id,
        }
        return PydanticAIHook.get_hook(self.llm_conn_id, hook_params=hook_params)

    def _build_agent(self) -> Agent[None, Any]:
        """Build and return a pydantic-ai Agent from the operator's config."""
        extra_kwargs = dict(self.agent_params)
        if self.toolsets:
            if self.enable_tool_logging:
                extra_kwargs["toolsets"] = wrap_toolsets_for_logging(self.toolsets, self.log)
            else:
                extra_kwargs["toolsets"] = self.toolsets
        return self.llm_hook.create_agent(
            output_type=self.output_type,
            instructions=self.system_prompt,
            **extra_kwargs,
        )

    def execute(self, context: Context) -> Any:
        agent = self._build_agent()
        result = agent.run_sync(self.prompt)
        log_run_summary(self.log, result)
        output = result.output

        if self.enable_hitl_review:
            result_str = self.run_hitl_review(  # type: ignore[misc]
                context,
                output,
                message_history=result.all_messages(),
            )
            # Deserialize back to dict
            try:
                return json.loads(result_str)
            except (ValueError, TypeError):
                return result_str

        if isinstance(output, BaseModel):
            return output.model_dump()
        return output

    def regenerate_with_feedback(self, *, feedback: str, message_history: Any) -> tuple[str, Any]:
        """Re-run the agent with *feedback* appended to the conversation history."""
        agent = self._build_agent()
        messages = message_history or []
        result = agent.run_sync(feedback, message_history=messages)
        log_run_summary(self.log, result)

        output = result.output
        if isinstance(output, BaseModel):
            output = output.model_dump_json()
        return str(output), result.all_messages()
