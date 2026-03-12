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
    BaseHook,
    BaseOperator,
    BaseOperatorLink,
)
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.providers.common.ai.hooks.base import BaseAIHook
    from airflow.providers.common.compat.sdk import TaskInstanceKey
    from airflow.sdk import Context


class HITLReviewLink(BaseOperatorLink):
    """
    Link that opens the live chat window for a running feedback session.

    The URL is constructed directly from the task instance key so that the
    link is available immediately -- even while the task is still running --
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
    Run an AI agent with tools and multi-turn reasoning.

    Works with **any** AI framework that implements
    :class:`~airflow.providers.common.ai.hooks.base.BaseAIHook` -- the backend
    is selected by the **connection type**, not by choosing a different operator.

    Supported backends:

    * **pydantic-ai** (connection type ``pydanticai``) -- default.  Uses
      `pydantic-ai <https://ai.pydantic.dev/>`__ agents.  All toolsets
      (``SQLToolset``, ``HookToolset``, ``MCPToolset``) work natively.
    * **Google ADK** (connection type ``adk``) -- uses Google's
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

    **HITL Review parameters** (requires the ``hitl_review`` plugin, pydantic-ai only):

    :param enable_hitl_review: When ``True``, the operator enters an
        iterative review loop after the first generation.  A human reviewer
        can approve, reject, or request changes via the plugin's REST API
        at ``/hitl-review`` or through the **HITL Review** extra link
        on the task instance.  Default ``False``.
    :param max_hitl_iterations: Maximum outputs shown to the reviewer (1 =
        initial output). When the reviewer requests changes at
        iteration >= this limit, the task fails with ``HITLMaxIterationsError``
        without calling the LLM. E.g. 5 allows changes at iterations 1--4.
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
        # HITL Review parameters
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
    def llm_hook(self) -> BaseAIHook:
        """
        Resolve the AI hook from the connection type.

        Checks the connection's ``conn_type`` to pick the right hook:

        * ``adk`` -> :class:`~airflow.providers.common.ai.hooks.adk.AdkHook`
        * ``pydanticai*`` -> delegates to
          :meth:`~PydanticAIHook.get_hook` which instantiates the correct
          subclass (e.g. ``PydanticAIAzureHook`` for ``pydanticai-azure``).

        This allows users to switch backends by changing the connection type,
        not the operator class.
        """
        conn_type = self._resolve_conn_type()

        if conn_type == "adk":
            from airflow.providers.common.ai.hooks.adk import AdkHook

            return AdkHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

        hook_params = {
            "model_id": self.model_id,
        }
        return PydanticAIHook.get_hook(self.llm_conn_id, hook_params=hook_params)

    def _resolve_conn_type(self) -> str:
        """Look up the connection type for ``llm_conn_id``."""
        try:
            conn = BaseHook.get_connection(self.llm_conn_id)
            return conn.conn_type or "pydanticai"
        except Exception:
            return "pydanticai"

    def _build_agent(self) -> Agent[None, Any]:
        """Build and return an Agent from the operator's config."""
        extra_kwargs = dict(self.agent_params)

        # Prepare toolsets -- wrap for logging when using pydantic-ai.
        resolved_toolsets = self.toolsets
        if self.toolsets and self.enable_tool_logging:
            hook_type = self._resolve_conn_type()
            if hook_type != "adk":
                resolved_toolsets = wrap_toolsets_for_logging(self.toolsets, self.log)

        return self.llm_hook.create_agent(
            output_type=self.output_type,
            instructions=self.system_prompt,
            toolsets=resolved_toolsets,
            **extra_kwargs,
        )

    def execute(self, context: Context) -> Any:
        if self.enable_hitl_review:
            # HITL path: run agent directly to access message_history
            # for the iterative feedback loop (pydantic-ai only).
            agent = self._build_agent()
            result = agent.run_sync(self.prompt)
            log_run_summary(self.log, result)
            output = result.output

            result_str = self.run_hitl_review(  # type: ignore[misc]
                context,
                output,
                message_history=result.all_messages(),
            )
            # Deserialize back to dict when output_type is a BaseModel
            try:
                return json.loads(result_str)
            except (ValueError, TypeError):
                return result_str

        # Standard path: delegate to hook's run_agent (supports all backends).
        agent = self._build_agent()
        output = self.llm_hook.run_agent(agent=agent, prompt=self.prompt)

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
