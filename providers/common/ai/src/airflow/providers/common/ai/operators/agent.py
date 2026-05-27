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
"""Operator for running LLM agents with tools and multi-turn reasoning."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.base_ai import AgentRunRequest, BaseAIHook, DurableContext
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.ai.utils.validation import reject_sequence_with_unsupported_feature
from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
    BaseOperatorLink,
    conf,
)
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from pydantic_ai.usage import UsageLimits

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
    Run an LLM agent with tools and multi-turn reasoning.

    Provide ``llm_conn_id`` and optional ``toolsets`` to let the operator build
    and run the agent. The agent reasons about the prompt, calls tools in a
    multi-turn loop, and returns a final answer.

    The agent backend is selected by the connection ``conn_type`` (for example
    ``pydanticai``, ``pydanticai-bedrock``, or ``pydanticai-azure``).

    :param prompt: The prompt to send to the agent.
    :param llm_conn_id: Connection ID for the agent provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
        Overrides the model stored in the connection's extra field.
    :param system_prompt: System-level instructions for the agent.
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output.
    :param toolsets: List of :class:`~airflow.providers.common.ai.hooks.base_ai.BaseToolset`
        instances the agent can use.
    :param enable_tool_logging: When ``True`` (default), wraps each tool callable with a
        logging shim that logs calls with timing at INFO level and arguments at DEBUG level.
        Set to ``False`` to disable.
    :param agent_params: Additional keyword arguments passed to the underlying agent
        constructor (e.g. ``retries``, ``model_settings``).
    :param usage_limits: Optional
        :class:`~pydantic_ai.usage.UsageLimits` enforced on every agent run
        (initial run, durable replay, and HITL regeneration). Pass
        ``UsageLimits(request_limit=..., total_tokens_limit=..., tool_calls_limit=..., ...)``
        to fail the task when the agent exceeds the configured token, request,
        or tool budget. ``None`` (default) means no enforcement.
    :param durable: When ``True``, enables step-level caching of model
        responses and tool results for durable execution.  On retry, cached
        steps are replayed instead of re-executing.  Default ``False``.
        Requires ``[common.ai] durable_cache_path`` to be set.

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
        toolsets: list[Any] | None = None,
        enable_tool_logging: bool = True,
        agent_params: dict[str, Any] | None = None,
        usage_limits: UsageLimits | None = None,
        durable: bool = False,
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
        self.usage_limits = usage_limits

        self.durable = durable

        if durable and enable_hitl_review:
            raise ValueError("durable=True and enable_hitl_review=True cannot be used together.")

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
        """Return the agent hook for the configured connection (resolved from ``conn_type``)."""
        hook_params = {
            "model_id": self.model_id,
        }
        return BaseAIHook.get_agent_hook(self.llm_conn_id, hook_params=hook_params)

    def _build_request(self, *, prompt: str, message_history: Any = None) -> AgentRunRequest:
        """Build an :class:`~airflow.providers.common.ai.hooks.base_ai.AgentRunRequest` from operator config."""
        durable_context: DurableContext | None = None
        if self.durable and hasattr(self, "_durable_ti") and self._durable_ti is not None:
            ti = self._durable_ti
            durable_context = DurableContext(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                map_index=ti.map_index if ti.map_index is not None else -1,
            )
        return AgentRunRequest(
            prompt=prompt,
            output_type=self.output_type,
            instructions=self.system_prompt,
            toolsets=self.toolsets,
            usage_limits=self.usage_limits,
            message_history=message_history,
            enable_tool_logging=self.enable_tool_logging,
            durable_context=durable_context,
            agent_params=dict(self.agent_params),
        )

    def execute(self, context: Context) -> Any:
        reject_sequence_with_unsupported_feature(
            self.prompt,
            decorator_name=type(self).__name__,
            feature_name="enable_hitl_review",
            feature_enabled=self.enable_hitl_review,
        )
        self._durable_ti = context["task_instance"] if self.durable else None

        request = self._build_request(prompt=self.prompt)
        agent = self.llm_hook.create_agent(request)
        run_result = self.llm_hook.run_agent(agent, request)

        log_run_summary(self.log, run_result)

        if run_result.durable_stats is not None:
            c = run_result.durable_stats
            replayed = c.replayed_model + c.replayed_tool
            cached = c.cached_model + c.cached_tool
            if replayed:
                self.log.info(
                    "Durable: replayed %d cached steps (%d model, %d tool), "
                    "executed %d new steps (%d model, %d tool)",
                    replayed,
                    c.replayed_model,
                    c.replayed_tool,
                    cached,
                    c.cached_model,
                    c.cached_tool,
                )

        output = run_result.output

        if self.enable_hitl_review:
            result_str = self.run_hitl_review(  # type: ignore[misc]
                context,
                output,
                message_history=run_result.message_history,
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
        request = self._build_request(prompt=feedback, message_history=message_history)
        agent = self.llm_hook.create_agent(request)
        run_result = self.llm_hook.run_agent(agent, request)
        log_run_summary(self.log, run_result)

        output = run_result.output
        if isinstance(output, BaseModel):
            output = output.model_dump_json()
        return str(output), run_result.message_history
