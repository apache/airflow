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
"""Operator for general-purpose LLM calls."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output
from airflow.providers.common.compat.sdk import BaseOperator

try:
    # New enough cores register an operator's declared ``output_type`` classes for
    # XCom deserialization from a worker-side walk over the loaded DAG. On those
    # cores the model instance flows through XCom unchanged. Older cores lack that
    # walk, so the operator dumps to a dict instead (still deserializable anywhere).
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:  # pragma: no cover - cores before the worker-side registration walk
    _CORE_WALKER = False

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.usage import UsageLimits

    from airflow.sdk import Context


class LLMOperator(BaseOperator, LLMApprovalMixin):
    """
    Call an LLM with a prompt and return the output.

    Uses a :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    for LLM access. Supports plain string output (default) and structured output
    via a Pydantic ``BaseModel``. When ``output_type`` is a ``BaseModel`` subclass,
    the model instance is returned to XCom unchanged so downstream tasks can
    type-hint it directly (e.g. ``def downstream(result: MyModel) -> None``).
    The class is auto-registered for deserialization in each process that parses
    the DAG, so no edit to ``[core] allowed_deserialization_classes`` is required.
    The Pydantic class must be defined at module scope: classes nested inside
    a function or ``@dag``-decorated body cannot be deserialized from XCom.

    :param prompt: The prompt to send to the LLM.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
        Overrides the model stored in the connection's extra field.
    :param system_prompt: System-level instructions for the LLM agent.
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output; the model instance is
        returned to XCom unchanged so downstream tasks can type-hint it
        directly. The class must be defined at module scope -- nested classes
        cannot be deserialized from XCom.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``, ``tools``).
        See `pydantic-ai Agent docs <https://ai.pydantic.dev/api/agent/>`__
        for the full list.
    :param usage_limits: Optional pydantic-ai
        :class:`~pydantic_ai.usage.UsageLimits` enforced on the run. Pass
        ``UsageLimits(request_limit=..., total_tokens_limit=..., ...)`` to fail
        the task when the agent exceeds the configured token, request, or tool
        budget. ``None`` (default) means no enforcement.
    :param require_approval: If ``True``, the task defers after generating
        output and waits for a human reviewer to approve or reject via the
        HITL interface.  Default ``False``.
    :param approval_timeout: Maximum time to wait for a review.  When
        exceeded, the task fails with ``TimeoutError``.
    :param allow_modifications: If ``True``, the reviewer can edit the output
        before approving.  The modified value is returned as the task result.
        Default ``False``.
    :param serialize_output: If ``True`` and ``output_type`` is a Pydantic
        ``BaseModel`` subclass, the model instance is dumped to a ``dict`` via
        ``model_dump()`` before being pushed to XCom. Default ``False`` --
        the Pydantic instance flows through XCom unchanged. Set to ``True``
        when a downstream consumer needs the dict shape (e.g. sending to an
        external system that expects JSON-style payloads).
    """

    deserialization_allowed_class_fields: ClassVar[tuple[str, ...]] = ("output_type",)

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
        agent_params: dict[str, Any] | None = None,
        usage_limits: UsageLimits | None = None,
        require_approval: bool = False,
        approval_timeout: timedelta | None = None,
        allow_modifications: bool = False,
        serialize_output: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.serialize_output = serialize_output
        # Return the Pydantic instance when the core can register ``output_type``
        # for deserialization (its worker-side DAG walk); otherwise, or when the
        # user opts in, dump to a dict so the value is deserializable anywhere.
        self._serialize_model_output = serialize_output or not _CORE_WALKER
        self.agent_params = agent_params or {}
        self.usage_limits = usage_limits
        self.require_approval = require_approval
        self.approval_timeout = approval_timeout
        self.allow_modifications = allow_modifications

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """
        Return the correct PydanticAIHook subclass for the configured connection.

        Delegates to :meth:`~PydanticAIHook.get_hook` which looks up
        the connection's ``conn_type`` and instantiates the matching subclass
        (e.g. :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIAzureHook`
        for ``pydanticai-azure`` connections).
        """
        hook_params = {
            "model_id": self.model_id,
        }
        return PydanticAIHook.get_hook(self.llm_conn_id, hook_params=hook_params)

    def execute(self, context: Context) -> Any:
        if self.require_approval and not isinstance(self.prompt, str):
            raise TypeError(
                f"{type(self).__name__}: require_approval=True is not supported "
                f"with a non-string prompt (got {type(self.prompt).__name__}). "
                f"The approval review body renders the prompt as text. Return a "
                f"str prompt, or disable require_approval."
            )

        agent: Agent[None, Any] = self.llm_hook.create_agent(
            output_type=self.output_type, instructions=self.system_prompt, **self.agent_params
        )
        result = agent.run_sync(self.prompt, usage_limits=self.usage_limits)
        log_run_summary(self.log, result)
        output = result.output

        if self.require_approval:
            self.defer_for_approval(context, output)  # type: ignore[misc]

        if self._serialize_model_output and isinstance(output, BaseModel):
            # ``serialize_output=True``, or a core without the worker-side
            # deserialization-class walk: dump to a dict so XCom carries a plain
            # JSON payload that deserializes without an allow-list entry.
            output = output.model_dump()

        return output

    def execute_complete(self, context: Context, generated_output: str, event: dict[str, Any]) -> Any:
        """Resume after human review and restore the Pydantic model for XCom consumers."""
        output = super().execute_complete(context, generated_output, event)
        return rehydrate_pydantic_output(
            self.output_type, output, serialize_output=self._serialize_model_output
        )
