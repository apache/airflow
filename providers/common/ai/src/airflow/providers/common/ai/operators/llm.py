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

from collections.abc import Iterable, Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output
from airflow.providers.common.ai.utils.usage import coerce_usage_limits
from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, BaseOperator
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS

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
        :class:`~pydantic_ai.usage.UsageLimits` enforced on the run, or a dict
        of the same fields (e.g.
        ``{"cost_limit": "{{ params.budget }}", "request_limit": 5}``). The dict
        form is templated: each value is rendered by Jinja like any other
        ``template_fields`` entry, then coerced to that field's type (``Decimal``,
        ``int``, or ``bool``). A value that cannot be coerced -- a Variable
        that exists but is empty renders to ``""``, a typo renders to a
        non-numeric string -- fails the task with a ``ValueError`` naming the
        field and the rendered value, instead of silently disabling the
        limit. A ``UsageLimits`` instance passed directly is used as-is and
        is not templated or validated. ``None`` (default) means no
        enforcement.

        A dict that omits ``request_limit`` still gets pydantic-ai's default of
        ``50`` requests -- pass ``"request_limit": None`` explicitly for no
        request cap. This matches building a ``UsageLimits`` directly, but it is
        easy to miss when moving from ``usage_limits=None`` to a dict that only
        sets ``cost_limit``. See :ref:`howto/operator:llm` for the full set of
        caveats.
    :param require_approval: If ``True``, the task defers after generating
        output and waits for a human reviewer to approve or reject via the
        HITL interface.  Default ``False``. Needs Airflow 3.1+.
    :param approval_timeout: Maximum time to wait for a review.  When
        exceeded, ``on_approval_timeout`` decides the outcome.
    :param on_approval_timeout: What to do when ``approval_timeout`` expires
        without a review.  ``"fail"`` (default) fails the task with
        ``HITLTimeoutError``; ``"approve"`` and ``"reject"`` answer the review
        with that option, so the task resumes as if a reviewer had chosen it.
        The chosen option is also pre-highlighted for the reviewer in the HITL
        form.  Requires ``require_approval=True`` and a positive
        ``approval_timeout``.
    :param allow_modifications: If ``True``, the reviewer can edit the output
        before approving.  The modified value is returned as the task result.
        Default ``False``.
    :param approval_notifiers: Notifiers called once the review is open, so a
        reviewer is told about it.  Only takes effect with
        ``require_approval=True``.  A retry re-notifies with the regenerated
        output while the open review keeps the original subject and body.
        Default ``None``.
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
        "usage_limits",
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
        usage_limits: UsageLimits | dict[str, Any] | None = None,
        require_approval: bool = False,
        approval_timeout: timedelta | None = None,
        on_approval_timeout: Literal["fail", "approve", "reject"] = "fail",
        allow_modifications: bool = False,
        approval_notifiers: BaseNotifier | Iterable[BaseNotifier] | None = None,
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
        # No validation here -- see coerce_usage_limits() docstring for why.
        self.usage_limits = usage_limits
        if on_approval_timeout not in ("fail", *LLMApprovalMixin.TIMEOUT_DEFAULTS):
            raise ValueError(
                f"on_approval_timeout must be 'fail', 'approve', or 'reject', got {on_approval_timeout!r}."
            )
        # Checked before the combination rule so an old core reports the core version
        # rather than sending the user to drop an argument that was never the problem.
        if require_approval and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException("require_approval=True needs Airflow 3.1+.")

        if on_approval_timeout != "fail" and not (
            require_approval and approval_timeout is not None and approval_timeout > timedelta(0)
        ):
            raise ValueError(
                f"on_approval_timeout={on_approval_timeout!r} needs require_approval=True and "
                "a positive approval_timeout to fire. "
                "Set both, or leave on_approval_timeout as 'fail'."
            )
        self.require_approval = require_approval
        self.approval_timeout = approval_timeout
        self.on_approval_timeout = on_approval_timeout
        self.allow_modifications = allow_modifications
        if approval_notifiers is None:
            approval_notifiers = []
        elif isinstance(approval_notifiers, BaseNotifier):
            approval_notifiers = [approval_notifiers]
        elif isinstance(approval_notifiers, str) or not isinstance(approval_notifiers, Iterable):
            raise TypeError(
                "approval_notifiers must be a BaseNotifier or an iterable of BaseNotifier instances, "
                f"got {approval_notifiers!r}"
            )
        self.approval_notifiers = list(approval_notifiers)
        for notifier in self.approval_notifiers:
            if not isinstance(notifier, BaseNotifier):
                raise TypeError(f"approval_notifiers must contain BaseNotifier instances, got {notifier!r}")

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """
        Return the correct PydanticAIHook subclass for the configured connection.

        Delegates to :meth:`~PydanticAIHook.get_hook` which looks up
        the connection's ``conn_type`` and instantiates the matching subclass
        (e.g. :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIAzureHook`
        for ``pydanticai_azure`` connections).
        """
        hook_params = {
            "model_id": self.model_id,
        }
        return PydanticAIHook.get_hook(self.llm_conn_id, hook_params=hook_params)

    def execute(self, context: Context) -> Any:
        if self.require_approval:
            self.validate_approval_prompt()  # type: ignore[misc]

        # Coerced first so a bad rendered value fails before the expensive setup below.
        usage_limits = coerce_usage_limits(self.usage_limits)

        agent: Agent[object, Any] = self.llm_hook.create_agent(
            output_type=self.output_type, instructions=self.system_prompt, **self.agent_params
        )
        result = agent.run_sync(self.prompt, usage_limits=usage_limits)
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
