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

import copy
import hashlib
import json
import sys
import uuid
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass, field, replace
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn, TypeGuard

from pydantic import TypeAdapter
from pydantic_ai import DeferredToolRequests, DeferredToolResults, ToolDenied
from pydantic_ai.capabilities import (
    AbstractCapability,
    CapabilityOrdering,
    Toolset,
    WrapperCapability,
    durable_operation,
)
from pydantic_ai.exceptions import UserError
from pydantic_ai.messages import ModelMessagesTypeAdapter, ModelResponse, ToolCallPart

# Resolved at run time: pydantic-ai reads the durable operation's signature to build its schema.
from pydantic_ai.tools import RunContext  # noqa: TC002
from pydantic_ai.toolsets import DynamicToolset
from pydantic_ai.toolsets.abstract import AbstractToolset
from pydantic_ai.usage import RunUsage
from pydantic_core import to_jsonable_python
from typing_extensions import TypedDict

from airflow.providers.common.ai.durable.capability import (
    DURABLE_UNIT_TOOLSETS,
    AirflowDurability,
    _get_chosen_toolset_id,
    _give_durable_ids,
)
from airflow.providers.common.ai.durable.fingerprint import fingerprint_tool_call
from airflow.providers.common.ai.durable.journal import (
    DurableJournal,
    JournalStats,
    build_task_storage,
    journal_scope,
)
from airflow.providers.common.ai.exceptions import (
    HITLMaxIterationsError,
    ToolApprovalAlreadyRequestedError,
    ToolApprovalError,
    UnsupportedToolDeferralError,
)
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin, normalize_assigned_users
from airflow.providers.common.ai.mixins.cancellable_run import CancellableAgentRunMixin
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.mixins.usage_budget import UsageBudgetMixin
from airflow.providers.common.ai.observability import (
    build_run_identity_attributes,
    make_task_instance_run_key,
    stamp_identity_on_agent_spans,
)
from airflow.providers.common.ai.toolsets.logging import ToolLoggingCapability
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.utils.logging import (
    MODEL_NAME_XCOM_KEY,
    format_usage_for_xcom,
    log_run_summary,
    log_run_usage,
)
from airflow.providers.common.ai.utils.output_type import (
    dump_output_for_xcom,
    dump_output_to_json,
    rehydrate_pydantic_output,
)
from airflow.providers.common.ai.utils.prompt_cache import PromptCaching
from airflow.providers.common.ai.utils.toolset_base import ensure_masked
from airflow.providers.common.ai.utils.toolsets import iter_toolsets
from airflow.providers.common.ai.utils.usage import coerce_usage_limits
from airflow.providers.common.ai.utils.usage_budget import (
    TaskStateStoreUsageBudget,
    copy_run_usage,
    subtract_run_usage,
)
from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
    BaseOperatorLink,
    conf,
    redact,
)
from airflow.providers.common.compat.version_compat import (
    AIRFLOW_V_3_1_PLUS,
    AIRFLOW_V_3_3_PLUS,
    AIRFLOW_V_3_4_PLUS,
)
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError, HITLTriggerEventError

if AIRFLOW_V_3_3_PLUS:
    # Per-tool approval parks the task in AWAITING_INPUT, which older Airflow versions do not have.
    from airflow.sdk.exceptions import TaskAwaitingInput
    from airflow.sdk.execution_time.context import NEVER_EXPIRE
    from airflow.sdk.execution_time.hitl import upsert_hitl_detail

try:
    # See LLMOperator: Newer ``apache-airflow-task-sdk`` versions register declared ``output_type`` classes
    # from a worker-side DAG walk, so the model instance flows through XCom; older
    # ``apache-airflow-task-sdk`` versions without the walk dump to a dict instead.
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:  # pragma: no cover - missing ``apache-airflow-task-sdk`` walker
    _CORE_WALKER = False

if TYPE_CHECKING:
    from types import UnionType

    import jinja2
    from pydantic_ai import Agent
    from pydantic_ai.capabilities import AgentCapability
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.output import OutputSpec
    from pydantic_ai.toolsets import ToolsetFunc
    from pydantic_ai.usage import UsageLimits

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.providers.common.ai.durable.capability import _GivenIds
    from airflow.providers.common.ai.utils.hitl_review import AgentSessionData
    from airflow.providers.common.compat.sdk import TaskInstanceKey
    from airflow.sdk import Context
    from airflow.sdk.execution_time.context import TaskStateStoreAccessor
    from airflow.sdk.execution_time.hitl import HITLUser

# Task state store keys: the transcript of a run paused for tool approval, and, on Airflow 3.3,
# a marker that this task instance has asked once. The store is keyed by Dag run, task and map
# index, so the marker survives retries and clears, as the task instance's approval request does.
_TOOL_APPROVAL_TRANSCRIPT_KEY = "common_ai_tool_approval_transcript"
_TOOL_APPROVAL_REQUESTED_KEY = "common_ai_tool_approval_requested"
# Under durable, every answer a reviewer gave this task instance's agent run, kept with the journal
# and deleted with it, so a resume, a later pause or a retry applies each to the calls it was for.
_TOOL_APPROVAL_ANSWERS_KEY = "common_ai_tool_approval_answers"
# Under durable, each request for changes in the HITL review, kept beside the journal until the review
# is approved or ends without approval, so a retry applies them to the outputs it replays instead of
# asking the reviewer again.
_HITL_REVIEW_ANSWERS_KEY = "common_ai_hitl_review_answers"
# Airflow 3.4 replaces a repeat approval request's details and clears the earlier answer, so a
# run can pause again; see ``_pause_for_tool_approval`` for why 3.3 cannot.
_REPEAT_APPROVAL_SUPPORTED = AIRFLOW_V_3_4_PLUS
# A hidden form field holding the pause number. The review form sends back the values it loaded,
# so a form left open on an earlier pause answers with that pause's number, which the resume
# refuses to apply to the calls that are waiting now.
_APPROVAL_REQUEST_PARAM = "request"
# How long the transcript outlives a timed pause, so a resume that runs late still finds it.
_TRANSCRIPT_RETENTION_MARGIN = timedelta(days=1)
_RUN_USAGE_ADAPTER: TypeAdapter[RunUsage] = TypeAdapter(RunUsage)
_STORED_ANSWERS_ADAPTER: TypeAdapter[list[dict[str, Any]]] = TypeAdapter(list[dict[str, Any]])
# Names the durable approval step in the journal, so it must never change.
_TOOL_APPROVAL_CAPABILITY_ID = "airflow_tool_approval"
# The method a durable run paused for tool approval resumes with.
_DURABLE_APPROVAL_RESUME = "resume_after_durable_tool_approval"


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


def _resolve_capability_toolset(capability: object) -> AbstractToolset[Any] | None:
    """Return the toolset a ``Toolset`` capability holds; ``None`` for a factory resolved per run or any other capability."""
    if isinstance(capability, Toolset) and isinstance(capability.toolset, AbstractToolset):
        return capability.toolset
    return None


def _sandboxes_in(toolsets: Iterable[AbstractToolset[Any]]) -> list[SandboxToolset]:
    """Every ``SandboxToolset`` in ``toolsets``, looked for inside wrappers and combinations."""
    return [
        nested
        for toolset in toolsets
        for nested in iter_toolsets(toolset)
        if isinstance(nested, SandboxToolset)
    ]


def _replace_capability_toolset(
    capability: AgentCapability[Any], wrap: Callable[[AbstractToolset[Any]], AbstractToolset[Any]]
) -> AgentCapability[Any]:
    """Return a ``Toolset`` capability holding ``wrap(toolset)``; any other capability comes back as is."""
    if isinstance(capability, Toolset) and isinstance(capability.toolset, AbstractToolset):
        return replace(capability, toolset=wrap(capability.toolset))
    return capability


def _iter_capabilities(
    capabilities: Iterable[AgentCapability[Any]],
) -> Iterator[tuple[AbstractCapability[Any], bool]]:
    """
    Yield every capability, and those nested inside combined or wrapper capabilities, with whether it is top-level.

    A capability function, or a ``DynamicCapability``, builds its capability when the run
    starts, so there is nothing to inspect here.
    """
    pending = [
        (capability, True) for capability in capabilities if isinstance(capability, AbstractCapability)
    ]
    while pending:
        capability, top_level = pending.pop()
        yield capability, top_level
        if isinstance(capability, WrapperCapability):
            # apply() does not visit a wrapper's single wrapped capability, only a combined one's children.
            pending.append((capability.wrapped, False))
        else:
            children: list[AbstractCapability[Any]] = []
            capability.apply(children.append)
            pending.extend((child, False) for child in children if child is not capability)


def _contains_capability(
    capabilities: Iterable[AgentCapability[Any]], capability_type: type[AbstractCapability[Any]]
) -> bool:
    """Whether any capability, or one nested inside a combined or wrapper capability, has the given type."""
    return any(isinstance(capability, capability_type) for capability, _ in _iter_capabilities(capabilities))


def _contains_code_mode(capabilities: Iterable[AgentCapability[Any]]) -> bool:
    """Whether the declared capabilities contain ``CodeMode``."""
    # CodeMode's own module is in sys.modules once CodeMode has been imported, and only then.
    # The pydantic_ai_harness package root is not a safe place to look: it exports CodeMode
    # through a module __getattr__ that imports that module, which fails without the
    # ``code-mode`` extra.
    code_mode_cls = getattr(sys.modules.get("pydantic_ai_harness.code_mode"), "CodeMode", None)
    return code_mode_cls is not None and _contains_capability(capabilities, code_mode_cls)


def _capability_toolset_without_id(capabilities: Iterable[AgentCapability[Any]]) -> str | None:
    """
    Say why a ``Toolset`` capability's steps would have no name under durable execution, or return ``None``.

    The operator gives ids only to a toolset a top-level ``Toolset`` capability holds. One that
    builds its toolset per run, or sits inside a combined or wrapper capability, keeps what it
    has, and pydantic-ai refuses to run a durable agent whose toolset has no id.
    """
    for capability, top_level in _iter_capabilities(capabilities):
        if not isinstance(capability, Toolset):
            continue
        if (toolset := _resolve_capability_toolset(capability)) is None:
            return (
                "a Toolset capability builds its toolset when the run starts, so durable replay "
                "has no id to record its tool calls under. Pass the toolset in toolsets=... instead."
            )
        if not top_level and any(
            isinstance(leaf, DURABLE_UNIT_TOOLSETS) and _get_chosen_toolset_id(leaf) is None
            for leaf in iter_toolsets(toolset)
        ):
            return (
                "a toolset without an id sits inside a combined or wrapper capability, so durable "
                "replay has no id to record its tool calls under. Give it an id, such as "
                "FunctionToolset(id='orders')."
            )
    return None


def _answered_request(event: dict[str, Any]) -> int | None:
    """Return the approval request a review answer was for, or ``None`` when it does not say."""
    answered = (event.get("params_input") or {}).get(_APPROVAL_REQUEST_PARAM)
    if isinstance(answered, dict):
        # Some UI versions send back the whole parameter spec instead of its value.
        answered = answered.get("value")
    if answered is None or answered == "":
        # An API caller that sent only the chosen option.
        return None
    try:
        return int(answered)
    except (TypeError, ValueError):
        # Unreadable, so it matches no request and the calls are asked about again.
        return -1


def _is_awaiting_input(error: BaseException) -> TypeGuard[TaskAwaitingInput]:
    """Whether ``error`` is the pause for tool approval, which only Airflow 3.3+ raises."""
    return AIRFLOW_V_3_3_PLUS and isinstance(error, TaskAwaitingInput)


def _refuse_external_calls(requests: DeferredToolRequests) -> None:
    """Fail on tool calls that hand their work to an external system, which AgentOperator cannot resolve."""
    if requests.calls:
        raise UnsupportedToolDeferralError(
            "The agent called tools that need external execution "
            f"({', '.join(call.tool_name for call in requests.calls)}); AgentOperator only "
            "supports tools that need approval."
        )


def _refuse_unsupported_approval(requests: DeferredToolRequests) -> NoReturn:
    """Fail on tool calls that need approval where the task cannot pause for it."""
    raise UnsupportedToolDeferralError(
        "The agent called tools that need approval "
        f"({', '.join(call.tool_name for call in requests.approvals)}), but tool approval "
        "needs Airflow 3.3+ and is not available with enable_hitl_review, a CodeMode "
        "capability or a SandboxToolset."
    )


def _fingerprint_call_content(call: ToolCallPart) -> str | None:
    """Fingerprint what a reviewer sees of a call, its tool and arguments, but not the id the model gave it."""
    return fingerprint_tool_call(call.tool_name, call.args_as_dict(), None)


@dataclass
class _ToolApprovalAnswer:
    """
    A reviewer's answer, and what it was given for.

    :param step: Which approval step of the run asked, counting replayed ones, so the answer
        never reaches another step, such as a later call with the same arguments.
    :param calls: The calls the reviewer saw, as ``(tool name, content fingerprint)``.
    :param toolset_ids: The agent's rendered toolset ids when the reviewer answered.
    """

    step: int
    calls: list[tuple[str, str | None]]
    toolset_ids: list[str]
    approval: bool | ToolDenied

    def take(self, pending: Sequence[ToolCallPart]) -> dict[str, bool | ToolDenied]:
        """
        Return the answer for each pending call it was given for, by the call's current id.

        A call matches on its tool and arguments, so a model request asked again on resume, whose
        calls get new ids, still gets the answer, and a different call with a reused id does not.
        Each answered call is used once: a step asked again can hold the same call twice where
        the reviewer saw it once.
        """
        settled: dict[str, bool | ToolDenied] = {}
        for call in pending:
            answered = (call.tool_name, _fingerprint_call_content(call))
            if answered[1] is not None and answered in self.calls:
                self.calls.remove(answered)
                settled[call.tool_call_id] = self.approval
        return settled

    def dump(self) -> dict[str, Any]:
        denied = self.approval.message if isinstance(self.approval, ToolDenied) else None
        return {
            "step": self.step,
            "calls": [list(call) for call in self.calls],
            "toolset_ids": self.toolset_ids,
            "denied": denied,
        }

    @classmethod
    def load(cls, stored: dict[str, Any]) -> _ToolApprovalAnswer:
        denied = stored["denied"]
        return cls(
            step=stored["step"],
            calls=[(name, fingerprint) for name, fingerprint in stored["calls"]],
            toolset_ids=stored["toolset_ids"],
            approval=True if denied is None else ToolDenied(denied),
        )


class _ReviewAnswer(TypedDict):
    """A request for changes, with its review round and the sha256 of the output the reviewer read."""

    iteration: int
    output_sha256: str
    feedback: str


_REVIEW_ANSWERS_ADAPTER: TypeAdapter[list[_ReviewAnswer]] = TypeAdapter(list[_ReviewAnswer])


@dataclass
class _DurableToolApproval(AbstractCapability[Any]):
    """
    Settles the tool calls a durable run is waiting to have approved, as a step of its journal.

    It runs first (outermost) and settles every call or parks the task, so the whole decision is
    one step, ``decide``: the reviewers' answers, then the other capabilities'
    ``handle_deferred_tool_calls`` handlers, then a request for whatever is left. A parked step
    is not recorded, so the resumed run, which replays every step before it, reaches it again
    with the answer. Once recorded, a retry replays the decision instead of asking anyone again,
    and replays the approved calls instead of running them twice.

    :param claim_next_step: Number the approval step about to run for these calls, replayed or not,
        or return ``None`` when ``decide`` itself is asking the other handlers.
    :param settle: The body of ``decide``.
    """

    claim_next_step: Callable[[DeferredToolRequests], int | None] = field(repr=False)
    settle: Callable[[RunContext[Any], DeferredToolRequests, int], Awaitable[DeferredToolResults]] = field(
        repr=False
    )

    @classmethod
    def get_serialization_name(cls) -> str | None:
        return None

    def get_ordering(self) -> CapabilityOrdering:
        # Ahead of any handler the Dag author ordered first, so none decides a call before the reviewer.
        return CapabilityOrdering(position="outermost")

    async def handle_deferred_tool_calls(
        self, ctx: RunContext[Any], *, requests: DeferredToolRequests
    ) -> DeferredToolResults | None:
        if (step := self.claim_next_step(requests)) is None:
            return None
        return await self.decide(ctx, requests=requests, step=step)

    @durable_operation(name="tool_approval")
    async def decide(
        self, ctx: RunContext[Any], *, requests: DeferredToolRequests, step: int
    ) -> DeferredToolResults | None:
        return await self.settle(ctx, requests, step)


def _filter_results_to_requests(
    requests: DeferredToolRequests, results: DeferredToolResults
) -> DeferredToolResults:
    """Keep the results for the calls in ``requests``, so a handler cannot settle a call it was not given."""
    approval_ids = {call.tool_call_id for call in requests.approvals}
    call_ids = {call.tool_call_id for call in requests.calls}
    return DeferredToolResults(
        approvals={i: result for i, result in results.approvals.items() if i in approval_ids},
        calls={i: result for i, result in results.calls.items() if i in call_ids},
        metadata={i: meta for i, meta in results.metadata.items() if i in approval_ids | call_ids},
    )


def _declares_agent_template_fields(toolset: Any) -> bool:
    """Whether *toolset*, or a toolset it wraps or combines, has connection IDs to render."""
    return isinstance(toolset, AbstractToolset) and any(
        getattr(leaf, "agent_template_fields", None) for leaf in iter_toolsets(toolset)
    )


# CancellableAgentRunMixin must precede BaseOperator so its on_kill overrides BaseOperator's
# no-op. The other mixins only add methods, so they can trail BaseOperator. See the MRO guard
# test in tests/unit/common/ai/mixins/test_cancellable_run.py.
class AgentOperator(CancellableAgentRunMixin, BaseOperator, HITLReviewMixin, UsageBudgetMixin):
    """
    Run a pydantic-ai Agent with tools and multi-turn reasoning.

    Provide ``llm_conn_id`` and optional ``toolsets`` to let the operator build
    and run the agent. The agent reasons about the prompt, calls tools in a
    multi-turn loop, and returns a final answer.

    Alongside the returned agent output, the run's ``run_id`` and token ``usage``
    are pushed to XCom under the ``run_id`` and ``usage`` keys, so a downstream
    task can reference the run and its cost. ``usage`` is this attempt's own
    usage, not the cross-attempt cumulative total described under
    ``usage_limits`` below; it is pushed on a failed attempt too, so a
    downstream ``all_done`` task or failure callback can read what the last
    attempt spent -- XCom is cleared at the start of every attempt, so only
    the most recent attempt's value survives, not each historical attempt's.
    The ``run_id`` also ties the task to its GenAI trace (see the provider's
    observability docs). With ``enable_hitl_review``, these reflect the
    initial model run, not the human-feedback regenerations.

    :param prompt: The prompt to send to the agent.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
        Overrides the model stored in the connection's extra field.
    :param fallback_conn_ids: Connection IDs to fail over to, in order, when
        the primary provider is unavailable. Overrides the ``fallback_conn_ids``
        set in the connection's extra field. ``None`` (default) reads the
        connection's own extra field; an explicit ``[]`` disables a chain
        configured there. See
        :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
        for how blank entries in the list are dropped.
    :param system_prompt: System-level instructions for the agent.
    :param output_type: Expected output type. Default ``str``. A single type
        (``str``, ``int``, ``list[str]``, a Pydantic ``BaseModel`` subclass) or a
        union (``A | B``). A list of types (``[str, int]``) is also accepted and treated
        as alternatives (a union). pydantic-ai output markers (``ToolOutput(...)``,
        ``NativeOutput(...)``) and output functions are also accepted.
        For a ``BaseModel`` subclass, the model instance is
        returned to XCom unchanged so downstream tasks can type-hint it
        directly, except inside a list of types, where it is returned as a ``dict``
        because the worker registers only the classes it finds in a single type or a union.
        The class must be defined at module scope -- nested classes
        cannot be deserialized from XCom.
    :param toolsets: List of pydantic-ai toolsets the agent can use
        (e.g. ``SQLToolset``, ``HookToolset``). The connection IDs of
        ``SQLToolset``, ``MCPToolset`` and ``HookToolset`` (its hook's
        ``conn_name_attr``) are templated, e.g.
        ``SQLToolset(db_conn_id="warehouse_{{ var.value.environment }}")`` per
        environment, or ``"tenant_{{ task.op_kwargs.customer }}"`` per map index of
        a mapped ``@task.agent``, and so is ``SandboxToolset.attach_to``, which is
        how ``SandboxToolset(attach_to="{{ ti.xcom_pull('provision') }}")``
        receives the sandbox an upstream task created. Each task instance renders
        its own copy and logs the rendered toolset id; the toolset object in the
        Dag file is not modified. Derive the connection ID from values the Dag
        controls rather than ``params`` or ``dag_run.conf``, which whoever triggers
        the Dag controls.
    :param capabilities: pydantic-ai capabilities for the agent, e.g.
        ``[Thinking(effort="high"), WebSearch()]``. A capability bundles tools,
        instructions, model settings and lifecycle hooks; pydantic-ai wraps their
        hooks in list order, first outermost, unless a capability declares its
        own position. A ``Toolset`` capability holding one of the
        toolsets above has its connection IDs templated the same way as
        ``toolsets=``. Capabilities passed here are not stored in the serialized
        Dag (the worker builds them from the Dag file), except on a mapped task,
        where they are stored as their repr. Passing ``capabilities`` inside
        ``agent_params`` still works, but stores each capability's repr
        in the serialized Dag, and cannot be combined with this argument (the
        task fails when it runs).
    :param enable_tool_logging: When ``True`` (default), the operator appends a
        ``ToolLoggingCapability``, whose ``get_wrapper_toolset`` wraps the agent's
        assembled function toolset so that tool calls are logged
        with timing at INFO level and arguments at DEBUG level. If ``capabilities``
        already holds a ``ToolLoggingCapability``, the operator adds none and that
        one's ``logger`` is used. Logging includes
        tools supplied through ``toolsets=``, ``agent_params["tools"]``, and
        capabilities, but not output tools, provider-native tools that run
        server-side, or tools another capability adds through its own wrapper
        (code mode's ``run_code``, ToolSearch's ``search_tools``). Set to
        ``False`` to disable.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``).
    :param usage_limits: Optional pydantic-ai
        :class:`~pydantic_ai.usage.UsageLimits` enforced on every agent run
        (initial run, durable replay, and HITL regeneration), or a dict of the
        same fields (e.g.
        ``{"cost_limit": "{{ params.budget }}", "request_limit": 5}``). The dict
        form is templated: each value is rendered by Jinja like any other
        ``template_fields`` entry, then coerced to that field's type (``Decimal``,
        ``int``, or ``bool``). A value that cannot be coerced -- a Variable
        that exists but is empty renders to ``""``, a typo renders to a
        non-numeric string -- fails the task with a ``ValueError`` naming the
        field and the rendered value, instead of silently disabling the
        limit. A ``UsageLimits`` instance passed directly is used as-is and
        is not templated or validated. ``None`` (default) sets no token, cost,
        or tool-call limits, but pydantic-ai still caps each run at its default
        ``request_limit`` of ``50`` requests.

        A dict that omits ``request_limit`` gets the same default of ``50``
        requests -- pass ``"request_limit": None`` explicitly for no request
        cap.

        On Airflow >= 3.3, this counts usage across every attempt combined
        -- initial run, retries, and HITL regenerations all add to one
        running total instead of resetting each attempt. Scale each limit
        by ``retries + 1``, or set ``usage_limits=None``, to keep the old
        per-attempt headroom. On Airflow < 3.3, or when ``usage_limits`` is
        ``None``, each attempt is checked and counted on its own, unchanged.
        See :ref:`howto/operator:llm` for the full caveats, and
        :ref:`the cross-attempt usage budget <agent-usage-budget>` for how
        it is persisted, reset, and how ``durable`` replay and HITL
        regeneration interact with it.
    :param durable: Whether a retry replays what earlier attempts completed instead of
        running it again. ``None`` (default) turns it on on Airflow 3.3+ for a task that
        can retry (``retries`` above 0, or from Airflow 3.4 a ``retry_policy``), and off for
        any other task. It also turns it off, with the reason in the task log, where it cannot
        apply: a ``SandboxToolset``, an ``MCPToolset`` without an ``id``, a toolset without an
        ``id`` inside a combined or wrapper capability or built by a ``Toolset`` capability, a
        capability pydantic-ai refuses to run durably (``Capability(toolsets=...)`` with a
        toolset without an ``id`` among them), or a context with no task state store; and it
        leaves the journal to an ``AirflowDurability`` capability passed in ``capabilities``.
        ``True`` turns it on wherever the task runs. It refuses the first three when the Dag
        loads; a capability pydantic-ai refuses fails the task when it runs;
        ``False`` turns it off (:ref:`durable-opt-out`). When on, the operator attaches
        :class:`~airflow.providers.common.ai.durable.AirflowDurability`, which
        records each step the agent completes -- model responses, tool calls from
        any toolset or capability, tool discovery, and other capabilities'
        ``@durable_operation`` methods -- in a journal kept for the task instance.
        On retry the agent runs again from the start and the steps the previous
        attempt completed are replayed from the journal instead of running again.
        Each step is replayed only if it matches what the previous attempt did at
        the same point: if the prompt, model, settings, tools, tool arguments or
        message history changed, that step and every step after it run live (with
        a warning). A replayed step adds nothing to the usage
        counted against ``usage_limits`` or reported in the ``usage`` XCom -- not
        its request, tokens, cost, or tool calls -- so every attempt counts only
        the model and tool calls it actually makes. Clearing a finished task
        instance starts a fresh budget; whether its rerun replays from the journal
        depends on the Airflow version and how the task was cleared
        (:ref:`durable-clear`). The agent's ``run_id`` (and its
        ``conversation_id``, when there is no message history) stays the first
        attempt's on every retry.
        On Airflow >= 3.3 the journal is kept in the AIP-103 task state store, so
        no extra configuration is needed. On older Airflow versions it is persisted to
        ObjectStorage and requires ``[common.ai] durable_cache_path`` to be set.
        A pydantic-ai ``MCPToolset`` needs a unique ``id``, and so do capabilities
        with ``@durable_operation`` methods. A ``FunctionToolset`` or a function that
        builds a toolset, passed in ``toolsets`` or ``agent_params["toolsets"]``, needs
        none, and neither does a ``FunctionToolset`` held directly by a ``Toolset``
        capability (:ref:`durable-toolset-ids`). A tool with the metadata
        ``{"airflow": False}`` runs again on every attempt instead of replaying
        (:ref:`durable-tool-opt-out`).
        ``durable=True`` cannot be combined with a ``SandboxToolset`` (raises), attached or
        not: a replayed tool result describes a workspace state the replay did
        not reproduce, and the first call that misses the cache runs against
        whatever the sandbox holds now. With a pydantic-ai-harness ``CodeMode``
        capability, a retry runs each ``run_code`` snippet again and replays the
        tool calls it makes; ``CodeMode``'s ``eager`` and ``speculate`` options are
        off while the run is durable. With ``enable_hitl_review``, every review round
        goes into the journal too; see :ref:`hitl-review-durable`.
    :param cache_prompt: When ``True`` (default), asks the provider to cache the
        tool definitions, system prompt and conversation so far, so the next
        request in the run -- and a mapped task's other instances within the
        cache lifetime -- reads them back at a fraction of the input price instead
        of paying for them again. Turns on prompt caching for Anthropic models and
        for Bedrock and OpenRouter models that support it; a no-op for OpenAI and
        Gemini, which cache long prompts on their own. A provider's own cache
        settings in ``agent_params["model_settings"]`` or a spec file take
        precedence: setting any ``anthropic_cache*`` key leaves Anthropic caching
        entirely to you, and a ``CachePoint`` in the prompt or message history
        leaves all of it to you. Set ``False`` where a cache write is rarely read
        back, such as a single long request that is not mapped. See
        :ref:`agent-prompt-caching` for when caching costs more than it saves.
    :param message_history: Prior conversation to seed the run with, for
        multi-turn sessions that span task runs. Accepts a ``list`` of
        pydantic-ai ``ModelMessage`` objects, or their JSON form as ``str`` /
        ``bytes`` -- e.g.
        ``"{{ ti.xcom_pull(task_ids='ask', key='message_history', default='[]') }}"``
        (pass ``default='[]'`` so the first run, with no XCom yet, starts a fresh
        session instead of failing to parse the string ``"None"``). ``None``
        (default) is a single-turn run -- no behavior change. When set (an empty
        ``[]`` / ``""`` starts a fresh session), the full transcript after the run
        -- ``result.all_messages()`` -- is pushed to XCom under the key
        ``message_history`` so the next run can resume. Persisting that transcript
        under a session key (e.g. in object storage) is the DAG's responsibility.
        The transcript is cumulative and grows each turn; for long sessions use an
        object-storage XCom backend or trim old turns. Not supported together with
        ``enable_hitl_review`` (raises) -- the post-review transcript is not yet
        recoverable.
    :param emit_message_history: Push the post-run transcript to XCom under the key
        ``message_history``. Defaults to ``True`` when ``message_history`` is set and ``False``
        otherwise. Set ``True`` to record a single-turn run without seeding it, or ``False``
        to keep a seeded run from pushing it. Not supported together with
        ``enable_hitl_review`` (raises).

    **HITL Review parameters** (requires the ``hitl_review`` plugin):

    :param enable_hitl_review: When ``True``, the operator enters an
        iterative review loop after the first generation.  A human reviewer
        can approve, reject, or request changes via the plugin's REST API
        at ``/hitl-review`` or through the **HITL Review** extra link
        on the task instance.  On Airflow 3.3+ the task waits for each
        answer in the ``awaiting_input`` state, holding no worker slot, and
        each round runs in a new worker process.  Default ``False``. Cannot be combined with a
        ``SandboxToolset`` that provisions its own sandbox (raises):
        regeneration after feedback is a second run, which would start from an
        empty sandbox while its history describes the first run's files. A
        ``SandboxToolset`` attached to a sandbox another task owns
        (``attach_to``) is fine, since both runs find the same files, as long
        as the reviewer answers inside that sandbox's lifetime: the wait spends
        the provisioning backend's ``sandbox_timeout``.
    :param max_hitl_iterations: Maximum outputs shown to the reviewer (1 =
        initial output). When the reviewer requests changes at
        iteration >= this limit, the task fails with ``HITLMaxIterationsError``
        without calling the LLM. E.g. 5 allows changes at iterations 1–4.
        Default ``5``.
    :param hitl_timeout: Maximum wall-clock time to wait for
        all review rounds combined.  ``None`` means no wall-clock timeout;
        the review still ends on a terminal action (approve or reject),
        ``max_hitl_iterations``, or, on Airflow before 3.3, when polling the
        human action XCom fails too many times in a row.
    :param hitl_poll_interval: Seconds between XCom polls
        while waiting for a human response on Airflow before 3.3; not used
        on Airflow 3.3+, where the task parks instead.  Default ``10``.

    **Per-tool approval** (Airflow 3.3+):

    Mark the tools a human must approve with pydantic-ai's own API --
    ``toolset.approval_required(...)``, or ``requires_approval=True`` on a function
    tool -- and the task pauses before running them. The pending calls, with their
    arguments, appear on the **Required Actions** page; the task waits in the
    ``awaiting_input`` state without holding a worker slot. On **Approve** the calls
    run and the agent carries on. On **Reject** the agent is told the call was denied
    (with the reviewer's reason, when given) and carries on without it. On Airflow
    3.4+, each later step that calls a marked tool pauses again; on Airflow 3.3 a task
    instance asks at most once per Dag run, across retries and clears, and a second
    request fails the task. ``usage_limits`` applies to every side of every pause.
    With ``durable``, the answer is recorded in the journal and the resumed run replays
    up to it, so a retry neither asks again nor runs an approved call twice.
    Not available together with ``enable_hitl_review``, a ``CodeMode``
    capability, or a ``SandboxToolset``
    that provisions its own sandbox; there, a tool that requires approval fails
    the task as before, except one called from inside ``CodeMode``'s ``run_code``,
    which does not run and is reported back to the model. A ``SandboxToolset`` attached to a
    sandbox another task owns is fine: the sandbox outlives the pause.

    :param tool_approval_timeout: How long the pause waits for a decision.
        ``None`` (default) waits indefinitely. Must be positive.
    :param on_tool_approval_timeout: What a timed-out pause does: ``"fail"``
        (default) fails the task, ``"deny"`` rejects the pending calls so the agent
        carries on without them, and needs a ``tool_approval_timeout``. There is no
        approve-on-timeout.
    :param tool_approval_assigned_users: Users allowed to decide. ``None`` (default)
        leaves it to anyone who can act on the task's Required Actions.

    :param serialize_output: If ``True``, an output that is itself a Pydantic ``BaseModel`` is
        dumped with ``model_dump()`` before being pushed to XCom, so ``date`` and ``Decimal``
        fields keep their types. Default ``False``: a model whose class the worker registers from
        ``output_type`` flows through XCom unchanged. Some outputs are pushed as dicts either way;
        see :ref:`When a model arrives as a dict <structured-output-dict-shape>`.
    """

    deserialization_allowed_class_fields: ClassVar[tuple[str, ...]] = ("output_type",)

    # This operator supports durable execution directly, without ResumableJobMixin --
    # it caches step results via task_state_store for replay on retry.
    __supports_durable_execution: ClassVar[bool] = True

    template_fields: Sequence[str] = (
        "prompt",
        "llm_conn_id",
        "model_id",
        "fallback_conn_ids",
        "system_prompt",
        "agent_params",
        "message_history",
        "usage_limits",
    )

    # HITL review needs Airflow 3.1. Airflow 2 would also log an error for the unregistered
    # link class every time the webserver loads a Dag with this operator.
    operator_extra_links = (HITLReviewLink(),) if AIRFLOW_V_3_1_PLUS else ()

    def __init__(
        self,
        *,
        prompt: str,
        llm_conn_id: str,
        model_id: str | None = None,
        fallback_conn_ids: list[str] | None = None,
        system_prompt: str = "",
        output_type: OutputSpec[Any] | UnionType = str,
        toolsets: list[AbstractToolset] | None = None,
        capabilities: list[AgentCapability[Any]] | None = None,
        enable_tool_logging: bool = True,
        agent_params: dict[str, Any] | None = None,
        usage_limits: UsageLimits | dict[str, Any] | None = None,
        durable: bool | None = None,
        cache_prompt: bool = True,
        message_history: list[ModelMessage] | str | bytes | None = None,
        emit_message_history: bool | None = None,
        # Agent feedback parameters
        enable_hitl_review: bool = False,
        max_hitl_iterations: int = 5,
        hitl_timeout: timedelta | None = None,
        hitl_poll_interval: float = 10.0,
        serialize_output: bool = False,
        tool_approval_timeout: timedelta | None = None,
        on_tool_approval_timeout: Literal["fail", "deny"] = "fail",
        tool_approval_assigned_users: HITLUser | Iterable[HITLUser] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.fallback_conn_ids = fallback_conn_ids
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.serialize_output = serialize_output
        # See LLMOperator: instance flows when Airflow registers ``output_type``
        # via its worker-side DAG walk; otherwise (or on opt-in) dump to a dict.
        self._serialize_model_output = serialize_output or not _CORE_WALKER
        self.toolsets = toolsets
        self.enable_tool_logging = enable_tool_logging
        self.agent_params = agent_params or {}
        self.capabilities = capabilities
        # No validation here -- see coerce_usage_limits() docstring for why.
        self.usage_limits = usage_limits
        self.message_history = message_history
        self.emit_message_history = (
            message_history is not None if emit_message_history is None else emit_message_history
        )

        self.durable = durable
        self.cache_prompt = cache_prompt
        # Whether this run is durable. ``durable=None`` leaves it to ``_decide_durable`` when the
        # task runs, since the task state store and the task log exist only then.
        self._is_durable_run = durable is True

        # The task instance's durable journal, set per run in ``execute`` when the run is durable.
        self._durable_journal: DurableJournal | None = None

        # Populated in ``execute``; also read (and, if unset, lazily initialized) by
        # ``regenerate_with_feedback`` outside ``execute``, which is why they need a
        # declared default here rather than only being set inline in ``execute``.
        self._usage_budget: TaskStateStoreUsageBudget | None = None
        self._run_usage: RunUsage | None = None
        self._run_usage_base: RunUsage = RunUsage()
        # Which tool approval pause of this try the run is resuming from; 0 before the first.
        self._approval_pause_number = 0
        # Under durable: every answer reviewers gave this run, read from the task state store, and
        # whether the store holds any; the one this resume brought, until a call takes it; the
        # approval steps the run has reached; whether an approval step is asking the other
        # handlers; and the run's usage when it paused, with this attempt's share of it.
        self._tool_approval_answers: list[_ToolApprovalAnswer] = []
        self._tool_approval_answers_stored = False
        self._new_tool_approval_answer: _ToolApprovalAnswer | None = None
        self._approval_steps = 0
        self._asking_other_handlers = False
        self._usage_at_pause: tuple[RunUsage, RunUsage] | None = None

        # The answer the HITL reviewer is looking at, as the agent returned it.
        self._output_under_review: Any = None
        # The agent _record_review_feedback built for the next regeneration, until it takes it.
        self._regeneration_agent: Agent[object, Any] | None = None

        # Checked ahead of the combination rules below. When Airflow is older than 3.1, its version
        # is the real blocker, and reporting a combination error first would send the
        # user to drop an argument that was never the problem -- they would hit this anyway.
        if enable_hitl_review and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "Human in the loop functionality needs Airflow 3.1+."
            )

        if durable and (problem := self._durable_problem()) is not None:
            raise ValueError(problem)

        if message_history is not None and enable_hitl_review:
            # The post-review transcript is not recoverable today (run_hitl_review
            # returns only the final string), so emitting the pre-review transcript
            # would silently drop the human-approved turns. Block until HITL can
            # surface the final message history.
            raise ValueError("message_history and enable_hitl_review=True cannot be used together.")

        if emit_message_history and enable_hitl_review:
            raise ValueError("emit_message_history and enable_hitl_review=True cannot be used together.")

        if enable_hitl_review:
            self._reject_sandbox_without_review_continuity()

        self.enable_hitl_review = enable_hitl_review
        self.max_hitl_iterations = max_hitl_iterations
        self.hitl_timeout = hitl_timeout
        self.hitl_poll_interval = hitl_poll_interval

        if on_tool_approval_timeout not in ("fail", "deny"):
            raise ValueError(
                f"on_tool_approval_timeout must be 'fail' or 'deny', got {on_tool_approval_timeout!r}."
            )
        if tool_approval_timeout is not None and tool_approval_timeout <= timedelta(0):
            raise ValueError(f"tool_approval_timeout must be positive, got {tool_approval_timeout!r}.")
        if on_tool_approval_timeout == "deny" and tool_approval_timeout is None:
            raise ValueError("on_tool_approval_timeout='deny' needs a tool_approval_timeout to fire.")
        self.tool_approval_timeout = tool_approval_timeout
        self.on_tool_approval_timeout = on_tool_approval_timeout
        self.tool_approval_assigned_users = normalize_assigned_users(
            tool_approval_assigned_users, param="tool_approval_assigned_users"
        )

    def _durable_problem(self) -> str | None:
        """
        Say why this task's agent cannot run durably, or return ``None``.

        ``durable=True`` raises it when the Dag loads; the default turns durable execution off
        and logs it when the task runs. A ``SandboxToolset`` is refused, attached or not: a
        replayed tool result is not executed again, so a replayed ``write_file`` leaves no file
        behind and the first call that is not replayed runs against whatever the sandbox holds.
        The toolset is looked for inside wrappers, combinations and every capability's toolset;
        one resolved per run from a callable cannot be inspected here.
        """
        if _contains_capability(self._declared_capabilities, AirflowDurability):
            return "durable=True attaches AirflowDurability itself; pass durable=True or the capability, not both."
        if self._durable_sandbox_toolsets():
            return (
                "durable=True cannot be used with a SandboxToolset: cached tool results would be "
                "replayed without touching the sandbox, so the workspace would not match the "
                "transcript. Drop durable=True, or move the sandbox work into its own task."
            )
        return self._durable_toolset_id_problem() or _capability_toolset_without_id(
            self._declared_capabilities
        )

    def _durable_toolset_id_problem(self) -> str | None:
        """
        Say why durable replay would have no name for some toolset's steps, or return ``None``.

        pydantic-ai names the durable steps of a ``FunctionToolset``, ``MCPToolset`` or
        ``DynamicToolset`` after the toolset's ``id``, and refuses to run one without it. The
        toolsets ``_build_agent`` hands the agent are given ids here as it gives them
        (``_give_durable_ids``), and a leaf still without one is a problem: an ``MCPToolset``, whose
        server URL or command can carry credentials, or a function without a qualified name.
        ``durable=True`` raises it when the Dag loads; the default turns durable execution off.
        """
        given: _GivenIds = {}
        leaves = [
            leaf
            for toolset in self._running_toolsets()
            for leaf in iter_toolsets(_give_durable_ids(toolset, given))
            if isinstance(leaf, DURABLE_UNIT_TOOLSETS)
        ]
        # One instance listed twice is one toolset to pydantic-ai; two instances sharing an id
        # would have their recorded calls filed under one name. Each listing of a function
        # becomes a DynamicToolset of its own, so two lambdas in one module share an id.
        first_by_id: dict[str, AbstractToolset[Any]] = {}
        duplicates: list[str] = []
        for leaf in leaves:
            if (toolset_id := _get_chosen_toolset_id(leaf)) is None and isinstance(leaf, DynamicToolset):
                return (
                    "durable=True names the steps of a toolset built per run after the function that "
                    f"builds it, but {leaf.toolset_func!r} has no __qualname__. Pass it as "
                    "DynamicToolset(function, id=...)."
                )
            if toolset_id is None:
                return (
                    "durable=True needs a unique id on every MCPToolset, such as "
                    "MCPToolset(..., id='weather'): durable replay records each tool call under it, and "
                    "none is derived from the server's URL or command, which can carry credentials. Set "
                    "it on the toolset itself; the id of a Toolset capability around a toolset does not "
                    "reach it."
                )
            if first_by_id.setdefault(toolset_id, leaf) is not leaf and toolset_id not in duplicates:
                duplicates.append(toolset_id)
        if duplicates:
            return (
                f"durable=True needs a unique id on each toolset, but more than one toolset has the id "
                f"{' or '.join(map(repr, duplicates))}: durable replay records each tool call "
                "under its toolset's id, so a retry could not tell their calls apart."
            )
        return None

    def _reject_sandbox_without_review_continuity(self) -> None:
        """
        Refuse a ``SandboxToolset`` that provisions its own sandbox under HITL review.

        A sandbox is provisioned on the first tool call and destroyed when the run ends.
        Regeneration after reviewer feedback starts a second agent run, which would get an
        empty sandbox while its message history still describes the files the first run
        wrote. A toolset attached to a sandbox another task owns (``attach_to``) keeps its
        files across runs, so the regenerated run finds what the first run wrote.
        """
        if any(sandbox.attach_to is None for sandbox in self._sandbox_toolsets()):
            raise ValueError(
                "enable_hitl_review=True cannot be used with a SandboxToolset that provisions its own "
                "sandbox: a regenerated run would start from an empty sandbox while its history "
                "describes files from the first run. Attach the toolset to a sandbox another task "
                "provisioned (attach_to=...), drop enable_hitl_review=True, or move the sandbox work "
                "into its own task."
            )

    def _sandbox_toolsets(self) -> list[SandboxToolset]:
        """Every ``SandboxToolset`` the agent was given, looked for inside wrappers and combinations."""
        return _sandboxes_in(self._declared_toolsets())

    def _durable_sandbox_toolsets(self) -> list[SandboxToolset]:
        """
        Every ``SandboxToolset`` the agent was given, including those any capability contributes.

        Unlike ``_sandbox_toolsets``, this looks at the toolset of every capability, nested ones
        included (``Capability(toolsets=...)``, a custom ``get_toolset``). A toolset built per
        run, by a function or a ``DynamicToolset``, cannot be looked inside.
        """
        toolsets = self._declared_toolsets()
        for capability, _ in _iter_capabilities(self._declared_capabilities):
            try:
                toolset = capability.get_toolset()
            except Exception:
                # Some capabilities read credentials here that only workers have. The worker parses
                # the Dag again with them, so the check runs there, before the task does.
                continue
            if isinstance(toolset, AbstractToolset):
                toolsets.append(toolset)
        return _sandboxes_in(toolsets)

    def _do_render_template_fields(
        self,
        parent: Any,
        template_fields: Iterable[str],
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
    ) -> None:
        super()._do_render_template_fields(parent, template_fields, context, jinja_env, seen_oids)
        # Hooked here rather than in render_template_fields because a mapped task never calls
        # that one -- MappedOperator renders through _do_render_template_fields on the unmapped task.
        if parent is self:
            self._render_toolsets(context, jinja_env, seen_oids)

    def _render_toolsets(self, context: Context, jinja_env: jinja2.Environment, seen_oids: set[int]) -> None:
        """
        Render the fields of toolsets that declare ``agent_template_fields``.

        ``toolsets`` is not itself a template field: serializing it would put each
        toolset's repr -- which for pydantic-ai's dataclass toolsets embeds function
        addresses -- into the Dag hash and the rendered-fields view. Instead, each leaf
        toolset that opts in (``SQLToolset``, ``MCPToolset``, ``HookToolset``, and
        ``SandboxToolset`` for the handle it attaches to) is rendered here, found with
        pydantic-ai's ``visit_and_replace`` inside
        ``.prefixed()`` / ``.filtered()`` wrappers, ``Toolset`` capabilities, and a
        ``toolsets`` list passed through ``agent_params``. A ``Toolset`` capability
        backed by a callable factory is resolved per run and is not rendered.

        A rendered *copy* replaces the original, which is left untouched: mapped task
        instances and ``dag.test()`` share one toolset object across runs in the same
        process, and rendering it in place would hand one map index's connection to
        the next. That is also why the opt-in is ``agent_template_fields`` and not
        ``template_fields``: Airflow's templater renders any object carrying
        ``template_fields`` in place wherever it sits inside another template field,
        such as ``agent_params``.
        """

        def render(toolset: AbstractToolset[Any]) -> AbstractToolset[Any]:
            fields = getattr(toolset, "agent_template_fields", None)
            if not fields:
                return toolset
            rendered = copy.copy(toolset)
            self._do_render_template_fields(rendered, fields, context, jinja_env, seen_oids)
            # The rendered connection is recorded nowhere else, so this line is the audit trail
            # of which connection this task instance's agent was given.
            if rendered.id != toolset.id:
                self.log.info("Rendered toolset %s", rendered.id)
            return rendered

        def render_all(toolsets: list[Any]) -> list[Any]:
            # Leave anything without a templated leaf alone: rebuilding a wrapper via
            # visit_and_replace breaks wrapper subclasses with their own __init__.
            return [
                toolset.visit_and_replace(render) if _declares_agent_template_fields(toolset) else toolset
                for toolset in toolsets
            ]

        def render_capabilities(capabilities: list[Any]) -> list[Any]:
            return [
                _replace_capability_toolset(capability, lambda toolset: toolset.visit_and_replace(render))
                if _declares_agent_template_fields(_resolve_capability_toolset(capability))
                else capability
                for capability in capabilities
            ]

        if self.toolsets:
            self.toolsets = render_all(self.toolsets)
        if self.capabilities:
            self.capabilities = render_capabilities(self.capabilities)
        agent_params = dict(self.agent_params)
        if agent_params.get("toolsets"):
            agent_params["toolsets"] = render_all(agent_params["toolsets"])
        if agent_params.get("capabilities"):
            agent_params["capabilities"] = render_capabilities(agent_params["capabilities"])
        self.agent_params = agent_params

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """Return PydanticAIHook for the configured LLM connection."""
        hook_params = {
            "model_id": self.model_id,
            "fallback_conn_ids": self.fallback_conn_ids,
        }
        return PydanticAIHook.get_hook(self.llm_conn_id, hook_params=hook_params)

    def _build_agent(self, *, approval_context: Context | None = None) -> Agent[object, Any]:
        """
        Build and return a pydantic-ai Agent from the operator's config.

        :param approval_context: The task's context, where a durable run can pause for tool approval.
        """
        extra_kwargs = dict(self.agent_params)
        passed_through = extra_kwargs.pop("capabilities", None)
        if passed_through is not None and self.capabilities is not None:
            # pydantic-ai wraps capability hooks in list order, so merging the two lists
            # would pick an order the Dag author never wrote down.
            raise ValueError("Pass capabilities either as capabilities=... or in agent_params, not both.")
        # create_agent adds a MaskingCapability that masks every tool call. The toolsets are
        # masked here as well because toolset wrappers a capability adds (get_wrapper_toolset,
        # as ToolLoggingCapability does) run below the capability hooks and would otherwise see
        # the unmasked result. The durable journal masks what it records on its own.
        given_ids: _GivenIds = {}

        def prepare(toolset: AbstractToolset[Any] | ToolsetFunc[Any]) -> AbstractToolset[Any]:
            return ensure_masked(_give_durable_ids(toolset, given_ids) if self._is_durable_run else toolset)

        if self.toolsets:
            toolsets: list[AbstractToolset] = [prepare(ts) for ts in self.toolsets]
            extra_kwargs["toolsets"] = toolsets
        elif extra_kwargs.get("toolsets"):
            extra_kwargs["toolsets"] = [prepare(ts) for ts in extra_kwargs["toolsets"]]
        capabilities = [
            _replace_capability_toolset(capability, prepare)
            for capability in self.capabilities or passed_through or []
        ]
        if self._is_durable_run:
            # Names the agent's steps in the journal; the task id is stable across retries.
            capabilities.append(AirflowDurability(name=extra_kwargs.get("name") or self.task_id))
            if approval_context is not None and self._supports_tool_approval():
                # First, so the other handlers run inside its step (see _DurableToolApproval).
                capabilities.insert(
                    0,
                    _DurableToolApproval(
                        claim_next_step=self._claim_next_approval_step,
                        settle=lambda ctx, requests, step: self._settle_tool_approvals(
                            approval_context, ctx, requests, step
                        ),
                        id=_TOOL_APPROVAL_CAPABILITY_ID,
                    ),
                )
        if self.cache_prompt:
            capabilities.append(PromptCaching())
        if self.enable_tool_logging and not _contains_capability(capabilities, ToolLoggingCapability):
            # ToolLoggingCapability's innermost ordering keeps logging inside capability wrappers,
            # including CodeModeToolset where code mode expects the wrapped tools.
            capabilities.append(ToolLoggingCapability(logger=self.log))
        if capabilities:
            extra_kwargs["capabilities"] = capabilities
        return self.llm_hook.create_agent(
            output_type=self._agent_output_type(),
            instructions=self.system_prompt,
            **extra_kwargs,
        )

    def _build_agent_as_decided(self, *, approval_context: Context | None = None) -> Agent[object, Any]:
        """
        Build the agent, durable if the run was decided durable.

        pydantic-ai checks a durable agent when it builds it and refuses some shapes the checks in
        ``_durable_problem`` do not cover, such as a capability with durable operations and no
        ``id``. Under the default, such an agent is built without durable execution and the task
        log gives pydantic-ai's reason; ``durable=True`` keeps the error.
        """
        try:
            return self._build_agent(approval_context=approval_context)
        except UserError as error:
            if not self._is_durable_run or self.durable is not None:
                raise
            self._is_durable_run = False
            self._durable_journal = None
            # Built first: an error unrelated to durable execution raises again here, unlogged.
            agent = self._build_agent(approval_context=approval_context)
            self.log.info("Durable execution is off for this task: %s", error)
            return agent

    def _supports_tool_approval(self) -> bool:
        """
        Whether a tool that requires approval pauses the task instead of failing it.

        Each excluded feature assumes the run finishes in one go: HITL review and code
        mode wrap the run, and a sandbox the toolset provisions itself is destroyed when
        the run ends, so its files would be gone on resume. A sandbox another task owns
        (``attach_to``) outlives the pause, and the resumed run attaches to it again.
        """
        if (
            not AIRFLOW_V_3_3_PLUS
            or self.enable_hitl_review
            or _contains_code_mode(self._declared_capabilities)
        ):
            return False
        return all(sandbox.attach_to is not None for sandbox in self._sandbox_toolsets())

    def _agent_output_type(self) -> Any:
        """
        Return ``output_type``, plus ``DeferredToolRequests`` when tool approval is supported or ``durable``.

        pydantic-ai drops ``DeferredToolRequests`` from the output schema the model sees;
        it only lets the run end on a tool call awaiting approval. Under ``durable`` where
        approval is not available, ending it there lets ``_pause_for_tool_approval`` fail the
        task with ``UnsupportedToolDeferralError`` instead of pydantic-ai's ``UserError``.
        ``self.output_type`` is left alone because it is part of the serialized Dag.
        """
        if not (self._supports_tool_approval() or self._is_durable_run):
            return self.output_type
        declared = self.output_type if isinstance(self.output_type, (list, tuple)) else [self.output_type]
        return [*declared, DeferredToolRequests]

    def _declared_toolsets(self) -> list[AbstractToolset[Any]]:
        """Toolsets passed via ``toolsets=``, ``agent_params["toolsets"]`` and concrete ``Toolset`` capabilities."""
        candidates = [
            toolset
            for toolset in (*(self.toolsets or []), *(self.agent_params.get("toolsets") or []))
            if isinstance(toolset, AbstractToolset)
        ]
        for capability in self._declared_capabilities:
            if (toolset := _resolve_capability_toolset(capability)) is not None:
                candidates.append(toolset)
        return candidates

    def _running_toolsets(self) -> list[AbstractToolset[Any] | ToolsetFunc[Any]]:
        """
        Return the toolsets ``_build_agent`` hands the agent, including functions that build one per run.

        Unlike ``_declared_toolsets``, ``agent_params["toolsets"]`` counts only when
        ``toolsets=`` is empty, because ``_build_agent`` ignores it otherwise.
        """
        candidates = list(self.toolsets or self.agent_params.get("toolsets") or [])
        for capability in self._declared_capabilities:
            if (toolset := _resolve_capability_toolset(capability)) is not None:
                candidates.append(toolset)
        return candidates

    @property
    def _declared_capabilities(self) -> list[AgentCapability[Any]]:
        """Capabilities passed via ``capabilities=`` and ``agent_params["capabilities"]``."""
        return [*(self.capabilities or ()), *(self.agent_params.get("capabilities") or ())]

    def _toolset_ids(self) -> list[str]:
        """Ids of every leaf toolset, which for SQL and MCP toolsets name the connection."""
        # Declared order, not sorted: two toolsets that swapped connections must not compare equal.
        return [
            leaf.id
            for toolset in self._declared_toolsets()
            for leaf in iter_toolsets(toolset)
            if leaf.id is not None
        ]

    def _build_durable_storage(self, context: Context) -> DurableStorageProtocol:
        """
        Return where the durable journal of this task instance is stored.

        The task state store on Airflow >= 3.3; ObjectStorage under ``[common.ai]
        durable_cache_path`` on older versions.
        """
        return build_task_storage(context)

    def _open_journal(self, context: Context, *, first_run: int = 0) -> DurableJournal:
        """
        Open the task instance's durable journal.

        A step result the journal cannot hold fails the task under ``durable=True``. On by
        default, the step is logged and runs again on retry instead, so turning on the
        default never fails a task that ran without it.
        """
        return DurableJournal(
            self._build_durable_storage(context),
            fail_on_unrecordable=self.durable is True,
            first_run=first_run,
        )

    def _report_failed_run(self, context: Context, run_usage: RunUsage, *, run_id: str | None = None) -> None:
        """
        Log and XCom-push the usage a failed attempt incurred before it raised.

        ``run_usage`` is the (possibly cross-attempt) cumulative total; the delta this
        attempt itself contributed is computed here, against ``self._run_usage_base`` --
        the snapshot ``execute()`` takes right after loading the budget. So an attempt
        that fails before the agent issues any call (for example, ``agent.override(...)``'s
        ``__enter__``, or a task-timeout signal landing before the agent runs) reports 0.

        Best-effort like ``_emit_run_metadata``: each push is wrapped separately so a
        failure here (e.g. a downed XCom backend) never masks the run's real exception
        -- the caller's bare ``raise`` after this call must still surface it. If the
        delta computation itself fails, the log and usage XCom are skipped entirely
        rather than falling back to the cumulative total, which would double-count
        this attempt's usage against earlier ones; ``run_id`` is still pushed.
        """
        attempt_usage: RunUsage | None
        try:
            attempt_usage = subtract_run_usage(run_usage, self._run_usage_base)
        except Exception:
            self.log.warning("Failed to compute this attempt's usage for the failed run", exc_info=True)
            attempt_usage = None
        if attempt_usage is not None:
            try:
                log_run_usage(self.log, attempt_usage, outcome="failed")
            except Exception:
                self.log.warning("Failed to log partial usage for the failed run", exc_info=True)
        if not self.do_xcom_push:
            return
        ti = context["task_instance"]
        try:
            # The id the agent ran with, which is the durable run's on a durable retry.
            ti.xcom_push(key="run_id", value=run_id or make_task_instance_run_key(ti))
        except Exception:
            self.log.warning("Failed to push run_id XCom for the failed run", exc_info=True)
        if attempt_usage is not None:
            try:
                ti.xcom_push(key="usage", value=format_usage_for_xcom(attempt_usage))
            except Exception:
                self.log.warning("Failed to push usage XCom for the failed run", exc_info=True)

    def _run_agent_tracked(
        self,
        agent: Agent[Any, Any],
        prompt: Any,
        *,
        run_usage: RunUsage,
        **run_kwargs: Any,
    ) -> tuple[Any, RunUsage]:
        """
        Run the agent, persisting cumulative usage after every attempt (success or failure).

        ``run_usage`` -- always ``self._run_usage`` -- is taken as an explicit,
        non-Optional parameter (rather than read off ``self``) purely so mypy can
        narrow it without an ``assert``: ``self._run_usage`` is declared ``RunUsage |
        None`` because it is set lazily, but every caller of this method has already
        ensured it is a real ``RunUsage`` by the time it gets here.

        Under durable execution, ``AirflowDurability`` keeps replayed steps out of
        ``run_usage`` and settles its credits before the run returns, so neither reaches
        the persisted total.
        """
        return self._run_tracked(agent, prompt, run_usage=run_usage, **run_kwargs)

    def _run_and_report_on_failure(
        self,
        context: Context,
        agent: Agent[Any, Any],
        run_usage: RunUsage,
        run_kwargs: dict[str, Any],
    ) -> Any:
        """Run ``self.prompt`` via ``_run_agent_tracked``, reporting usage-at-failure on any raise."""
        try:
            with self._journal_scope():
                result, _ = self._run_agent_tracked(agent, self.prompt, run_usage=run_usage, **run_kwargs)
            return result
        except BaseException as error:
            if not _is_awaiting_input(error):
                self._report_failed_run(context, run_usage, run_id=run_kwargs["run_id"])
            elif error.method_name == _DURABLE_APPROVAL_RESUME:
                # The durable run paused for approval. Its usage so far, known only now that the run
                # has settled it, goes with the pause, so the resume reports both sides of it.
                error.kwargs = {
                    **(error.kwargs or {}),
                    "usage": _RUN_USAGE_ADAPTER.dump_python(run_usage, mode="json"),
                    "attempt_usage": _RUN_USAGE_ADAPTER.dump_python(
                        subtract_run_usage(run_usage, self._run_usage_base), mode="json"
                    ),
                }
            raise

    def _decide_durable(self, context: Context) -> None:
        """
        Decide whether this run is durable, and log why when the default turned it off.

        ``durable=True`` and ``durable=False`` decide it themselves. The default turns it on
        where a retry can replay the journal and every step can be recorded, and off, with the
        reason in the task log, everywhere else.
        """
        if self.durable is not None:
            self._is_durable_run = self.durable
            return
        if _contains_capability(self._declared_capabilities, AirflowDurability):
            # Inside a running task the capability opens a journal of its own (current_journal).
            self._is_durable_run = False
            self.log.info(
                "Durable execution is left to the AirflowDurability capability the agent was given, "
                "which deletes each agent run's steps as soon as that run succeeds."
            )
            return
        reason = self._why_not_durable_by_default(context)
        self._is_durable_run = reason is None
        if reason is not None:
            self.log.info("Durable execution is off for this task: %s", reason)

    def _why_not_durable_by_default(self, context: Context) -> str | None:
        """Say why durable execution cannot be on by default for this task, or return ``None``."""
        # From 3.4 a retry policy can run a task with no retries again (RetryAction.RESCHEDULE);
        # before that it cannot, and before 3.3 BaseOperator has no retry_policy.
        if not self.retries and (not AIRFLOW_V_3_4_PLUS or self.retry_policy is None):
            return "the task has no retries, so no attempt would replay this one."
        if not AIRFLOW_V_3_3_PLUS:
            return (
                "it is on by default only from Airflow 3.3. Before that, pass durable=True and set "
                "[common.ai] durable_cache_path."
            )
        if "task_state_store" not in context:
            return "the task's context has no task state store to keep the journal in."
        return self._durable_problem()

    @property
    def _durable_mode(self) -> str:
        """How this run's durable execution was turned on, for the summary line."""
        return "on by default" if self.durable is None else "durable=True"

    def _span_attributes(self, ti: Any) -> dict[str, Any]:
        """Attributes for every span the agent emits: the task instance, and whether the run is durable."""
        return {**build_run_identity_attributes(ti), "airflow.common_ai.durable": self._is_durable_run}

    def _journal_scope(self) -> AbstractContextManager[DurableJournal | None]:
        """Run the code in the ``with`` block under the task's journal, when the task is durable."""
        return journal_scope(self._durable_journal) if self._durable_journal is not None else nullcontext()

    def execute(self, context: Context) -> Any:
        # Coerced first so a bad rendered value fails before the expensive setup below.
        usage_limits = coerce_usage_limits(self.usage_limits)
        self._decide_durable(context)

        # A try that paused without durable execution and then ended some other way (marked failed
        # while waiting, a failed request) leaves its transcript behind; a fresh run never reads it.
        # A durable run too: a try from before the upgrade that turned the default on may have left
        # one. ``.get``: a context built by hand in a unit test has no task state store.
        if self._supports_tool_approval() and (store := context.get("task_state_store")) is not None:
            self._delete_approval_state(store, _TOOL_APPROVAL_TRANSCRIPT_KEY)

        ti = context["task_instance"]
        self._durable_journal = None
        # Reads the state store before the expensive setup below (_build_agent, durable
        # storage). None on < 3.3 or usage_limits=None -- see _build_usage_budget.
        self._usage_budget = self._build_usage_budget(context, usage_limits, ti=ti)
        if self._usage_at_pause is None:
            self._run_usage = self._usage_budget.load() if self._usage_budget else RunUsage()
            self._run_usage_base = copy_run_usage(self._run_usage)
        else:
            # A durable run resumed after tool approval. The budget, when there is one, saved the
            # same total when the run paused; this attempt's usage started before the pause.
            usage_at_pause, attempt_usage_at_pause = self._usage_at_pause
            self._run_usage = usage_at_pause
            self._run_usage_base = subtract_run_usage(usage_at_pause, attempt_usage_at_pause)
        # A local, non-Optional alias of `self._run_usage` for the rest of this method --
        # see `_run_agent_tracked`'s docstring for why callers pass this explicitly instead
        # of letting callees read `self._run_usage` (which mypy can't narrow past None).
        run_usage: RunUsage = self._run_usage

        agent = self._build_agent_as_decided(approval_context=context)
        if self._is_durable_run:
            self._durable_journal = self._open_journal(context)
            self._approval_steps = 0
            if self._supports_tool_approval() and (store := context.get("task_state_store")) is not None:
                self._load_tool_approval_answers(store)

        self._run_identity_attrs = self._span_attributes(ti)
        stamp_identity_on_agent_spans(agent, self._run_identity_attrs)

        # A per-attempt key (the task-instance id on Airflow 3, which is regenerated on
        # each retry; dag/run/task/map/try on Airflow 2) is a unique, reverse-resolvable
        # join key. It lands on result.run_id, the run's messages, and the
        # ``gen_ai.agent.call.id`` span attribute.
        run_kwargs: dict[str, Any] = {"usage_limits": usage_limits, "run_id": make_task_instance_run_key(ti)}
        history = self._resolve_message_history()
        if history is not None:
            run_kwargs["message_history"] = history
        if self._durable_journal is not None:
            # Under durable execution the run keeps the first attempt's id on every retry, because
            # replayed steps carry it: capabilities that key their own state on the run id
            # (pydantic-ai-harness SpendLimits, StepPersistence) would otherwise look up the
            # retry's id in the previous attempt's records. Attempts still differ in their
            # spans' task instance id and try number.
            run_kwargs["run_id"] = self._durable_journal.get_run_id(default=run_kwargs["run_id"])
            if not history:
                # Without a history to inherit one from, pydantic-ai starts a new conversation
                # on every attempt; keep one for the durable run, for the same reason.
                run_kwargs["conversation_id"] = run_kwargs["run_id"]

        # A killed run raises RunCancelled (see run_agent_sync), which propagates to fail the
        # task. The journal cleanup in _complete_run is skipped on the raise, keeping it for the retry.
        try:
            result = self._run_and_report_on_failure(context, agent, run_usage, run_kwargs)
        finally:
            # Also on a raise: the failed attempt is the one Airflow retries.
            if self._durable_journal is not None:
                self._durable_journal.log_summary(self.log, mode=self._durable_mode)
        if (answer := self._new_tool_approval_answer) is not None and answer.calls:
            # The replay went another way, such as after a change to the Dag, and never reached them.
            self.log.warning(
                "The resumed run finished without reaching the tool calls the answer was for (%s), "
                "so they did not run.",
                ", ".join(name for name, _ in answer.calls),
            )
        # From where this attempt started, which for a resumed run is before the pause.
        return self._complete_run(
            context, result, attempt_usage=subtract_run_usage(run_usage, self._run_usage_base)
        )

    def _complete_run(self, context: Context, result: Any, *, attempt_usage: RunUsage) -> Any:
        """Finish a run, or pause it when the agent is waiting on a tool call to be approved."""
        log_run_summary(self.log, result, usage=attempt_usage)
        if isinstance(result.output, DeferredToolRequests):
            try:
                self._pause_for_tool_approval(context, result, attempt_usage=attempt_usage)
            except BaseException as e:
                # The run itself ended normally, after _run_and_report_on_failure, so a refusal
                # here is the first point this attempt is known to have failed.
                if (run_usage := self._run_usage) is not None and not _is_awaiting_input(e):
                    self._report_failed_run(context, run_usage, run_id=result.run_id)
                raise
        self._emit_run_metadata(context, result, usage=attempt_usage)
        if self._usage_budget and (run_usage := self._run_usage) is not None:
            self._log_cumulative_usage(run_usage)

        if self.emit_message_history:
            self._emit_message_history(context, result)

        output = result.output

        if self.enable_hitl_review:
            self._output_under_review = output
            with self._durable_review(context):
                # On Airflow 3.3+ this parks the task, and the review ends in resume_hitl_review.
                approved = self.run_hitl_review(
                    context,
                    output,
                    message_history=result.all_messages(),
                )
                return self.complete_hitl_review(context, approved, state=None)

        output = self._convert_output_for_xcom(output)

        # Clean up the durable journal only after the run and every post-run step
        # that can still fail (the run-metadata and message-history XCom pushes
        # above and output serialization) has succeeded. Cleaning up earlier and
        # then raising would leave the Airflow retry with an empty journal,
        # re-executing every already-completed model and tool step.
        if self._durable_journal is not None:
            self._durable_journal.cleanup()
            if self._tool_approval_answers_stored and (store := context.get("task_state_store")) is not None:
                self._delete_approval_state(store, _TOOL_APPROVAL_ANSWERS_KEY)
        self._clear_usage_budget(context)
        return output

    def _convert_output_for_xcom(self, output: Any) -> Any:
        """Return the XCom value, with dicts in place of models that a downstream task could not load."""
        if not self.do_xcom_push:
            return output
        return dump_output_for_xcom(output, self.output_type, serialize_output=self._serialize_model_output)

    def _pause_for_tool_approval(self, context: Context, result: Any, *, attempt_usage: RunUsage) -> NoReturn:
        """
        Park the task until a human approves or rejects the tool calls the agent is waiting on.

        The transcript goes to the task state store, not the continuation kwargs: it holds tool
        results such as query rows, which do not belong on the task instance row. The
        continuation carries its hash, so the resume refuses a transcript that changed while
        the task waited, and the rendered toolset ids, so it refuses to run approved calls
        against a connection id the reviewer did not see.

        Airflow keeps one approval request per task instance, across retries and clears. From
        Airflow 3.4 a repeat request replaces its details and clears the earlier answer, so the
        run can pause again in a later step or a later try. Before 3.4 a repeat request kept the
        first one's subject and body, so the reviewer would approve a new call while reading the
        old one; there a task instance asks at most once.
        """
        requests: DeferredToolRequests = result.output
        _refuse_external_calls(requests)
        if not self._supports_tool_approval():
            # Reached where approval is unsupported (a durable run's approval step is not added
            # then), or with DeferredToolRequests in a user-set output_type.
            _refuse_unsupported_approval(requests)
        store = context["task_state_store"]
        self._refuse_repeat_approval_request(context, requests.approvals)
        transcript = ModelMessagesTypeAdapter.dump_json(result.all_messages()).decode()
        retention = (
            self.tool_approval_timeout + _TRANSCRIPT_RETENTION_MARGIN
            if self.tool_approval_timeout is not None
            else NEVER_EXPIRE
        )
        store.set(_TOOL_APPROVAL_TRANSCRIPT_KEY, transcript, retention=retention)
        self._ask_for_tool_approval(
            context,
            requests.approvals,
            continuation={
                "tool_call_ids": [call.tool_call_id for call in requests.approvals],
                "usage": _RUN_USAGE_ADAPTER.dump_python(result.usage, mode="json"),
                "attempt_usage": _RUN_USAGE_ADAPTER.dump_python(attempt_usage, mode="json"),
                "transcript_sha256": hashlib.sha256(transcript.encode()).hexdigest(),
                "toolset_ids": self._toolset_ids(),
            },
        )

    def _refuse_repeat_approval_request(self, context: Context, calls: Sequence[ToolCallPart]) -> None:
        """Before Airflow 3.4, fail rather than ask a task instance's reviewer a second time."""
        if not _REPEAT_APPROVAL_SUPPORTED and context["task_state_store"].get(_TOOL_APPROVAL_REQUESTED_KEY):
            raise ToolApprovalAlreadyRequestedError(
                f"The agent asked for a second tool approval ({', '.join(call.tool_name for call in calls)}), "
                "but this task instance already asked once in this Dag run. Before Airflow 3.4, a repeat "
                "approval request shows the reviewer the earlier request's details, so the task fails "
                "instead of pausing. Upgrade to Airflow 3.4, have the agent ask for the gated calls in "
                "one step, or give each irreversible action its own task."
            )

    def _ask_for_tool_approval(
        self,
        context: Context,
        calls: Sequence[ToolCallPart],
        *,
        continuation: dict[str, Any],
        resume_with: str = "resume_after_tool_approval",
    ) -> NoReturn:
        """Write the approval request for ``calls`` and park the task until it is answered."""
        pending_names = ", ".join(call.tool_name for call in calls)
        # Tool arguments can carry credentials (an HTTP header, an MCP token); mask them first.
        pending = "\n\n".join(
            f"**{call.tool_name}**\n\n```json\n"
            f"{json.dumps(redact(call.args_as_dict()), indent=2, default=str)}\n```"
            for call in calls
        )
        pause_number = self._approval_pause_number + 1
        upsert_hitl_detail(
            ti_id=context["task_instance"].id,
            options=[LLMApprovalMixin.APPROVE, LLMApprovalMixin.REJECT],
            # Numbered, so each pause of a run looks different on the Required Actions page.
            subject=f"Approve {pending_names} for task `{self.task_id}` (request {pause_number})",
            body=f"The agent wants to run:\n\n{pending}",
            defaults=[LLMApprovalMixin.REJECT] if self.on_tool_approval_timeout == "deny" else None,
            multiple=False,
            params={
                "reason": {
                    # "null" in the type is what makes the field optional in the review form (a
                    # plain "string" forces a reason before Approve can be clicked); the empty
                    # default renders as an empty box, where some UI versions show a null one
                    # as "[object Object]".
                    "value": "",
                    "description": "Sent to the agent when you reject the call (optional).",
                    "schema": {"type": ["string", "null"]},
                },
                # A "const" schema renders as a hidden field the reviewer cannot change.
                _APPROVAL_REQUEST_PARAM: {
                    "value": pause_number,
                    "description": "Which approval request of this run the answer is for.",
                    "schema": {"type": "integer", "const": pause_number},
                },
            },
            assigned_users=self.tool_approval_assigned_users,
        )
        if not _REPEAT_APPROVAL_SUPPORTED:
            # Only once the request exists: a failed request must not block the retry from asking.
            context["task_state_store"].set(_TOOL_APPROVAL_REQUESTED_KEY, True, retention=NEVER_EXPIRE)
        self.log.info("Waiting for approval of %s", pending_names)
        raise TaskAwaitingInput(
            method_name=resume_with,
            kwargs={**continuation, "pause_number": pause_number},
            timeout=self.tool_approval_timeout,
        )

    def _delete_approval_state(self, store: TaskStateStoreAccessor, key: str) -> None:
        # Best-effort: the transcript and the answers say what the agent did, but failing the task
        # over their cleanup would be worse, and the rows go with the Dag run anyway.
        try:
            store.delete(key)
        except Exception:
            self.log.warning("Could not delete %s from the task state store", key, exc_info=True)

    def resume_after_tool_approval(
        self,
        context: Context,
        tool_call_ids: list[str],
        usage: dict[str, Any],
        transcript_sha256: str,
        toolset_ids: list[str],
        event: dict[str, Any],
        attempt_usage: dict[str, Any] | None = None,
        pause_number: int = 1,
    ) -> Any:
        """Continue a run paused by :meth:`_pause_for_tool_approval` with the reviewer's decision."""
        # Only a run without durable execution pauses this way, such as one paused before an
        # upgrade turned the default on, so the resume does not decide it again: its
        # continuation has no journal to replay from.
        store = context["task_state_store"]
        self._approval_pause_number = pause_number
        try:
            output = self._resume_after_tool_approval(
                context, store, tool_call_ids, usage, transcript_sha256, toolset_ids, event, attempt_usage
            )
        except TaskAwaitingInput:
            # The resumed run paused again: the transcript now belongs to that new pause.
            raise
        except BaseException:
            self._delete_approval_state(store, _TOOL_APPROVAL_TRANSCRIPT_KEY)
            raise
        self._delete_approval_state(store, _TOOL_APPROVAL_TRANSCRIPT_KEY)
        return output

    def _resume_after_tool_approval(
        self,
        context: Context,
        store: TaskStateStoreAccessor,
        tool_call_ids: list[str],
        usage: dict[str, Any],
        transcript_sha256: str,
        toolset_ids: list[str],
        event: dict[str, Any],
        attempt_usage: dict[str, Any] | None,
    ) -> Any:
        self._check_review_answer(event, toolset_ids)
        transcript = store.get(_TOOL_APPROVAL_TRANSCRIPT_KEY)
        if not isinstance(transcript, str) or (
            hashlib.sha256(transcript.encode()).hexdigest() != transcript_sha256
        ):
            raise ToolApprovalError(
                "The transcript saved when the task paused for tool approval is missing or was modified."
            )
        if not self._is_answer_for_waiting_request(event):
            calls = [
                part
                for message in ModelMessagesTypeAdapter.validate_json(transcript)
                if isinstance(message, ModelResponse)
                for part in message.parts
                if isinstance(part, ToolCallPart) and part.tool_call_id in tool_call_ids
            ]
            self._ask_for_tool_approval(
                context,
                calls,
                continuation={
                    "tool_call_ids": tool_call_ids,
                    "usage": usage,
                    "attempt_usage": attempt_usage,
                    "transcript_sha256": transcript_sha256,
                    "toolset_ids": toolset_ids,
                },
            )

        approval = self._build_approval_from_answer(event)

        usage_limits = coerce_usage_limits(self.usage_limits)
        ti = context["task_instance"]
        # Same try as the paused run, so the budget it saved is still this task instance's.
        self._usage_budget = self._build_usage_budget(context, usage_limits, ti=ti)
        # ``usage`` is the paused run's total, cumulative across attempts when the budget
        # applies; ``attempt_usage`` is this attempt's share of it, so the usage reported
        # after the resume covers both sides of the pause. A continuation written before
        # ``attempt_usage`` existed has none; only the resumed side is then reported.
        self._run_usage = _RUN_USAGE_ADAPTER.validate_python(usage)
        run_usage: RunUsage = self._run_usage
        self._run_usage_base = (
            subtract_run_usage(run_usage, _RUN_USAGE_ADAPTER.validate_python(attempt_usage))
            if attempt_usage is not None
            else copy_run_usage(run_usage)
        )

        agent = self._build_agent()
        self._run_identity_attrs = self._span_attributes(ti)
        stamp_identity_on_agent_spans(agent, self._run_identity_attrs)
        try:
            # The full transcript, not a trimmed one: pydantic-ai reads its last request to skip
            # the calls that already ran in the paused step. No new prompt: it would land after
            # the tool results as a second user turn.
            result, _ = self._run_agent_tracked(
                agent,
                None,
                run_usage=run_usage,
                message_history=ModelMessagesTypeAdapter.validate_json(transcript),
                deferred_tool_results=DeferredToolResults(
                    approvals={tool_call_id: approval for tool_call_id in tool_call_ids}
                ),
                usage_limits=usage_limits,
                # pydantic-ai refuses a run_id already in the history, so each resume of the try
                # gets its own; the task-instance id stays the prefix, so the resumed run still
                # joins back to the task.
                run_id=f"{ti.id}-resumed-{self._approval_pause_number}",
            )
        except BaseException:
            self._report_failed_run(context, run_usage)
            raise
        return self._complete_run(
            context, result, attempt_usage=subtract_run_usage(run_usage, self._run_usage_base)
        )

    def _check_review_answer(self, event: dict[str, Any], toolset_ids: list[str]) -> None:
        """Refuse to resume when the review ended without an answer, or the toolsets changed while it waited."""
        if "error" in event:
            if event.get("error_type") == "timeout":
                raise HITLTimeoutError(f"Tool approval timed out: {event['error']}")
            raise HITLTriggerEventError(event)
        if (current := self._toolset_ids()) != toolset_ids:
            raise ToolApprovalError(
                f"The agent's toolsets changed while it waited for approval (paused with {toolset_ids}, "
                f"resumed with {current}), so the approved calls would reach a connection the reviewer "
                "did not see."
            )

    def _is_answer_for_waiting_request(self, event: dict[str, Any]) -> bool:
        """Whether the answer is for the approval request waiting now, not one a form left open showed."""
        if (answered := _answered_request(event)) is None or answered == self._approval_pause_number:
            return True
        # A review form left open on an earlier pause: its answer was about other calls.
        self.log.warning(
            "An answer for approval request %s arrived while request %s was waiting; "
            "asking again instead of applying it.",
            answered,
            self._approval_pause_number,
        )
        return False

    def _build_approval_from_answer(self, event: dict[str, Any]) -> bool | ToolDenied:
        """Turn the reviewer's answer into the result each paused call gets: run it, or tell the agent why not."""
        if event.get("timedout"):
            # on_tool_approval_timeout="deny": nobody refused, so do not tell the agent a person did.
            self.log.info(
                "Tool calls denied: nobody answered within tool_approval_timeout=%s",
                self.tool_approval_timeout,
            )
            return ToolDenied("No reviewer answered within the approval timeout, so this call was not run.")
        if LLMApprovalMixin.APPROVE in event["chosen_options"]:
            self.log.info("Tool calls approved by %s", LLMApprovalMixin._describe_responder(event))
            return True
        reason = (event.get("params_input") or {}).get("reason")
        # Only a typed reason reaches the agent; an untouched field can come back as None, "",
        # or, from some UI versions, the whole parameter spec.
        if not isinstance(reason, str) or not reason.strip():
            reason = "A reviewer denied this tool call."
        self.log.info("Tool calls rejected by %s", LLMApprovalMixin._describe_responder(event))
        return ToolDenied(reason)

    def _load_tool_approval_answers(self, store: TaskStateStoreAccessor) -> None:
        """Read the answers reviewers gave this run, leaving out ones given for other toolsets."""
        stored = _STORED_ANSWERS_ADAPTER.validate_python(store.get(_TOOL_APPROVAL_ANSWERS_KEY) or [])
        self._tool_approval_answers_stored = bool(stored)
        toolset_ids = self._toolset_ids()
        answers = [_ToolApprovalAnswer.load(raw) for raw in stored]
        if self._new_tool_approval_answer is not None and answers:
            # The resume stored it last; keep it as the object the warnings look at.
            answers[-1] = self._new_tool_approval_answer
        # Like a resume, an answer never reaches a connection the reviewer did not see.
        self._tool_approval_answers = [answer for answer in answers if answer.toolset_ids == toolset_ids]
        if skipped := len(answers) - len(self._tool_approval_answers):
            self.log.warning(
                "Not applying %s approval answer(s) given while the agent had other toolsets.",
                skipped,
            )

    def _claim_next_approval_step(self, requests: DeferredToolRequests) -> int | None:
        if self._asking_other_handlers:
            return None
        # The task's agent asks for approval once its tool calls have finished, so a tool call still
        # running means this run is inside it. Asked of the journal, not the context: a tool can run
        # the agent in a thread that does not inherit the context, such as with ``loop.run_in_executor``.
        journal = self._durable_journal
        if journal is not None and (running := journal.find_executing_tool_steps()):
            # A tool's own work (an agent it runs, or calls it makes inline) is not run again once
            # the tool is recorded, so an approval reached there would not be reached on replay, and
            # every later approval step would be numbered one lower and get another step's answer.
            if not requests.approvals:
                _refuse_external_calls(requests)
            raise UnsupportedToolDeferralError(
                "The agent called tools that need approval "
                f"({', '.join(call.tool_name for call in requests.approvals)}) while a tool call was "
                f"running ({', '.join(repr(step.name) for step in running)}), such as from an agent run "
                "by a tool. With durable, a tool that needs approval has to be called by the task's agent "
                "itself."
            )
        self._approval_steps += 1
        return self._approval_steps

    async def _settle_tool_approvals(
        self, context: Context, ctx: RunContext[Any], requests: DeferredToolRequests, step: int
    ) -> DeferredToolResults:
        """
        Settle every call a durable run is waiting on, as its approval step number ``step``.

        The answers reviewers gave this step go first, each to the calls it was given for, so no
        handler can overrule one. The other capabilities' ``handle_deferred_tool_calls``
        handlers settle what they can of the rest, through the run's own chain, as they would
        without durable. Whatever is left is asked about, and the task parks: the resume runs
        the agent again, which replays every step before this one and reaches it with the answer.
        """
        settled = DeferredToolResults()
        for answer in self._tool_approval_answers:
            if answer.step != step:
                continue
            pending = [call for call in requests.approvals if call.tool_call_id not in settled.approvals]
            if taken := answer.take(pending):
                self.log.info(
                    "Applying the %s answer given at approval step %s to %s",
                    "approve" if answer.approval is True else "reject",
                    step,
                    ", ".join(call.tool_name for call in pending if call.tool_call_id in taken),
                )
                settled.approvals.update(taken)
        remaining: DeferredToolRequests | None = (
            requests.remaining(settled) if settled.approvals else requests
        )
        if remaining is not None and (root := ctx.root_capability) is not None:
            self._asking_other_handlers = True
            try:
                result = await root.handle_deferred_tool_calls(ctx, requests=remaining)
            finally:
                self._asking_other_handlers = False
            if result:
                result = _filter_results_to_requests(remaining, result)
                settled.update(result)
                remaining = remaining.remaining(result)
        if remaining is None:
            return settled
        _refuse_external_calls(remaining)
        if (new := self._new_tool_approval_answer) is not None and new.calls:
            self.log.warning(
                "The resumed run reached tool calls no answer covers (%s); asking about them.",
                ", ".join(call.tool_name for call in remaining.approvals),
            )
        self._refuse_repeat_approval_request(context, remaining.approvals)
        self._ask_for_tool_approval(
            context,
            remaining.approvals,
            continuation={
                "step": step,
                "calls": [[call.tool_name, _fingerprint_call_content(call)] for call in remaining.approvals],
                "toolset_ids": self._toolset_ids(),
            },
            resume_with=_DURABLE_APPROVAL_RESUME,
        )

    def resume_after_durable_tool_approval(
        self,
        context: Context,
        step: int,
        calls: list[list[str | None]],
        toolset_ids: list[str],
        usage: dict[str, Any],
        attempt_usage: dict[str, Any],
        event: dict[str, Any],
        pause_number: int,
    ) -> Any:
        """
        Continue a durable run that paused for tool approval, with the reviewer's decision.

        The answer joins the run's earlier ones in the task state store, for approval step
        ``step`` and the calls in ``calls`` (tool names and argument fingerprints). The task
        then runs again from ``execute``, under the task's journal: every step before the pause
        replays, and the approval step, which the pause left unrecorded, gets the answer.
        """
        self._approval_pause_number = pause_number
        self._check_review_answer(event, toolset_ids)
        if self._is_answer_for_waiting_request(event):
            answer = _ToolApprovalAnswer(
                step=step,
                calls=[(str(name), fingerprint) for name, fingerprint in calls],
                toolset_ids=toolset_ids,
                approval=self._build_approval_from_answer(event),
            )
            store = context["task_state_store"]
            stored = _STORED_ANSWERS_ADAPTER.validate_python(store.get(_TOOL_APPROVAL_ANSWERS_KEY) or [])
            store.set(_TOOL_APPROVAL_ANSWERS_KEY, [*stored, answer.dump()], retention=NEVER_EXPIRE)
            self._new_tool_approval_answer = answer
        # Without an answer for these calls, the run reaches them and asks again.
        self._usage_at_pause = (
            _RUN_USAGE_ADAPTER.validate_python(usage),
            _RUN_USAGE_ADAPTER.validate_python(attempt_usage),
        )
        return self.execute(context)

    def _resolve_message_history(self) -> list[ModelMessage] | None:
        """
        Deserialize :attr:`message_history` into a list of pydantic-ai messages.

        ``None`` means single-turn (no history passed to the run). A ``str`` /
        ``bytes`` value is parsed as the JSON the operator emits to XCom; a list
        (of ``ModelMessage`` objects or their dict form) is validated as-is.
        """
        raw = self.message_history
        if raw is None:
            return None
        if isinstance(raw, (str, bytes)) and not raw.strip():
            # A template that renders to empty (no prior XCom) starts a fresh session.
            return []
        if isinstance(raw, (str, bytes)):
            return ModelMessagesTypeAdapter.validate_json(raw)
        return ModelMessagesTypeAdapter.validate_python(raw)

    def _emit_message_history(self, context: Context, result: Any) -> None:
        """Push the full post-run transcript to XCom for the next turn to resume."""
        transcript = ModelMessagesTypeAdapter.dump_json(result.all_messages()).decode()
        context["task_instance"].xcom_push(key="message_history", value=transcript)

    def _emit_run_metadata(self, context: Context, result: Any, *, usage: RunUsage) -> None:
        """Expose the pydantic-ai run id, resolved model name, and token usage on XCom."""
        if not self.do_xcom_push:
            return
        ti = context["task_instance"]
        ti.xcom_push(key="run_id", value=result.run_id)
        ti.xcom_push(key="usage", value=format_usage_for_xcom(usage))
        if (model_name := getattr(result.response, "model_name", None)) is not None:
            ti.xcom_push(key=MODEL_NAME_XCOM_KEY, value=model_name)

    def dump_review_state(self, message_history: Any) -> str:
        """Save the conversation, and what the task returns if the output under review is approved."""
        output = self._output_under_review
        state: dict[str, Any] = {
            "messages": ModelMessagesTypeAdapter.dump_python(message_history, mode="json")
        }
        output_json = dump_output_to_json(output)
        if (
            rehydrate_pydantic_output(self.output_type, output_json, serialize_output=False, is_edited=False)
            == output
        ):
            state["output_json"] = output_json
        else:
            # Reading the JSON back would give a different value: an output function's or an output
            # marker's result stays text, and "42" under ``str | int`` becomes 42. Save the result instead.
            state["result"] = to_jsonable_python(self._convert_output_for_xcom(output))
        return json.dumps(state)

    def load_review_state(self, context: Context, state: str) -> Any:
        """Restore the conversation and the usage budget before regenerating in the resumed task."""
        ti = context["task_instance"]
        # The regeneration shares the budget the paused rounds spent, as it does without a pause.
        self._usage_budget = self._build_usage_budget(context, coerce_usage_limits(self.usage_limits), ti=ti)
        self._run_usage = self._usage_budget.load() if self._usage_budget else None
        self._run_identity_attrs = self._span_attributes(ti)
        return ModelMessagesTypeAdapter.validate_python(json.loads(state)["messages"])

    def complete_hitl_review(self, context: Context, approved_output: str, *, state: str | None) -> Any:
        """Return the approved output as the agent returned it; ``approved_output`` is only its display text."""
        # The reviewer can approve, reject, or ask for a new answer, but cannot edit the answer text.
        # So the approved answer is the last answer the agent returned.
        if state is None:
            result = self._convert_output_for_xcom(self._output_under_review)
        elif "result" in (saved := json.loads(state)):
            result = saved["result"]
        else:
            result = self._convert_output_for_xcom(
                rehydrate_pydantic_output(
                    self.output_type, saved["output_json"], serialize_output=False, is_edited=False
                )
            )
        if self._durable_journal is not None:
            # The approved output is the task's result, so nothing replays the review again.
            self._durable_journal.cleanup()
            self._delete_review_answers(context)
        self._clear_usage_budget(context)
        return result

    def resume_hitl_review(
        self,
        context: Context,
        iteration: int,
        deadline: str | None,
        state_sha256: str,
        event: dict[str, Any],
    ) -> Any:
        self._decide_durable(context)
        if self._is_durable_run:
            # The regeneration after round ``iteration`` is the journal's run ``iteration``, in this
            # process as on a retry that replays the rounds before it; run 0 made the first output.
            self._durable_journal = self._open_journal(context, first_run=iteration)
        with self._durable_review(context):
            return super().resume_hitl_review(context, iteration, deadline, state_sha256, event)

    @contextmanager
    def _durable_review(self, context: Context) -> Iterator[None]:
        """Under durable, make a review that ends in a reject or ``max_hitl_iterations`` start over on retry."""
        try:
            yield
        except (HITLRejectException, HITLMaxIterationsError):
            if (journal := self._durable_journal) is not None:
                # Replaying would show the reviewer the outputs of this review again. The retry asks
                # the model again for the first output, replaying the tool calls before it, and
                # regenerates from there.
                self.log.info(
                    "The review ended without an approved output; a retry asks the model again for "
                    "the first output and starts the review over."
                )
                journal.ask_model_again(0)
                self._delete_review_answers(context)
            raise

    @staticmethod
    def _load_review_answers(context: Context) -> list[_ReviewAnswer]:
        return _REVIEW_ANSWERS_ADAPTER.validate_python(
            context["task_state_store"].get(_HITL_REVIEW_ANSWERS_KEY) or []
        )

    def _get_recorded_review_feedback(self, context: Context, session: AgentSessionData) -> str | None:
        if self._durable_journal is None:
            return None
        answers = self._load_review_answers(context)
        if (answer := next((a for a in answers if a["iteration"] == session.iteration), None)) is None:
            return None
        if answer["output_sha256"] != hashlib.sha256(session.current_output.encode()).hexdigest():
            # The replay went another way, such as after a change to the Dag.
            self.log.warning(
                "The output of iteration %d is not the one the reviewer asked to change in an earlier "
                "attempt, so the reviewer is asked again.",
                session.iteration,
            )
            return None
        return answer["feedback"]

    def _record_review_feedback(self, context: Context, session: AgentSessionData, feedback: str) -> None:
        if self._durable_journal is None:
            return
        # As in execute, building settles the decision before anything depends on it: the answer
        # stored below, and the span attributes load_review_state snapshots next.
        self._regeneration_agent = self._build_agent_as_decided()
        if self._durable_journal is None:
            # pydantic-ai refused the durable agent, and the default turned durable execution off.
            return
        # The regenerations after this round's earlier answer, on an earlier attempt, followed from
        # that answer; one that replayed a first step with no fingerprint would bring it back.
        self._durable_journal.drop_runs_from(session.iteration)
        if "task_state_store" not in context:
            # Before Airflow 3.3 the review polls, and a retry asks the reviewer again.
            return
        # The answers for later rounds followed from this round's old answer, if it had one.
        answers = [
            answer for answer in self._load_review_answers(context) if answer["iteration"] < session.iteration
        ]
        answers.append(
            _ReviewAnswer(
                iteration=session.iteration,
                output_sha256=hashlib.sha256(session.current_output.encode()).hexdigest(),
                feedback=feedback,
            )
        )
        context["task_state_store"].set(
            _HITL_REVIEW_ANSWERS_KEY,
            _REVIEW_ANSWERS_ADAPTER.dump_python(answers, mode="json"),
            retention=NEVER_EXPIRE,
        )

    def _delete_review_answers(self, context: Context) -> None:
        if (store := context.get("task_state_store")) is not None:
            self._delete_approval_state(store, _HITL_REVIEW_ANSWERS_KEY)

    def regenerate_with_feedback(self, *, feedback: str, message_history: Any) -> tuple[str, Any]:
        """
        Re-run the agent with *feedback* appended to the conversation history.

        Shares the cross-run ``RunUsage`` with the run that produced the output being
        reviewed -- so a ``usage_limits`` cap bounds the initial run plus every
        regeneration combined -- only when ``usage_limits`` is set. With
        ``usage_limits=None``, each regeneration starts from a fresh ``RunUsage()``,
        matching the behaviour before the cross-attempt budget existed: nothing shares
        usage. This applies on every Airflow version; only the *cross-attempt*
        persistence of a shared, budget-tracked ``RunUsage`` (via the task state store)
        is gated on >= 3.3, in ``execute()``.
        """
        usage_limits = coerce_usage_limits(self.usage_limits)
        agent = self._regeneration_agent or self._build_agent_as_decided()
        self._regeneration_agent = None
        identity = getattr(self, "_run_identity_attrs", None)
        if identity:
            stamp_identity_on_agent_spans(agent, identity)
        messages = message_history or []
        if usage_limits is None or self._run_usage is None:
            # No budget requested, or called directly outside execute() (e.g. a
            # standalone regeneration) -- always start fresh rather than share
            # `self._run_usage` with whatever produced the output under review.
            self._run_usage = RunUsage()
        run_usage: RunUsage = self._run_usage
        run_kwargs: dict[str, Any] = {}
        journal = self._durable_journal
        if journal is not None:
            # Each regeneration logs what it replayed and recorded, like the run before the review.
            journal.stats = JournalStats()
            # The same id on every attempt, as for the first run; a new one per round, as pydantic-ai
            # refuses an id already in the conversation.
            run_id = journal.get_run_id(default=uuid.uuid4().hex)
            run_kwargs["run_id"] = f"{run_id}-review-{journal.next_run_number}"
        try:
            with self._journal_scope():
                result, regen_usage = self._run_agent_tracked(
                    agent,
                    feedback,
                    run_usage=run_usage,
                    message_history=messages,
                    usage_limits=usage_limits,
                    **run_kwargs,
                )
        finally:
            if journal is not None:
                journal.log_summary(self.log, mode=self._durable_mode)
        if isinstance(result.output, DeferredToolRequests):
            # The first run's check in _complete_run: a review cannot pause for tool approval.
            _refuse_external_calls(result.output)
            _refuse_unsupported_approval(result.output)
        # This regeneration's own delta, not `result.usage` -- which, when `run_usage` is
        # shared, is the seeded cumulative object, not what this call alone contributed.
        log_run_summary(self.log, result, usage=regen_usage)

        self._output_under_review = result.output
        return self._to_string(result.output), result.all_messages()
