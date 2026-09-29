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

import collections
import copy
import hashlib
import json
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import replace
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn

from pydantic import BaseModel, TypeAdapter
from pydantic_ai import DeferredToolRequests, DeferredToolResults, ToolDenied
from pydantic_ai.capabilities import AbstractCapability, Toolset, WrapperCapability
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.toolsets.abstract import AbstractToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.exceptions import (
    ToolApprovalAlreadyRequestedError,
    ToolApprovalError,
    UnsupportedToolDeferralError,
)
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin, normalize_assigned_users
from airflow.providers.common.ai.mixins.cancellable_run import CancellableAgentRunMixin
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.observability import (
    build_run_identity_attributes,
    make_task_instance_run_key,
    stamp_identity_on_agent_spans,
)
from airflow.providers.common.ai.toolsets.logging import ToolLoggingCapability
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.utils.logging import (
    format_usage_for_xcom,
    log_run_summary,
    log_run_usage,
)
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output
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
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS
from airflow.providers.standard.exceptions import HITLTimeoutError, HITLTriggerEventError

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
    import jinja2
    from pydantic_ai import Agent
    from pydantic_ai.capabilities import AgentCapability
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.usage import UsageLimits

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.providers.common.ai.durable.caching_model import CachingModel
    from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
    from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
    from airflow.providers.common.compat.sdk import TaskInstanceKey
    from airflow.sdk import Context
    from airflow.sdk.execution_time.context import TaskStateStoreAccessor
    from airflow.sdk.execution_time.hitl import HITLUser

# Task state store keys: the transcript of a run paused for tool approval, and a marker that
# this task instance has asked once. The store is keyed by Dag run, task and map index, so the
# marker survives retries and clears, as the task instance's single approval request does.
_TOOL_APPROVAL_TRANSCRIPT_KEY = "common_ai_tool_approval_transcript"
_TOOL_APPROVAL_REQUESTED_KEY = "common_ai_tool_approval_requested"
# How long the transcript outlives a timed pause, so a resume that runs late still finds it.
_TRANSCRIPT_RETENTION_MARGIN = timedelta(days=1)
_RUN_USAGE_ADAPTER: TypeAdapter[RunUsage] = TypeAdapter(RunUsage)


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


def _replace_capability_toolset(
    capability: AgentCapability[Any], wrap: Callable[[AbstractToolset[Any]], AbstractToolset[Any]]
) -> AgentCapability[Any]:
    """Return a ``Toolset`` capability holding ``wrap(toolset)``; any other capability comes back as is."""
    if isinstance(capability, Toolset) and isinstance(capability.toolset, AbstractToolset):
        return replace(capability, toolset=wrap(capability.toolset))
    return capability


def _contains_code_mode(capabilities: Iterable[AgentCapability[Any]]) -> bool:
    """
    Whether any capability, or one nested inside a combined or wrapper capability, is ``CodeMode``.

    A capability function, or a ``DynamicCapability``, builds its capability when the run
    starts, so there is nothing to inspect here.
    """
    # CodeMode's own module is in sys.modules once CodeMode has been imported, and only then.
    # The pydantic_ai_harness package root is not a safe place to look: it exports CodeMode
    # through a module __getattr__ that imports that module, which fails without the
    # ``code-mode`` extra.
    code_mode_cls = getattr(sys.modules.get("pydantic_ai_harness.code_mode"), "CodeMode", None)
    if code_mode_cls is None:
        return False
    pending = [capability for capability in capabilities if isinstance(capability, AbstractCapability)]
    while pending:
        capability = pending.pop()
        if isinstance(capability, code_mode_cls):
            return True
        if isinstance(capability, WrapperCapability):
            # apply() does not visit a wrapper's single wrapped capability, only a combined one's children.
            pending.append(capability.wrapped)
        else:
            children: list[AbstractCapability[Any]] = []
            capability.apply(children.append)
            pending.extend(child for child in children if child is not capability)
    return False


def _declares_agent_template_fields(toolset: Any) -> bool:
    """Whether *toolset*, or a toolset it wraps or combines, has connection IDs to render."""
    return isinstance(toolset, AbstractToolset) and any(
        getattr(leaf, "agent_template_fields", None) for leaf in iter_toolsets(toolset)
    )


# CancellableAgentRunMixin must precede BaseOperator so its on_kill overrides BaseOperator's
# no-op. The other mixins only add methods, so they can trail BaseOperator. See the MRO guard
# test in tests/unit/common/ai/mixins/test_cancellable_run.py.
class AgentOperator(CancellableAgentRunMixin, BaseOperator, HITLReviewMixin):
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
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output; the model instance is
        returned to XCom unchanged so downstream tasks can type-hint it
        directly. The class must be defined at module scope -- nested classes
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
    :param enable_tool_logging: When ``True`` (default), wraps the agent's
        assembled function toolset in a ``LoggingToolset`` that logs tool calls
        with timing at INFO level and arguments at DEBUG level. This includes
        tools supplied through ``toolsets=``, ``agent_params["tools"]``, and
        capabilities, but not output tools or provider-native tools that run
        server-side. Set to ``False`` to disable.
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
    :param durable: Experimental. When ``True``, enables step-level caching of model
        responses and tool results for durable execution.  On retry, cached
        steps are replayed instead of re-executing.  Each cached step is
        verified against the current request before replay: if the prompt,
        model, settings, tools, or message history changed since the failed
        attempt, the affected steps re-run live (with a warning) instead of
        replaying stale results.  Default ``False``. A replayed step adds
        nothing to the usage counted against ``usage_limits`` or reported in
        the ``usage`` XCom -- not its request, tokens, cost, or tool calls --
        so every attempt counts only the model and tool calls it actually
        makes. This holds the same way after clearing a failed task
        instance: it starts a fresh budget but keeps the durable cache its
        attempts left behind, and whatever the rerun replays from that cache
        is free.
        On Airflow >= 3.3 the cache is kept in the AIP-103 task state store, so
        no extra configuration is needed. On older Airflow versions it is persisted to
        ObjectStorage and requires ``[common.ai] durable_cache_path`` to be set.
        Tools are durably cached when provided via ``toolsets=`` or via a
        concrete pydantic-ai ``Toolset`` capability. Tools reaching the agent
        through any *other* capability -- ``MCP``, ``PrefixTools``,
        ``CombinedCapability``, a ``Toolset`` backed by a callable factory, or
        capabilities loaded from a ``spec_file`` -- are not cached and re-run on
        retry; put tools you need replayed in ``toolsets=``. Provider-native
        capabilities such as ``WebSearch`` and ``Thinking`` execute inside the
        model call and are covered by model-response caching.
        Cannot be combined with a ``SandboxToolset`` (raises), attached or
        not: a replayed tool result describes a workspace state the replay did
        not reproduce, and the first call that misses the cache runs against
        whatever the sandbox holds now. Cannot be combined with a pydantic-ai-harness
        ``CodeMode`` capability (raises).
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

    **HITL Review parameters** (requires the ``hitl_review`` plugin):

    :param enable_hitl_review: When ``True``, the operator enters an
        iterative review loop after the first generation.  A human reviewer
        can approve, reject, or request changes via the plugin's REST API
        at ``/hitl-review`` or through the **HITL Review** extra link
        on the task instance.  Default ``False``. Cannot be combined with a
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
        all review rounds combined.  ``None`` means no timeout (the
        operator blocks until a terminal action).
    :param hitl_poll_interval: Seconds between XCom polls
        while waiting for a human response.  Default ``10``.

    **Per-tool approval** (Airflow 3.3+, experimental):

    Mark the tools a human must approve with pydantic-ai's own API --
    ``toolset.approval_required(...)``, or ``requires_approval=True`` on a function
    tool -- and the task pauses before running them. The pending calls, with their
    arguments, appear on the **Required Actions** page; the task waits in the
    ``awaiting_input`` state without holding a worker slot. On **Approve** the calls
    run and the agent carries on. On **Reject** the agent is told the call was denied
    (with the reviewer's reason, when given) and carries on without it. A task
    instance asks at most once per Dag run, across retries and clears; a second
    request fails the task. ``usage_limits`` applies to both sides of the pause.
    Not available together with ``durable``, ``enable_hitl_review``, a ``CodeMode``
    capability, or a ``SandboxToolset``
    that provisions its own sandbox; there, a tool that requires approval fails
    the task as before, except one called from inside ``CodeMode``'s ``run_code``,
    which does not run and is reported back to the model. A ``SandboxToolset`` attached to a
    sandbox another task owns is fine: the sandbox outlives the pause.

    :param tool_approval_timeout: Experimental. How long the pause waits for a decision.
        ``None`` (default) waits indefinitely. Must be positive.
    :param on_tool_approval_timeout: Experimental. What a timed-out pause does: ``"fail"``
        (default) fails the task, ``"deny"`` rejects the pending calls so the agent
        carries on without them, and needs a ``tool_approval_timeout``. There is no
        approve-on-timeout.
    :param tool_approval_assigned_users: Experimental. Users allowed to decide. ``None`` (default)
        leaves it to anyone who can act on the task's Required Actions.

    :param serialize_output: If ``True`` and ``output_type`` is a Pydantic
        ``BaseModel`` subclass, the model instance is dumped to a ``dict`` via
        ``model_dump()`` before being pushed to XCom. Default ``False`` --
        the Pydantic instance flows through XCom unchanged. Set to ``True``
        when a downstream consumer needs the dict shape.
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
        output_type: type = str,
        toolsets: list[AbstractToolset] | None = None,
        capabilities: list[AgentCapability[Any]] | None = None,
        enable_tool_logging: bool = True,
        agent_params: dict[str, Any] | None = None,
        usage_limits: UsageLimits | dict[str, Any] | None = None,
        durable: bool = False,
        cache_prompt: bool = True,
        message_history: list[ModelMessage] | str | bytes | None = None,
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

        self.durable = durable
        self.cache_prompt = cache_prompt

        # Populated per run in ``execute`` when durable=True. Declared here so
        # ``_build_agent`` -- also reached via ``regenerate_with_feedback``
        # outside ``execute`` -- can read them unconditionally.
        self._durable_storage: DurableStorageProtocol | None = None
        self._durable_counter: DurableStepCounter | None = None
        self._replay_usage: ReplayUsageLedger | None = None

        # Populated in ``execute``; also read (and, if unset, lazily initialized) by
        # ``regenerate_with_feedback`` outside ``execute``, which is why they need a
        # declared default here rather than only being set inline in ``execute``.
        self._usage_budget: TaskStateStoreUsageBudget | None = None
        self._run_usage: RunUsage | None = None
        self._run_usage_base: RunUsage = RunUsage()

        # Checked ahead of the combination rules below. When Airflow is older than 3.1, its version
        # is the real blocker, and reporting a combination error first would send the
        # user to drop an argument that was never the problem -- they would hit this anyway.
        if enable_hitl_review and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "Human in the loop functionality needs Airflow 3.1+."
            )

        if durable and enable_hitl_review:
            raise ValueError("durable=True and enable_hitl_review=True cannot be used together.")

        if durable and _contains_code_mode(self._declared_capabilities):
            # Durable replay caches individual model/tool steps via CachingModel /
            # CachingToolset and a shared step counter that assumes a stable call
            # order across runs. Code mode collapses tools into one ``run_code``
            # tool and lets the model emit arbitrary Python, so step counts and
            # ordering can differ between the original run and a retry, breaking
            # replay. Reject the combination rather than silently mis-replaying.
            raise ValueError("durable=True cannot be used with a CodeMode capability.")

        if message_history is not None and enable_hitl_review:
            # The post-review transcript is not recoverable today (run_hitl_review
            # returns only the final string), so emitting the pre-review transcript
            # would silently drop the human-approved turns. Block until HITL can
            # surface the final message history.
            raise ValueError("message_history and enable_hitl_review=True cannot be used together.")

        if durable or enable_hitl_review:
            self._reject_sandbox_without_continuity(durable=durable, enable_hitl_review=enable_hitl_review)

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

    def _reject_sandbox_without_continuity(self, *, durable: bool, enable_hitl_review: bool) -> None:
        """
        Refuse a ``SandboxToolset`` under a feature that assumes the sandbox outlives the run.

        A sandbox is provisioned on the first tool call and destroyed when the run
        ends, so nothing in it survives into a retry or a second run. Two features
        assume otherwise, and each produces a wrong answer rather than an error:

        * ``durable=True`` replays cached tool results on a retry without calling the
          backend, so a replayed ``write_file`` reports success while no sandbox exists,
          and the first call that misses the cache runs against a fresh, empty one.
        * ``enable_hitl_review=True`` regenerates after reviewer feedback by starting a
          second agent run, which gets an empty sandbox while its message history still
          describes the files the first run wrote.

        A toolset attached to a sandbox another task owns (``attach_to``) keeps its
        files across runs, so HITL review is allowed with it: the regenerated run finds
        what the first run wrote. Durable replay stays refused even then, because a
        replayed tool result is not re-executed, so the workspace does not move with the
        transcript; a cached ``write_file`` on a retry leaves no file behind.

        The toolset is looked for inside wrappers and combinations (``.prefixed()``,
        ``.filtered()``, several toolsets passed together) and inside ``Toolset``
        capabilities, since those are the compositions the documentation recommends.
        A toolset resolved per run from a callable cannot be inspected here.
        """
        sandboxes = self._sandbox_toolsets()
        if durable and sandboxes:
            raise ValueError(
                "durable=True cannot be used with a SandboxToolset: cached tool results would be "
                "replayed without touching the sandbox, so the workspace would not match the "
                "transcript. Drop durable=True, or move the sandbox work into its own task."
            )
        if enable_hitl_review and any(sandbox.attach_to is None for sandbox in sandboxes):
            raise ValueError(
                "enable_hitl_review=True cannot be used with a SandboxToolset that provisions its own "
                "sandbox: a regenerated run would start from an empty sandbox while its history "
                "describes files from the first run. Attach the toolset to a sandbox another task "
                "provisioned (attach_to=...), drop enable_hitl_review=True, or move the sandbox work "
                "into its own task."
            )

    def _sandbox_toolsets(self) -> list[SandboxToolset]:
        """Every ``SandboxToolset`` the agent was given, looked for inside wrappers and combinations."""
        return [
            nested
            for toolset in self._declared_toolsets()
            for nested in iter_toolsets(toolset)
            if isinstance(nested, SandboxToolset)
        ]

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
            # of which connection this task instance's agent was given. @task.agent renders a
            # second time, when the id no longer changes, so this logs once per task instance.
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

    def _build_agent(self) -> Agent[object, Any]:
        """Build and return a pydantic-ai Agent from the operator's config."""
        extra_kwargs = dict(self.agent_params)
        passed_through = extra_kwargs.pop("capabilities", None)
        if passed_through is not None and self.capabilities is not None:
            # pydantic-ai wraps capability hooks in list order, so merging the two lists
            # would pick an order the Dag author never wrote down.
            raise ValueError("Pass capabilities either as capabilities=... or in agent_params, not both.")
        storage = self._durable_storage
        counter = self._durable_counter
        if self.toolsets:
            # Innermost, so the durable cache only ever stores masked results.
            toolsets: list[AbstractToolset] = [ensure_masked(ts) for ts in self.toolsets]
            if self.durable and storage is not None and counter is not None:
                toolsets = self._build_durable_toolsets(toolsets, storage, counter)
            extra_kwargs["toolsets"] = toolsets
        elif extra_kwargs.get("toolsets"):
            extra_kwargs["toolsets"] = [ensure_masked(ts) for ts in extra_kwargs["toolsets"]]
        capabilities = [
            _replace_capability_toolset(capability, ensure_masked)
            for capability in self.capabilities or passed_through or []
        ]
        if self.durable and storage is not None and counter is not None:
            # Tools supplied through a ``Toolset`` capability bypass the
            # ``toolsets=`` wrapping above, so their results would re-execute on
            # every retry instead of replaying; wrap their inner toolset too.
            capabilities = self._build_durable_capabilities(capabilities, storage, counter)
        if self.cache_prompt:
            capabilities.append(PromptCaching())
        if self.enable_tool_logging:
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

    def _supports_tool_approval(self) -> bool:
        """
        Whether a tool that requires approval pauses the task instead of failing it.

        Each excluded feature assumes the run finishes in one go: durable replay counts
        steps across a single run, HITL review and code mode wrap the run, and a
        sandbox the toolset provisions itself is destroyed when the run ends, so its
        files would be gone on resume. A sandbox another task owns (``attach_to``)
        outlives the pause, and the resumed run attaches to it again.
        """
        if (
            not AIRFLOW_V_3_3_PLUS
            or self.durable
            or self.enable_hitl_review
            or _contains_code_mode(self._declared_capabilities)
        ):
            return False
        return all(sandbox.attach_to is not None for sandbox in self._sandbox_toolsets())

    def _agent_output_type(self) -> Any:
        """
        Return ``output_type``, plus ``DeferredToolRequests`` when tool approval is supported.

        pydantic-ai drops ``DeferredToolRequests`` from the output schema the model sees;
        it only lets the run end on a tool call awaiting approval. ``self.output_type`` is
        left alone because it is part of the serialized Dag.
        """
        if not self._supports_tool_approval():
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

    def _build_durable_toolsets(
        self, toolsets: list[AbstractToolset], storage: DurableStorageProtocol, counter: DurableStepCounter
    ) -> list[AbstractToolset]:
        """Wrap each toolset with CachingToolset for durable execution."""
        from airflow.providers.common.ai.durable.caching_toolset import CachingToolset

        return [
            CachingToolset(wrapped=ts, storage=storage, counter=counter, replay_usage=self._replay_usage)
            for ts in toolsets
        ]

    def _build_durable_capabilities(
        self, capabilities: list[Any], storage: DurableStorageProtocol, counter: DurableStepCounter
    ) -> list[Any]:
        """
        Wrap toolsets provided via a pydantic-ai ``Toolset`` capability for durable replay.

        Tools reaching the agent through ``capabilities=[Toolset(ts)]`` bypass the
        operator's ``toolsets=`` list, so the ``CachingToolset`` applied in
        :meth:`_build_durable_toolsets` never sees them and their results
        re-execute on every retry instead of replaying. Wrap each ``Toolset``
        capability's inner toolset with the same ``CachingToolset``, preserving
        the capability's other fields. Non-``Toolset`` capabilities pass through
        unchanged, as does a ``Toolset`` holding a callable factory rather than a
        concrete toolset (only a concrete toolset can be wrapped here).
        """
        from airflow.providers.common.ai.durable.caching_toolset import CachingToolset

        rewrapped: list[Any] = []
        for capability in capabilities:
            # ``Toolset.toolset`` can be a concrete toolset or a callable factory
            # resolved per run; only a concrete toolset can be wrapped here.
            toolset = _resolve_capability_toolset(capability)
            if toolset is not None:
                cached = CachingToolset(
                    wrapped=toolset,
                    storage=storage,
                    counter=counter,
                    replay_usage=self._replay_usage,
                )
                rewrapped.append(replace(capability, toolset=cached))
                continue
            if isinstance(capability, Toolset):
                # The toolset is a callable factory resolved per run, so there is
                # no concrete toolset to wrap; its results won't be cached for
                # replay. Warn so durable users aren't silently surprised on retry.
                self.log.warning(
                    "durable=True: tools from a Toolset capability backed by a callable "
                    "factory are not cached for replay; pass the toolset via `toolsets=` "
                    "for durability."
                )
            rewrapped.append(capability)
        return rewrapped

    def _log_durable_summary(self, counter: DurableStepCounter) -> None:
        """
        Log what this attempt replayed and cached, and which steps it could not cache.

        A step whose cache write was skipped (a tool result that is not
        JSON-serializable, a store write that fails) ran live but is not cached,
        so a retry runs it again. For a tool with side effects the side effect
        repeats, so those tools are named rather than counted as cached.
        """
        self.log.info(
            "Durable: replayed %d cached steps (%d model, %d tool), cached %d new steps (%d model, %d tool)",
            counter.replayed_model + counter.replayed_tool,
            counter.replayed_model,
            counter.replayed_tool,
            counter.cached_model + counter.cached_tool,
            counter.cached_model,
            counter.cached_tool,
        )
        if counter.skipped_tools:
            calls = collections.Counter(counter.skipped_tools)
            self.log.warning(
                "Durable: %d tool results were not cached, and a retry runs them again: %s",
                len(counter.skipped_tools),
                ", ".join(name if n == 1 else f"{name} (x{n})" for name, n in calls.items()),
            )
        if counter.skipped_model:
            self.log.warning(
                "Durable: %d model responses were not cached, and a retry re-runs them "
                "and every step after the first of them",
                counter.skipped_model,
            )

    def _build_durable_storage(self, context: Context) -> DurableStorageProtocol:
        """
        Return the durable storage backend for the current task instance.

        On Airflow >= 3.3 durable steps are cached in the AIP-103 task state
        store, which handles persistence and large-value offload natively, so no
        ``[common.ai] durable_cache_path`` is required. On older Airflow versions, fall back
        to the ObjectStorage backend configured via ``durable_cache_path``.
        """
        if AIRFLOW_V_3_3_PLUS:
            # Imported lazily: NEVER_EXPIRE and the task state store accessor do
            # not exist on Airflow versions before 3.3.
            from airflow.providers.common.ai.durable.task_state_store import TaskStateStoreDurableStorage

            return TaskStateStoreDurableStorage(context["task_state_store"])

        from airflow.providers.common.ai.durable.storage import DurableStorage

        ti = context["task_instance"]
        return DurableStorage(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index if ti.map_index is not None else -1,
        )

    def _build_usage_budget(
        self, context: Context, usage_limits: UsageLimits | None, *, ti: Any
    ) -> TaskStateStoreUsageBudget | None:
        """
        Return the cross-attempt usage-budget accessor, or ``None`` when it should not apply.

        Gated like ``_build_durable_storage``: only on Airflow >= 3.3, where the task
        state store survives retries. Also gated on ``usage_limits is not None`` --
        with ``usage_limits=None`` turning this on would silently impose pydantic-ai's
        default ``request_limit=50`` across every attempt of every ``AgentOperator`` on
        3.3+, which nobody asked for.
        """
        if not (AIRFLOW_V_3_3_PLUS and usage_limits is not None):
            return None
        return TaskStateStoreUsageBudget(context["task_state_store"], max_tries=ti.max_tries)

    def _report_failed_run(self, context: Context, run_usage: RunUsage) -> None:
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
            ti.xcom_push(key="run_id", value=make_task_instance_run_key(ti))
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
        caching_model: CachingModel | None = None,
        **run_kwargs: Any,
    ) -> tuple[Any, RunUsage]:
        """
        Run the agent, persisting cumulative usage after every attempt (success or failure).

        ``run_usage`` -- always ``self._run_usage`` -- is taken as an explicit,
        non-Optional parameter (rather than read off ``self``) purely so mypy can
        narrow it without an ``assert``: ``self._run_usage`` is declared ``RunUsage |
        None`` because it is set lazily, but every caller of this method has already
        ensured it is a real ``RunUsage`` by the time it gets here.

        With ``durable=True``, the replay ledger's unused credits are given back before
        the total is persisted (see ``ReplayUsageLedger.settle``).
        """
        base = copy_run_usage(run_usage)
        try:
            if caching_model is not None:
                # After the snapshot above, so the credit never shows up in this attempt's delta.
                caching_model.credit_first_replay()
            result = self.run_agent_sync(agent, prompt, usage=run_usage, **run_kwargs)
        finally:
            if self._replay_usage is not None:
                # A replay credit the run never used must not reach the persisted total.
                self._replay_usage.settle()
            if self._usage_budget:
                self._usage_budget.save(run_usage)
        return result, subtract_run_usage(run_usage, base)

    def _run_and_report_on_failure(
        self,
        context: Context,
        agent: Agent[Any, Any],
        run_usage: RunUsage,
        run_kwargs: dict[str, Any],
        caching_model: CachingModel | None = None,
    ) -> tuple[Any, RunUsage]:
        """Run ``self.prompt`` via ``_run_agent_tracked``, reporting usage-at-failure on any raise."""
        try:
            if caching_model is not None:
                with agent.override(model=caching_model):
                    return self._run_agent_tracked(
                        agent, self.prompt, run_usage=run_usage, caching_model=caching_model, **run_kwargs
                    )
            return self._run_agent_tracked(agent, self.prompt, run_usage=run_usage, **run_kwargs)
        except BaseException:
            self._report_failed_run(context, run_usage)
            raise

    def execute(self, context: Context) -> Any:
        if self.enable_hitl_review and not isinstance(self.prompt, str):
            raise TypeError(
                f"{type(self).__name__}: enable_hitl_review=True is not supported "
                f"with a non-string prompt (got {type(self.prompt).__name__}). "
                f"The HITL session model requires a string prompt. Return a str "
                f"prompt, or disable enable_hitl_review."
            )

        # Coerced first so a bad rendered value fails before the expensive setup below.
        usage_limits = coerce_usage_limits(self.usage_limits)

        # A try that paused and then ended some other way (marked failed while waiting, a failed
        # request) leaves its transcript behind; a fresh run never reads it. ``.get``: a context
        # built by hand in a unit test has no task state store, and nothing to clean up.
        if self._supports_tool_approval() and (store := context.get("task_state_store")) is not None:
            self._delete_approval_transcript(store)

        ti = context["task_instance"]
        self._durable_storage = None
        self._durable_counter = None
        self._replay_usage = None
        # Reads the state store before the expensive setup below (_build_agent, durable
        # storage). None on < 3.3 or usage_limits=None -- see _build_usage_budget.
        self._usage_budget = self._build_usage_budget(context, usage_limits, ti=ti)
        self._run_usage = self._usage_budget.load() if self._usage_budget else RunUsage()
        # A local, non-Optional alias of `self._run_usage` for the rest of this method --
        # see `_run_agent_tracked`'s docstring for why callers pass this explicitly instead
        # of letting callees read `self._run_usage` (which mypy can't narrow past None).
        run_usage: RunUsage = self._run_usage
        self._run_usage_base = copy_run_usage(run_usage)

        if self.durable:
            from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
            from airflow.providers.common.ai.durable.step_counter import DurableStepCounter

            self._durable_storage = self._build_durable_storage(context)
            self._durable_counter = DurableStepCounter()
            # Built before _build_agent so the CachingToolset wrappers it creates share it.
            self._replay_usage = ReplayUsageLedger(run_usage=run_usage, usage_limits=usage_limits)

        agent = self._build_agent()

        self._run_identity_attrs = build_run_identity_attributes(ti)
        stamp_identity_on_agent_spans(agent, self._run_identity_attrs)

        # A per-attempt key (the task-instance id on Airflow 3, which is regenerated on
        # each retry; dag/run/task/map/try on Airflow 2) is a unique, reverse-resolvable
        # join key. It lands on result.run_id, the run's messages, and the
        # ``gen_ai.agent.call.id`` span attribute.
        run_kwargs: dict[str, Any] = {"usage_limits": usage_limits, "run_id": make_task_instance_run_key(ti)}
        history = self._resolve_message_history()
        if history is not None:
            run_kwargs["message_history"] = history

        storage = self._durable_storage
        counter = self._durable_counter
        caching_model: CachingModel | None = None
        # A killed run raises RunCancelled (see run_agent_sync), which propagates to fail the
        # task. The durable cache cleanup below is skipped on the raise, preserving it for retry.
        if self.durable and storage is not None and counter is not None:
            from pydantic_ai.models import infer_model

            from airflow.providers.common.ai.durable.caching_model import CachingModel

            if agent.model is None:
                raise ValueError("Agent model must be set when durable=True")
            resolved_model = infer_model(agent.model)
            caching_model = CachingModel(
                resolved_model, storage=storage, counter=counter, replay_usage=self._replay_usage
            )

        try:
            result, attempt_usage = self._run_and_report_on_failure(
                context, agent, run_usage, run_kwargs, caching_model
            )
        finally:
            # Also on a raise: the failed attempt is the one Airflow retries.
            if counter is not None:
                self._log_durable_summary(counter)
        return self._complete_run(context, result, attempt_usage=attempt_usage)

    def _complete_run(self, context: Context, result: Any, *, attempt_usage: RunUsage) -> Any:
        """Finish a run, or pause it when the agent is waiting on a tool call to be approved."""
        log_run_summary(self.log, result, usage=attempt_usage)
        if isinstance(result.output, DeferredToolRequests):
            self._pause_for_tool_approval(context, result, attempt_usage=attempt_usage)
        self._emit_run_metadata(context, result, usage=attempt_usage)
        if self._usage_budget and (run_usage := self._run_usage) is not None:
            self.log.info(
                "Cumulative usage across attempts: requests=%s, tool_calls=%s, input_tokens=%s, "
                "output_tokens=%s, total_tokens=%s",
                run_usage.requests,
                run_usage.tool_calls,
                run_usage.input_tokens,
                run_usage.output_tokens,
                run_usage.total_tokens,
            )
            if run_usage.cost is not None:
                self.log.info(
                    "Cumulative cost across attempts: $%s (USD, best-effort)",
                    format(run_usage.cost, "f"),
                )

        if self.message_history is not None:
            self._emit_message_history(context, result)

        output = result.output

        if self.enable_hitl_review:
            result_str = self.run_hitl_review(  # type: ignore[misc]
                context,
                output,
                message_history=result.all_messages(),
            )
            hitl_output = rehydrate_pydantic_output(
                self.output_type,
                result_str,
                serialize_output=self._serialize_model_output,
            )
            if self._usage_budget:
                self._usage_budget.clear()
            return hitl_output

        if self._serialize_model_output and isinstance(output, BaseModel):
            output = output.model_dump()

        # Clean up the durable cache only after the run and every post-run step
        # that can still fail (the run-metadata and message-history XCom pushes
        # above and output serialization) has succeeded. Cleaning up earlier and
        # then raising would leave the Airflow retry with an empty cache,
        # re-executing every already-completed model and tool step.
        if self._durable_storage is not None:
            self._durable_storage.cleanup()
        if self._usage_budget:
            self._usage_budget.clear()
        return output

    def _pause_for_tool_approval(self, context: Context, result: Any, *, attempt_usage: RunUsage) -> NoReturn:
        """
        Park the task until a human approves or rejects the tool calls the agent is waiting on.

        The transcript goes to the task state store, not the continuation kwargs: it holds tool
        results such as query rows, which do not belong on the task instance row. The
        continuation carries its hash, so the resume refuses a transcript that changed while
        the task waited, and the rendered toolset ids, so it refuses to run approved calls
        against a connection id the reviewer did not see.

        A task instance asks at most once. Airflow keeps one approval request per task
        instance, across retries and clears, and a repeat request keeps the first one's
        subject and body, so the reviewer would approve a new call while reading the old one.
        """
        requests: DeferredToolRequests = result.output
        if requests.calls:
            raise UnsupportedToolDeferralError(
                "The agent called tools that need external execution "
                f"({', '.join(call.tool_name for call in requests.calls)}); AgentOperator only "
                "supports tools that need approval."
            )
        pending_names = ", ".join(call.tool_name for call in requests.approvals)
        if not self._supports_tool_approval():
            # DeferredToolRequests in a user-set output_type reaches here where approval is off.
            raise UnsupportedToolDeferralError(
                f"The agent called tools that need approval ({pending_names}), but tool approval "
                "needs Airflow 3.3+ and is not available with durable, enable_hitl_review, "
                "a CodeMode capability or a SandboxToolset."
            )
        store = context["task_state_store"]
        if store.get(_TOOL_APPROVAL_REQUESTED_KEY):
            raise ToolApprovalAlreadyRequestedError(
                f"The agent asked for a second tool approval ({pending_names}), but this task "
                "instance already asked once in this Dag run. Airflow keeps one approval request per "
                "task instance and would show the reviewer the earlier request's details, so the task "
                "fails instead of pausing. Have the agent ask for the gated calls in one step, give "
                "each irreversible action its own task, or trigger a new Dag run."
            )
        transcript = ModelMessagesTypeAdapter.dump_json(result.all_messages()).decode()
        retention = (
            self.tool_approval_timeout + _TRANSCRIPT_RETENTION_MARGIN
            if self.tool_approval_timeout is not None
            else NEVER_EXPIRE
        )
        store.set(_TOOL_APPROVAL_TRANSCRIPT_KEY, transcript, retention=retention)

        # Tool arguments can carry credentials (an HTTP header, an MCP token); mask them first.
        pending = "\n\n".join(
            f"**{call.tool_name}**\n\n```json\n"
            f"{json.dumps(redact(call.args_as_dict()), indent=2, default=str)}\n```"
            for call in requests.approvals
        )
        upsert_hitl_detail(
            ti_id=context["task_instance"].id,
            options=[LLMApprovalMixin.APPROVE, LLMApprovalMixin.REJECT],
            subject=f"Approve tool call for task `{self.task_id}`",
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
            },
            assigned_users=self.tool_approval_assigned_users,
        )
        # Only once the request exists: a failed request must not block the retry from asking.
        store.set(_TOOL_APPROVAL_REQUESTED_KEY, True, retention=NEVER_EXPIRE)
        self.log.info("Waiting for approval of %s", pending_names)
        raise TaskAwaitingInput(
            method_name="resume_after_tool_approval",
            kwargs={
                "tool_call_ids": [call.tool_call_id for call in requests.approvals],
                "usage": _RUN_USAGE_ADAPTER.dump_python(result.usage, mode="json"),
                "attempt_usage": _RUN_USAGE_ADAPTER.dump_python(attempt_usage, mode="json"),
                "transcript_sha256": hashlib.sha256(transcript.encode()).hexdigest(),
                "toolset_ids": self._toolset_ids(),
            },
            timeout=self.tool_approval_timeout,
        )

    def _delete_approval_transcript(self, store: TaskStateStoreAccessor) -> None:
        # Best-effort: the transcript holds tool results, but failing the task over its cleanup
        # would be worse, and the row goes with the Dag run anyway.
        try:
            store.delete(_TOOL_APPROVAL_TRANSCRIPT_KEY)
        except Exception:
            self.log.warning("Could not delete the tool approval transcript", exc_info=True)

    def resume_after_tool_approval(
        self,
        context: Context,
        tool_call_ids: list[str],
        usage: dict[str, Any],
        transcript_sha256: str,
        toolset_ids: list[str],
        event: dict[str, Any],
        attempt_usage: dict[str, Any] | None = None,
    ) -> Any:
        """Continue a run paused by :meth:`_pause_for_tool_approval` with the reviewer's decision."""
        store = context["task_state_store"]
        try:
            return self._resume_after_tool_approval(
                context, store, tool_call_ids, usage, transcript_sha256, toolset_ids, event, attempt_usage
            )
        finally:
            # Whatever the outcome. A second pause fails closed before writing a new transcript.
            self._delete_approval_transcript(store)

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
        transcript = store.get(_TOOL_APPROVAL_TRANSCRIPT_KEY)
        if not isinstance(transcript, str) or (
            hashlib.sha256(transcript.encode()).hexdigest() != transcript_sha256
        ):
            raise ToolApprovalError(
                "The transcript saved when the task paused for tool approval is missing or was modified."
            )

        approval: bool | ToolDenied
        if event.get("timedout"):
            # on_tool_approval_timeout="deny": nobody refused, so do not tell the agent a person did.
            approval = ToolDenied(
                "No reviewer answered within the approval timeout, so this call was not run."
            )
            self.log.info(
                "Tool calls denied: nobody answered within tool_approval_timeout=%s",
                self.tool_approval_timeout,
            )
        elif LLMApprovalMixin.APPROVE in event["chosen_options"]:
            approval = True
            self.log.info("Tool calls approved by %s", LLMApprovalMixin._describe_responder(event))
        else:
            reason = (event.get("params_input") or {}).get("reason")
            # Only a typed reason reaches the agent; an untouched field can come back as None, "",
            # or, from some UI versions, the whole parameter spec.
            if not isinstance(reason, str) or not reason.strip():
                reason = "A reviewer denied this tool call."
            approval = ToolDenied(reason)
            self.log.info("Tool calls rejected by %s", LLMApprovalMixin._describe_responder(event))

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
        self._run_identity_attrs = build_run_identity_attributes(ti)
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
                # pydantic-ai refuses a run_id already in the history; the task-instance id stays
                # the prefix, so the resumed run still joins back to the task.
                run_id=f"{ti.id}-resumed",
            )
        except BaseException:
            self._report_failed_run(context, run_usage)
            raise
        return self._complete_run(
            context, result, attempt_usage=subtract_run_usage(run_usage, self._run_usage_base)
        )

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
        """Expose the pydantic-ai run id and token usage on XCom for downstream tasks."""
        if not self.do_xcom_push:
            return
        ti = context["task_instance"]
        ti.xcom_push(key="run_id", value=result.run_id)
        ti.xcom_push(key="usage", value=format_usage_for_xcom(usage))

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
        agent = self._build_agent()
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
        result, regen_usage = self._run_agent_tracked(
            agent, feedback, run_usage=run_usage, message_history=messages, usage_limits=usage_limits
        )
        # This regeneration's own delta, not `result.usage` -- which, when `run_usage` is
        # shared, is the seeded cumulative object, not what this call alone contributed.
        log_run_summary(self.log, result, usage=regen_usage)

        output = result.output
        if isinstance(output, BaseModel):
            output = output.model_dump_json()
        return str(output), result.all_messages()
