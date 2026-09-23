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
from collections.abc import Iterable, Sequence
from dataclasses import replace
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn

from pydantic import BaseModel, TypeAdapter
from pydantic_ai import DeferredToolRequests, DeferredToolResults, ToolDenied
from pydantic_ai.capabilities import Toolset
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
    stamp_identity_on_agent_spans,
)
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.utils.logging import log_run_summary, wrap_toolsets_for_logging
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output
from airflow.providers.common.ai.utils.toolsets import find_toolset, iter_toolsets
from airflow.providers.common.ai.utils.usage import coerce_usage_limits
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
    # Per-tool approval parks the task in AWAITING_INPUT, which older cores do not have.
    from airflow.sdk.exceptions import TaskAwaitingInput
    from airflow.sdk.execution_time.context import NEVER_EXPIRE
    from airflow.sdk.execution_time.hitl import upsert_hitl_detail

try:
    # See LLMOperator: new enough cores register declared ``output_type`` classes
    # from a worker-side DAG walk, so the model instance flows through XCom; older
    # cores dump to a dict instead.
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:  # pragma: no cover - cores before the worker-side registration walk
    _CORE_WALKER = False

if TYPE_CHECKING:
    import jinja2
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.usage import UsageLimits

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
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


def _is_concrete_toolset_capability(capability: Any) -> bool:
    """Whether *capability* is a ``Toolset`` holding a toolset, not a callable factory resolved per run."""
    return isinstance(capability, Toolset) and isinstance(capability.toolset, AbstractToolset)


def _declares_agent_template_fields(toolset: Any) -> bool:
    """Whether *toolset*, or a toolset it wraps or combines, has connection IDs to render."""
    return isinstance(toolset, AbstractToolset) and any(
        getattr(leaf, "agent_template_fields", None) for leaf in iter_toolsets(toolset)
    )


def _build_code_mode() -> Any:
    """
    Return a pydantic-ai-harness ``CodeMode`` capability, or raise if not installed.

    Kept here (not a module-level import) because ``pydantic-ai-harness`` is an
    optional dependency behind the ``code-mode`` extra; importing it eagerly
    would break installs that don't enable the extra.
    """
    try:
        from pydantic_ai_harness import CodeMode
    except ImportError as e:
        # Only report "extra not installed" when pydantic-ai-harness itself is
        # missing. A failure deeper in its import chain (a broken or missing
        # transitive dependency) is a different problem -- re-raise it as-is so
        # the real error isn't masked by a misleading "install the extra" message.
        missing = e.name or ""
        if missing == "pydantic_ai_harness" or missing.startswith("pydantic_ai_harness."):
            raise AirflowOptionalProviderFeatureException(
                "code_mode=True requires the 'code-mode' extra. Install it with "
                '`pip install "apache-airflow-providers-common-ai[code-mode]"`.'
            ) from e
        raise
    return CodeMode()


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
    task can reference the run and its cost. The ``run_id`` also ties the task to
    its GenAI trace (see the provider's observability docs). With
    ``enable_hitl_review``, these reflect the initial model run, not the
    human-feedback regenerations.

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
        a mapped ``@task.agent``. Each task instance renders its own copy and logs
        the rendered toolset id; the toolset object in the Dag file is not
        modified. Derive the connection ID from values the Dag controls rather than
        ``params`` or ``dag_run.conf``, which whoever triggers the Dag controls.
    :param enable_tool_logging: When ``True`` (default), wraps each toolset in a
        ``LoggingToolset`` that logs tool calls with timing at INFO level and
        arguments at DEBUG level. Set to ``False`` to disable.
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
        is not templated or validated. ``None`` (default) means no
        enforcement.

        A dict that omits ``request_limit`` still gets pydantic-ai's default of
        ``50`` requests -- pass ``"request_limit": None`` explicitly for no
        request cap. See :ref:`howto/operator:llm` for the full set of caveats,
        and :ref:`howto/operator:agent` for the ``durable=True`` replay
        double-counting warning.
    :param durable: When ``True``, enables step-level caching of model
        responses and tool results for durable execution.  On retry, cached
        steps are replayed instead of re-executing.  Each cached step is
        verified against the current request before replay: if the prompt,
        model, settings, tools, or message history changed since the failed
        attempt, the affected steps re-run live (with a warning) instead of
        replaying stale results.  Default ``False``.
        On Airflow >= 3.3 the cache is kept in the AIP-103 task state store, so
        no extra configuration is needed. On older cores it is persisted to
        ObjectStorage and requires ``[common.ai] durable_cache_path`` to be set.
        Tools are durably cached when provided via ``toolsets=`` or via a
        concrete pydantic-ai ``Toolset`` capability. Tools reaching the agent
        through any *other* capability -- ``MCP``, ``PrefixTools``,
        ``CombinedCapability``, a ``Toolset`` backed by a callable factory, or
        capabilities loaded from a ``spec_file`` -- are not cached and re-run on
        retry; put tools you need replayed in ``toolsets=``. Provider-native
        capabilities such as ``WebSearch`` and ``Thinking`` execute inside the
        model call and are covered by model-response caching.
        Cannot be combined with a ``SandboxToolset`` (raises): a sandbox is
        destroyed when the run ends, so replayed tool results would describe
        files that no longer exist.
    :param code_mode: When ``True``, wraps the agent's tools in a single
        ``run_code`` tool powered by the Monty sandbox (pydantic-ai-harness
        ``CodeMode``). Instead of one model round-trip per tool call, the model
        writes Python that calls the tools as functions, with loops and
        ``asyncio.gather``, in one turn. The generated code runs in Monty's
        deny-by-default sandbox; the tools it calls still run in the worker, so
        ``code_mode`` does not widen what the tools can reach -- it only changes
        how the model invokes them. Requires the ``code-mode`` extra
        (``pip install "apache-airflow-providers-common-ai[code-mode]"``).
        Cannot be combined with ``durable=True`` (durable replay assumes a
        stable per-step call order that code mode does not guarantee).
        Default ``False``.
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
        ``SandboxToolset`` (raises): regeneration after feedback is a second
        run, which starts from an empty sandbox while its history describes
        the first run's files.
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

    **Per-tool approval** (Airflow 3.3+):

    Mark the tools a human must approve with pydantic-ai's own API --
    ``toolset.approval_required(...)``, or ``requires_approval=True`` on a function
    tool -- and the task pauses before running them. The pending calls, with their
    arguments, appear on the **Required Actions** page; the task waits in the
    ``awaiting_input`` state without holding a worker slot. On **Approve** the calls
    run and the agent carries on. On **Reject** the agent is told the call was denied
    (with the reviewer's reason, when given) and carries on without it. A task
    instance asks at most once per Dag run, across retries and clears; a second
    request fails the task. ``usage_limits`` applies to both sides of the pause.
    Not available together with ``durable``, ``enable_hitl_review``, ``code_mode``,
    or a ``SandboxToolset``; there, a tool that requires approval fails the task as
    before.

    :param tool_approval_timeout: How long the pause waits for a decision.
        ``None`` (default) waits indefinitely. Must be positive.
    :param on_tool_approval_timeout: What a timed-out pause does: ``"fail"``
        (default) fails the task, ``"deny"`` rejects the pending calls so the agent
        carries on without them, and needs a ``tool_approval_timeout``. There is no
        approve-on-timeout.
    :param tool_approval_assigned_users: Users allowed to decide. ``None`` (default)
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

    operator_extra_links = (HITLReviewLink(),)

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
        enable_tool_logging: bool = True,
        agent_params: dict[str, Any] | None = None,
        usage_limits: UsageLimits | dict[str, Any] | None = None,
        durable: bool = False,
        code_mode: bool = False,
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
        # See LLMOperator: instance flows when the core registers ``output_type``
        # via its worker-side DAG walk; otherwise (or on opt-in) dump to a dict.
        self._serialize_model_output = serialize_output or not _CORE_WALKER
        self.toolsets = toolsets
        self.enable_tool_logging = enable_tool_logging
        self.agent_params = agent_params or {}
        # No validation here -- see coerce_usage_limits() docstring for why.
        self.usage_limits = usage_limits
        self.message_history = message_history

        self.durable = durable
        self.code_mode = code_mode

        # Populated per run in ``execute`` when durable=True. Declared here so
        # ``_build_agent`` -- also reached via ``regenerate_with_feedback``
        # outside ``execute`` -- can read them unconditionally.
        self._durable_storage: DurableStorageProtocol | None = None
        self._durable_counter: DurableStepCounter | None = None

        # Checked ahead of the combination rules below. On a core older than 3.1 the core
        # version is the real blocker, and reporting a combination error first would send the
        # user to drop an argument that was never the problem -- they would hit this anyway.
        if enable_hitl_review and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "Human in the loop functionality needs Airflow 3.1+."
            )

        if durable and enable_hitl_review:
            raise ValueError("durable=True and enable_hitl_review=True cannot be used together.")

        if durable and code_mode:
            # Durable replay caches individual model/tool steps via CachingModel /
            # CachingToolset and a shared step counter that assumes a stable call
            # order across runs. Code mode collapses tools into one ``run_code``
            # tool and lets the model emit arbitrary Python, so step counts and
            # ordering can differ between the original run and a retry, breaking
            # replay. Reject the combination rather than silently mis-replaying.
            raise ValueError("durable=True and code_mode=True cannot be used together.")

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

        The toolset is looked for inside wrappers and combinations (``.prefixed()``,
        ``.filtered()``, several toolsets passed together) and inside ``Toolset``
        capabilities, since those are the compositions the documentation recommends.
        A toolset resolved per run from a callable cannot be inspected here.
        """
        if find_toolset(self._declared_toolsets(), SandboxToolset) is None:
            return
        flag = "durable=True" if durable else "enable_hitl_review=True"
        why = (
            "cached tool results would be replayed against a sandbox that no longer exists"
            if durable
            else "a regenerated run would start from an empty sandbox while its history describes "
            "files from the first run"
        )
        raise ValueError(
            f"{flag} cannot be used with a SandboxToolset: {why}. "
            f"Drop {flag}, or move the sandbox work into its own task."
        )

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
        Render the connection IDs of toolsets that declare ``agent_template_fields``.

        ``toolsets`` is not itself a template field: serializing it would put each
        toolset's repr -- which for pydantic-ai's dataclass toolsets embeds function
        addresses -- into the Dag hash and the rendered-fields view. Instead, each leaf
        toolset that opts in (``SQLToolset``, ``MCPToolset``, ``HookToolset``) is
        rendered here, found with pydantic-ai's ``visit_and_replace`` inside
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

        if self.toolsets:
            self.toolsets = render_all(self.toolsets)
        agent_params = dict(self.agent_params)
        if agent_params.get("toolsets"):
            agent_params["toolsets"] = render_all(agent_params["toolsets"])
        if agent_params.get("capabilities"):
            agent_params["capabilities"] = [
                replace(capability, toolset=capability.toolset.visit_and_replace(render))
                if _is_concrete_toolset_capability(capability)
                and _declares_agent_template_fields(capability.toolset)
                else capability
                for capability in agent_params["capabilities"]
            ]
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
        storage = self._durable_storage
        counter = self._durable_counter
        if self.toolsets:
            toolsets = self.toolsets
            if self.durable and storage is not None and counter is not None:
                toolsets = self._build_durable_toolsets(toolsets, storage, counter)
            if self.enable_tool_logging:
                toolsets = wrap_toolsets_for_logging(toolsets, self.log)
            extra_kwargs["toolsets"] = toolsets
        capabilities = list(extra_kwargs.get("capabilities") or [])
        if self.durable and storage is not None and counter is not None:
            # Tools supplied through a ``Toolset`` capability bypass the
            # ``toolsets=`` wrapping above, so their results would re-execute on
            # every retry instead of replaying; wrap their inner toolset too.
            capabilities = self._build_durable_capabilities(capabilities, storage, counter)
        if self.code_mode:
            capabilities.append(_build_code_mode())
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
        sandbox is destroyed when the run ends, so its files would be gone on resume.
        """
        if not AIRFLOW_V_3_3_PLUS or self.durable or self.enable_hitl_review or self.code_mode:
            return False
        return find_toolset(self._declared_toolsets(), SandboxToolset) is None

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
        for capability in self.agent_params.get("capabilities") or ():
            if _is_concrete_toolset_capability(capability):
                candidates.append(capability.toolset)
        return candidates

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

        return [CachingToolset(wrapped=ts, storage=storage, counter=counter) for ts in toolsets]

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
            if _is_concrete_toolset_capability(capability):
                cached = CachingToolset(wrapped=capability.toolset, storage=storage, counter=counter)
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

    def _build_durable_storage(self, context: Context) -> DurableStorageProtocol:
        """
        Return the durable storage backend for the current task instance.

        On Airflow >= 3.3 durable steps are cached in the AIP-103 task state
        store, which handles persistence and large-value offload natively, so no
        ``[common.ai] durable_cache_path`` is required. On older cores, fall back
        to the ObjectStorage backend configured via ``durable_cache_path``.
        """
        if AIRFLOW_V_3_3_PLUS:
            # Imported lazily: NEVER_EXPIRE and the task state store accessor do
            # not exist on cores before 3.3.
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

        self._durable_storage = None
        self._durable_counter = None

        if self.durable:
            from airflow.providers.common.ai.durable.step_counter import DurableStepCounter

            self._durable_storage = self._build_durable_storage(context)
            self._durable_counter = DurableStepCounter()

        agent = self._build_agent()

        ti = context["task_instance"]
        self._run_identity_attrs = build_run_identity_attributes(ti)
        stamp_identity_on_agent_spans(agent, self._run_identity_attrs)

        # The task-instance id is non-nullable and regenerated on each retry, so it
        # is a unique, reverse-resolvable join key. It lands on result.run_id, the
        # run's messages, and the ``gen_ai.agent.call.id`` span attribute.
        run_kwargs: dict[str, Any] = {"usage_limits": usage_limits, "run_id": str(ti.id)}
        history = self._resolve_message_history()
        if history is not None:
            run_kwargs["message_history"] = history

        storage = self._durable_storage
        counter = self._durable_counter
        # A killed run raises RunCancelled (see run_agent_sync), which propagates to fail the
        # task. The durable cache cleanup below is skipped on the raise, preserving it for retry.
        if self.durable and storage is not None and counter is not None:
            from pydantic_ai.models import infer_model

            from airflow.providers.common.ai.durable.caching_model import CachingModel

            if agent.model is None:
                raise ValueError("Agent model must be set when durable=True")
            resolved_model = infer_model(agent.model)
            caching_model = CachingModel(resolved_model, storage=storage, counter=counter)
            with agent.override(model=caching_model):
                result = self.run_agent_sync(agent, self.prompt, **run_kwargs)
        else:
            result = self.run_agent_sync(agent, self.prompt, **run_kwargs)

        return self._complete_run(context, result)

    def _complete_run(self, context: Context, result: Any) -> Any:
        """Finish a run, or pause it when the agent is waiting on a tool call to be approved."""
        log_run_summary(self.log, result)
        if isinstance(result.output, DeferredToolRequests):
            self._pause_for_tool_approval(context, result)
        self._emit_run_metadata(context, result)

        if self._durable_counter is not None:
            c = self._durable_counter
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

        if self.message_history is not None:
            self._emit_message_history(context, result)

        output = result.output

        if self.enable_hitl_review:
            result_str = self.run_hitl_review(  # type: ignore[misc]
                context,
                output,
                message_history=result.all_messages(),
            )
            if isinstance(self.output_type, type) and issubclass(self.output_type, BaseModel):
                return rehydrate_pydantic_output(
                    self.output_type,
                    result_str,
                    serialize_output=self._serialize_model_output,
                )
            try:
                return json.loads(result_str)
            except (ValueError, TypeError):
                return result_str

        if self._serialize_model_output and isinstance(output, BaseModel):
            output = output.model_dump()

        # Clean up the durable cache only after the run and every post-run step
        # that can still fail (the run-metadata and message-history XCom pushes
        # above and output serialization) has succeeded. Cleaning up earlier and
        # then raising would leave the Airflow retry with an empty cache,
        # re-executing every already-completed model and tool step.
        if self._durable_storage is not None:
            self._durable_storage.cleanup()
        return output

    def _pause_for_tool_approval(self, context: Context, result: Any) -> NoReturn:
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
                "code_mode or a SandboxToolset."
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
    ) -> Any:
        """Continue a run paused by :meth:`_pause_for_tool_approval` with the reviewer's decision."""
        store = context["task_state_store"]
        try:
            return self._resume_after_tool_approval(
                context, store, tool_call_ids, usage, transcript_sha256, toolset_ids, event
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

        agent = self._build_agent()
        ti = context["task_instance"]
        self._run_identity_attrs = build_run_identity_attributes(ti)
        stamp_identity_on_agent_spans(agent, self._run_identity_attrs)
        # The full transcript, not a trimmed one: pydantic-ai reads its last request to skip
        # the calls that already ran in the paused step. No new prompt: it would land after
        # the tool results as a second user turn.
        result = self.run_agent_sync(
            agent,
            None,
            message_history=ModelMessagesTypeAdapter.validate_json(transcript),
            deferred_tool_results=DeferredToolResults(
                approvals={tool_call_id: approval for tool_call_id in tool_call_ids}
            ),
            usage=_RUN_USAGE_ADAPTER.validate_python(usage),
            usage_limits=coerce_usage_limits(self.usage_limits),
            # pydantic-ai refuses a run_id already in the history; the task-instance id stays
            # the prefix, so the resumed run still joins back to the task.
            run_id=f"{ti.id}-resumed",
        )
        return self._complete_run(context, result)

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

    def _emit_run_metadata(self, context: Context, result: Any) -> None:
        """Expose the pydantic-ai run id and token usage on XCom for downstream tasks."""
        if not self.do_xcom_push:
            return
        usage = result.usage
        ti = context["task_instance"]
        ti.xcom_push(key="run_id", value=result.run_id)
        ti.xcom_push(
            key="usage",
            value={
                "requests": usage.requests,
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "total_tokens": usage.total_tokens,
                "tool_calls": usage.tool_calls,
                # Decimal | None, stringified so XCom serialization stays lossless.
                "cost": str(usage.cost) if usage.cost is not None else None,
            },
        )

    def regenerate_with_feedback(self, *, feedback: str, message_history: Any) -> tuple[str, Any]:
        """Re-run the agent with *feedback* appended to the conversation history."""
        usage_limits = coerce_usage_limits(self.usage_limits)
        agent = self._build_agent()
        identity = getattr(self, "_run_identity_attrs", None)
        if identity:
            stamp_identity_on_agent_spans(agent, identity)
        messages = message_history or []
        result = self.run_agent_sync(
            agent,
            feedback,
            message_history=messages,
            usage_limits=usage_limits,
        )
        log_run_summary(self.log, result)

        output = result.output
        if isinstance(output, BaseModel):
            output = output.model_dump_json()
        return str(output), result.all_messages()
