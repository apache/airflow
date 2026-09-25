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
import json
from collections.abc import Iterable, Sequence
from dataclasses import replace
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel
from pydantic_ai.capabilities import Toolset
from pydantic_ai.toolsets.abstract import AbstractToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.cancellable_run import CancellableAgentRunMixin
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.observability import (
    build_run_identity_attributes,
    stamp_identity_on_agent_spans,
)
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.utils.logging import (
    format_usage_for_xcom,
    log_run_summary,
    log_run_usage,
    wrap_toolsets_for_logging,
)
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output
from airflow.providers.common.ai.utils.toolsets import find_toolset, iter_toolsets
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
)
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS

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
    from airflow.providers.common.ai.durable.caching_model import CachingModel
    from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
    from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
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
        request cap.

        On Airflow >= 3.3, when this is set, the limit counts usage across
        every attempt of the task instance combined -- the initial run, every
        retry, and every HITL regeneration all add to one running total kept
        in the AIP-103 task state store under the ``__commonai_usage__`` key
        -- rather than resetting on each attempt. This also applies to the
        implicit ``request_limit=50`` default, which can now block a retry
        that used to pass on its own. A step replayed by ``durable=True`` does
        not count toward the total (see ``durable`` below). Clearing and
        rerunning a *finished* (failed or
        succeeded) task instance gets a fresh budget automatically; clearing
        a *running* task instance does not bump ``max_tries``, so the
        restarted attempt still sees the prior spend. To reset the budget for
        a task instance that keeps retrying without a clear of a finished
        attempt, delete the ``__commonai_usage__`` key via the Task State
        Store UI. A worker killed with SIGKILL -- including after
        ``on_kill``'s grace period expires, or an OOM kill -- cannot persist
        that attempt's usage, so the next attempt's count under-represents
        actual spend by that amount. To keep
        the same effective per-attempt headroom this cross-attempt total used
        to give each attempt on its own, scale each limit by
        ``retries + 1``, or use ``usage_limits=None`` to opt back out. On
        Airflow < 3.3, and whenever ``usage_limits`` is ``None``, each attempt
        is still checked and counted on its own, as before. See
        :ref:`howto/operator:llm` for the full set of caveats, and
        :ref:`howto/operator:agent` for more on the cross-attempt budget.
    :param durable: When ``True``, enables step-level caching of model
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
        self._replay_usage: ReplayUsageLedger | None = None

        # Populated in ``execute``; also read (and, if unset, lazily initialized) by
        # ``regenerate_with_feedback`` outside ``execute``, which is why they need a
        # declared default here rather than only being set inline in ``execute``.
        self._usage_budget: TaskStateStoreUsageBudget | None = None
        self._run_usage: RunUsage | None = None
        self._run_usage_base: RunUsage | None = None

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
        candidates = list(self.toolsets or [])
        for capability in self.agent_params.get("capabilities") or ():
            if _is_concrete_toolset_capability(capability):
                candidates.append(capability.toolset)
        if find_toolset(candidates, SandboxToolset) is None:
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
            output_type=self.output_type,
            instructions=self.system_prompt,
            **extra_kwargs,
        )

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
            if _is_concrete_toolset_capability(capability):
                cached = CachingToolset(
                    wrapped=capability.toolset,
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
        the pre-run snapshot ``_run_agent_tracked`` stashes before calling the agent.
        ``self._run_usage_base`` can still be ``None`` if the raise happened before
        ``_run_agent_tracked`` was ever entered (for example, ``agent.override(...)``'s
        ``__enter__``, or a task-timeout signal landing in the narrow window before
        that method's first line runs) -- fall back to the untouched cumulative total
        rather than raising, which would replace the caller's real exception.

        Best-effort like ``_emit_run_metadata``: each push is wrapped separately so a
        failure here (e.g. a downed XCom backend) never masks the run's real exception
        -- the caller's bare ``raise`` after this call must still surface it.
        """
        base = self._run_usage_base
        try:
            attempt_usage = subtract_run_usage(run_usage, base) if base is not None else run_usage
        except Exception:
            attempt_usage = run_usage
        try:
            log_run_usage(self.log, attempt_usage, outcome="failed")
        except Exception:
            self.log.warning("Failed to log partial usage for the failed run", exc_info=True)
        if not self.do_xcom_push:
            return
        ti = context["task_instance"]
        try:
            ti.xcom_push(key="run_id", value=str(ti.id))
        except Exception:
            self.log.warning("Failed to push run_id XCom for the failed run", exc_info=True)
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

        Stashes the pre-run snapshot on ``self._run_usage_base`` so a caller that
        catches an exception raised from inside this call can still compute this
        attempt's delta via ``subtract_run_usage(run_usage, self._run_usage_base)``
        (see ``_report_failed_run``) -- this method's own return value only covers
        the success path.
        """
        base = copy_run_usage(run_usage)
        self._run_usage_base = base
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

        if self.durable:
            from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
            from airflow.providers.common.ai.durable.step_counter import DurableStepCounter

            self._durable_storage = self._build_durable_storage(context)
            self._durable_counter = DurableStepCounter()
            # Built before _build_agent so the CachingToolset wrappers it creates share it.
            self._replay_usage = ReplayUsageLedger(run_usage, usage_limits)

        agent = self._build_agent()

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

        result, attempt_usage = self._run_and_report_on_failure(
            context, agent, run_usage, run_kwargs, caching_model
        )

        log_run_summary(self.log, result, usage=attempt_usage)
        self._emit_run_metadata(context, result, usage=attempt_usage)
        if self._usage_budget:
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
                hitl_output = rehydrate_pydantic_output(
                    self.output_type,
                    result_str,
                    serialize_output=self._serialize_model_output,
                )
            else:
                try:
                    hitl_output = json.loads(result_str)
                except (ValueError, TypeError):
                    hitl_output = result_str
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
        # pydantic-ai is imported lazily here to match this module's pattern of
        # keeping pydantic-ai out of DAG-parse-time imports.
        from pydantic_ai.messages import ModelMessagesTypeAdapter

        if isinstance(raw, (str, bytes)):
            return ModelMessagesTypeAdapter.validate_json(raw)
        return ModelMessagesTypeAdapter.validate_python(raw)

    def _emit_message_history(self, context: Context, result: Any) -> None:
        """Push the full post-run transcript to XCom for the next turn to resume."""
        # Lazy import: see _resolve_message_history.
        from pydantic_ai.messages import ModelMessagesTypeAdapter

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
