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
from dataclasses import replace
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.utils.logging import log_run_summary, wrap_toolsets_for_logging
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output
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
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.toolsets.abstract import AbstractToolset
    from pydantic_ai.usage import UsageLimits

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
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
        ``BaseModel`` subclass for structured output; the model instance is
        returned to XCom unchanged so downstream tasks can type-hint it
        directly. The class must be defined at module scope -- nested classes
        cannot be deserialized from XCom.
    :param toolsets: List of pydantic-ai toolsets the agent can use
        (e.g. ``SQLToolset``, ``HookToolset``).
    :param enable_tool_logging: When ``True`` (default), wraps each toolset in a
        ``LoggingToolset`` that logs tool calls with timing at INFO level and
        arguments at DEBUG level. Set to ``False`` to disable.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``).
    :param usage_limits: Optional pydantic-ai
        :class:`~pydantic_ai.usage.UsageLimits` enforced on every agent run
        (initial run, durable replay, and HITL regeneration). Pass
        ``UsageLimits(request_limit=..., total_tokens_limit=..., tool_calls_limit=..., ...)``
        to fail the task when the agent exceeds the configured token, request,
        or tool budget. ``None`` (default) means no enforcement.
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
    :param serialize_output: If ``True`` and ``output_type`` is a Pydantic
        ``BaseModel`` subclass, the model instance is dumped to a ``dict`` via
        ``model_dump()`` before being pushed to XCom. Default ``False`` --
        the Pydantic instance flows through XCom unchanged. Set to ``True``
        when a downstream consumer needs the dict shape.
    """

    deserialization_allowed_class_fields: ClassVar[tuple[str, ...]] = ("output_type",)

    template_fields: Sequence[str] = (
        "prompt",
        "llm_conn_id",
        "model_id",
        "system_prompt",
        "agent_params",
        "message_history",
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
        usage_limits: UsageLimits | None = None,
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
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.serialize_output = serialize_output
        # See LLMOperator: instance flows when the core registers ``output_type``
        # via its worker-side DAG walk; otherwise (or on opt-in) dump to a dict.
        self._serialize_model_output = serialize_output or not _CORE_WALKER
        self.toolsets = toolsets
        self.enable_tool_logging = enable_tool_logging
        self.agent_params = agent_params or {}
        self.usage_limits = usage_limits
        self.message_history = message_history

        self.durable = durable
        self.code_mode = code_mode

        # Populated per run in ``execute`` when durable=True. Declared here so
        # ``_build_agent`` -- also reached via ``regenerate_with_feedback``
        # outside ``execute`` -- can read them unconditionally.
        self._durable_storage: DurableStorageProtocol | None = None
        self._durable_counter: DurableStepCounter | None = None

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
        # pydantic-ai (and the pydantic-ai-importing CachingToolset) are imported
        # lazily to keep them out of DAG-parse-time imports, matching
        # ``_build_durable_toolsets`` and the rest of this module.
        from pydantic_ai.capabilities import Toolset
        from pydantic_ai.toolsets.abstract import AbstractToolset

        from airflow.providers.common.ai.durable.caching_toolset import CachingToolset

        rewrapped: list[Any] = []
        for capability in capabilities:
            # ``Toolset.toolset`` can be a concrete toolset or a callable factory
            # resolved per run; only a concrete toolset can be wrapped here.
            if isinstance(capability, Toolset) and isinstance(capability.toolset, AbstractToolset):
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

        self._durable_storage = None
        self._durable_counter = None

        if self.durable:
            from airflow.providers.common.ai.durable.step_counter import DurableStepCounter

            self._durable_storage = self._build_durable_storage(context)
            self._durable_counter = DurableStepCounter()

        agent = self._build_agent()

        run_kwargs: dict[str, Any] = {"usage_limits": self.usage_limits}
        history = self._resolve_message_history()
        if history is not None:
            run_kwargs["message_history"] = history

        storage = self._durable_storage
        counter = self._durable_counter
        if self.durable and storage is not None and counter is not None:
            from pydantic_ai.models import infer_model

            from airflow.providers.common.ai.durable.caching_model import CachingModel

            if agent.model is None:
                raise ValueError("Agent model must be set when durable=True")
            resolved_model = infer_model(agent.model)
            caching_model = CachingModel(resolved_model, storage=storage, counter=counter)
            with agent.override(model=caching_model):
                result = agent.run_sync(self.prompt, **run_kwargs)
        else:
            result = agent.run_sync(self.prompt, **run_kwargs)

        log_run_summary(self.log, result)

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
        # that can still fail (the message-history XCom push above and output
        # serialization) has succeeded. Cleaning up earlier and then raising
        # would leave the Airflow retry with an empty cache, re-executing every
        # already-completed model and tool step.
        if self._durable_storage is not None:
            self._durable_storage.cleanup()
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

    def regenerate_with_feedback(self, *, feedback: str, message_history: Any) -> tuple[str, Any]:
        """Re-run the agent with *feedback* appended to the conversation history."""
        agent = self._build_agent()
        messages = message_history or []
        result = agent.run_sync(feedback, message_history=messages, usage_limits=self.usage_limits)
        log_run_summary(self.log, result)

        output = result.output
        if isinstance(output, BaseModel):
            output = output.model_dump_json()
        return str(output), result.all_messages()
