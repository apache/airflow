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
"""Shared contract for agent-framework hooks used by :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`."""

from __future__ import annotations

import functools
import inspect
import json
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar, cast

from typing_extensions import get_type_hints

from airflow.providers.common.compat.sdk import BaseHook

_EMPTY_OBJECT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}

# Attribute name for durable storage/counter bound to a framework agent instance.
_AIRFLOW_DURABLE_ATTR = "_airflow_durable_state"


@dataclass
class AgentUsage:
    """Token and request usage from an agent run, when the backend exposes it."""

    requests: int = 0
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


@dataclass
class DurableStats:
    """Step-level cache statistics from a durable agent run."""

    replayed_model: int = 0
    replayed_tool: int = 0
    cached_model: int = 0
    cached_tool: int = 0


@dataclass
class AgentRunResult:
    """
    Backend-neutral result from :meth:`BaseAIHook.run_agent`.

    :param output: Final agent output (``str``, Pydantic model instance, etc.).
    :param message_history: Opaque conversation state for HITL regeneration; only pass back to the
        same hook implementation that produced it.
    :param model_name: Resolved model identifier, when available.
    :param usage: Usage counters when the backend exposes them.
    :param tool_names: Ordered tool names invoked during the run, when known.
    :param durable_stats: Durable step-cache statistics, populated when durable execution is enabled.
    """

    output: Any
    message_history: Any = None
    model_name: str | None = None
    usage: AgentUsage | None = None
    tool_names: list[str] | None = None
    durable_stats: DurableStats | None = None


@dataclass
class ToolSpec:
    """
    Framework-neutral tool descriptor.

    Toolsets produce :class:`ToolSpec` objects; each hook converts them to its
    native tool representation via :meth:`BaseAIHook._tool_spec_to_native`.

    :param name: Tool name exposed to the LLM.
    :param description: Human-readable description used by the LLM to decide when to call this tool.
    :param parameters: JSON Schema ``object`` describing the tool's parameters.
    :param fn: Callable that implements the tool. Must accept keyword arguments matching *parameters*.
    :param sequential: When ``True``, the backend must not invoke this tool concurrently with others
        in the same turn (for example when tools share a non-thread-safe connection).
    """

    name: str
    description: str
    parameters: dict[str, Any]
    fn: Callable[..., Any]
    sequential: bool = False


@dataclass
class DurableContext:
    """Framework-neutral identity of the running task, used to locate the durable cache file."""

    dag_id: str
    task_id: str
    run_id: str
    map_index: int = -1


@dataclass
class AgentRunRequest:
    """
    Parameter object passed to :meth:`BaseAIHook.create_agent` and :meth:`BaseAIHook.run_agent`.

    Encapsulates everything the hook needs to build and run an agent in a single
    framework-neutral structure, so that :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`
    has zero framework-specific imports.

    :param prompt: User prompt for this invocation (plain ``str`` or a multimodal
        ``Sequence`` accepted by the backend agent's run API).
    :param output_type: Expected structured output type (default: ``str``).
    :param instructions: System-level instructions for the agent.
    :param toolsets: List of :class:`BaseToolset` instances the agent may call.
    :param usage_limits: Backend-specific usage limits; ignored if the hook does not support them.
    :param message_history: Prior conversation state from a previous :class:`AgentRunResult`.
    :param enable_tool_logging: When ``True`` (default), wraps Airflow-resolved tool callables with
        a logging shim. Backend-native tool objects may be passed through unchanged by the concrete
        hook and might not receive this wrapper.
    :param durable_context: When set, enables step-level durable caching for the run.
    :param agent_params: Extra keyword arguments forwarded to the underlying agent constructor.
        Use this escape hatch for framework-specific options.
    """

    prompt: str | Sequence[Any]
    output_type: type[Any] = str
    instructions: str = ""
    toolsets: list[Any] | None = None
    usage_limits: Any = None
    message_history: Any = None
    enable_tool_logging: bool = True
    durable_context: DurableContext | None = None
    agent_params: dict[str, Any] = field(default_factory=dict)


class BaseToolset(metaclass=ABCMeta):
    """
    Abstract base for framework-agnostic toolsets.

    Subclasses implement :meth:`as_tools` to return a list of :class:`ToolSpec`
    objects.  Each hook converts those specs to its native tool representation
    via :meth:`BaseAIHook._tool_spec_to_native`.
    """

    @abstractmethod
    def as_tools(self) -> list[ToolSpec]:
        """Return the list of tools this toolset exposes."""


class BaseAIHook(BaseHook, metaclass=ABCMeta):
    """
    Abstract hook for multi-turn LLM agents.

    :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` resolves the concrete hook
    from the Airflow connection ``conn_type`` (for example ``pydanticai`` or ``pydanticai-bedrock``).

    :param llm_conn_id: Optional connection ID override (subclasses may apply a default).
    :param model_id: Optional model override; not all backends use this parameter.

    Subclasses implement :meth:`get_model`, :meth:`create_agent`, :meth:`run_agent`, and
    :meth:`_tool_spec_to_native`.

    Shared helpers :meth:`_init_durable`, :meth:`_resolve_tools`, :meth:`_logged_callable`, and
    :meth:`_cached_callable` are provided for all hooks.
    """

    conn_name_attr = "llm_conn_id"

    supports_toolsets: ClassVar[bool] = False
    supports_durable: ClassVar[bool] = False
    supports_usage_limits: ClassVar[bool] = False

    def __init__(
        self,
        llm_conn_id: str | None = None,
        model_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id

    @classmethod
    def get_agent_hook(cls, conn_id: str, *, hook_params: dict[str, Any] | None = None) -> BaseAIHook:
        """
        Return an agent hook for *conn_id*, verifying it implements this contract.

        Uses the connection's ``conn_type`` to select the hook class registered in
        ``provider.yaml``.
        """
        hook = cls.get_hook(conn_id, hook_params=hook_params)
        if not isinstance(hook, BaseAIHook):
            raise TypeError(
                f"Connection {conn_id!r} resolved to {type(hook).__name__}, which is not a BaseAIHook. "
                "Use a connection type registered for agent frameworks (e.g. pydanticai, pydanticai-bedrock)."
            )
        return hook

    @abstractmethod
    def get_model(self) -> Any:
        """Return the backend model/client used to construct agents."""

    def get_conn(self) -> Any:
        """Return the backend model/client. Delegates to :meth:`get_model`."""
        return self.get_model()

    @abstractmethod
    def create_agent(self, request: AgentRunRequest) -> Any:
        """
        Build (but do not run) the agent described by *request*.

        Responsible for resolving :attr:`AgentRunRequest.toolsets` via
        :meth:`_resolve_tools` and constructing the framework-native agent object
        with the model, tools, instructions, and output type from *request*.

        When :attr:`AgentRunRequest.durable_context` is set, implementations
        should call :meth:`_init_durable` and bind the returned storage/counter
        to the agent via :meth:`_bind_agent_durable` so that :meth:`run_agent`
        can retrieve and clean them up.

        Implementations must call :meth:`validate_run_request` at the start of
        this method before any agent construction or durable initialisation.

        :param request: All parameters needed to configure the agent.
        :returns: Framework-native agent object, ready to be passed to :meth:`run_agent`.
        """

    @abstractmethod
    def run_agent(self, agent: Any, request: AgentRunRequest) -> AgentRunResult:
        """
        Execute *agent* for *request* and return a normalized :class:`AgentRunResult`.

        Implementations with durable execution should pop durable state via
        :meth:`_pop_agent_durable`, apply it during the run, and call
        ``storage.cleanup()`` only after a successful run (keep the cache file
        when the run raises so Airflow retries can replay cached steps).

        :param agent: Framework-native agent produced by :meth:`create_agent`.
        :param request: The same request used to create the agent (prompt, usage
            limits, message history, etc.).
        """

    @abstractmethod
    def _tool_spec_to_native(self, spec: ToolSpec) -> Any:
        """
        Convert a :class:`ToolSpec` to the agent framework's native tool representation.

        Called once per tool inside :meth:`_resolve_tools`. The returned object
        is collected into a list and passed to the underlying agent constructor.

        :param spec: Universal tool descriptor, with the callable already wrapped
            by any enabled logging / caching shims.
        """

    def validate_run_request(self, request: AgentRunRequest) -> None:
        """
        Raise if *request* uses features this hook implementation does not support.

        Hook implementations call this at the start of :meth:`create_agent`.
        """
        hook_name = type(self).__name__
        conn_id = self.llm_conn_id or "unknown"
        if request.toolsets and not self.supports_toolsets:
            raise ValueError(
                f"toolsets are not supported for connection {conn_id!r} (conn_type resolves to {hook_name}). "
            )
        if request.usage_limits is not None and not self.supports_usage_limits:
            raise ValueError(
                f"usage_limits are not supported for connection {conn_id!r} "
                f"(conn_type resolves to {hook_name})."
            )
        if request.durable_context is not None and not self.supports_durable:
            raise ValueError(
                f"durable execution requires a hook that supports durable caching; "
                f"got {hook_name} for connection {conn_id!r}."
            )

    def _init_durable(self, ctx: DurableContext) -> tuple[Any, Any]:
        """
        Create and return a ``DurableStorage`` / ``DurableStepCounter`` pair for *ctx*.

        Hooks call this inside :meth:`create_agent` when
        :attr:`AgentRunRequest.durable_context` is set.
        """
        from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
        from airflow.providers.common.ai.durable.storage import DurableStorage

        storage = DurableStorage(
            dag_id=ctx.dag_id,
            task_id=ctx.task_id,
            run_id=ctx.run_id,
            map_index=ctx.map_index,
        )
        counter = DurableStepCounter()
        return storage, counter

    @staticmethod
    def _bind_agent_durable(agent: Any, storage: Any, counter: Any) -> None:
        """Associate *storage* and *counter* with *agent* until :meth:`run_agent` completes."""
        setattr(agent, _AIRFLOW_DURABLE_ATTR, (storage, counter))

    @staticmethod
    def _pop_agent_durable(agent: Any) -> tuple[Any, Any] | None:
        """Remove and return durable state bound to *agent*, if any."""
        state = getattr(agent, _AIRFLOW_DURABLE_ATTR, None)
        if state is None:
            return None
        delattr(agent, _AIRFLOW_DURABLE_ATTR)
        return state

    def _resolve_tools(
        self,
        toolsets: list[Any],
        enable_logging: bool,
        storage: Any,
        counter: Any,
    ) -> list[Any]:
        """
        Convert a mixed list of toolsets / callables / native tools into framework-native tools.

        Three cases per item:

        * :class:`BaseToolset` — calls ``as_tools()`` and processes each :class:`ToolSpec`.
        * Any callable (plain function, bound method, :func:`functools.partial`, or callable
          object) — auto-wraps into a :class:`ToolSpec` using ``__name__`` and ``__doc__``
          (with sensible fallbacks for partials and callable objects), then processes it the
          same way.
        * Anything else — passed through unchanged (assumed to be a native tool object already
          constructed for the target framework).

        The processing pipeline for ``BaseToolset`` and callable items:
        *fn* → optional cache wrap → optional log wrap → :meth:`_tool_spec_to_native`.

        :param toolsets: Mix of :class:`BaseToolset` instances, callables (functions, bound
            methods, :func:`functools.partial`, or callable objects), and native tool objects.
        :param enable_logging: When ``True``, wrap each callable with :meth:`_logged_callable`.
        :param storage: ``DurableStorage`` instance, or ``None`` to skip caching.
        :param counter: ``DurableStepCounter`` instance, or ``None`` to skip caching.
        """
        native: list[Any] = []
        for ts in toolsets:
            if isinstance(ts, BaseToolset):
                specs = ts.as_tools()
            elif callable(ts):
                if isinstance(ts, functools.partial):
                    name = getattr(ts.func, "__name__", type(ts.func).__name__)
                    doc = ts.func.__doc__ or ""
                else:
                    name = getattr(ts, "__name__", type(ts).__name__)
                    doc = ts.__doc__ or ""
                specs = [
                    ToolSpec(
                        name=name,
                        description=doc,
                        parameters=_EMPTY_OBJECT_SCHEMA,
                        fn=ts,
                    )
                ]
            else:
                native.append(ts)
                continue
            for spec in specs:
                fn = spec.fn
                if storage is not None and counter is not None:
                    fn = self._cached_callable(fn, storage, counter)
                if enable_logging:
                    fn = self._logged_callable(fn, self.log, name=spec.name)
                adapted = ToolSpec(
                    name=spec.name,
                    description=spec.description,
                    parameters=spec.parameters,
                    fn=fn,
                    sequential=spec.sequential,
                )
                native.append(self._tool_spec_to_native(adapted))
        return native

    @staticmethod
    def _logged_callable(
        fn: Callable[..., Any],
        logger: Any,
        *,
        name: str | None = None,
    ) -> Callable[..., Any]:
        """Wrap *fn* to log tool name, args, timing, and exceptions."""
        _tool_name = name or getattr(fn, "__name__", type(fn).__name__)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            logger.info("::group::Tool call: %s", _tool_name)
            if kwargs:
                logger.debug("Tool args: %s", json.dumps(kwargs, default=str))
            start = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                elapsed = time.monotonic() - start
                logger.info("Tool %s returned in %.2fs", _tool_name, elapsed)
                logger.info("::endgroup::")
                return result
            except Exception:
                elapsed = time.monotonic() - start
                logger.exception("Tool %s failed after %.2fs", _tool_name, elapsed)
                logger.info("::endgroup::")
                raise

        BaseAIHook._copy_wrapper_introspection_metadata(fn, wrapper)
        return wrapper

    @staticmethod
    def _cached_callable(
        fn: Callable[..., Any],
        storage: Any,
        counter: Any,
    ) -> Callable[..., Any]:
        """Wrap *fn* to cache its result in *storage* using a monotonic step counter."""

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            step = counter.next_step()
            key = f"tool_step_{step}"
            found, cached = storage.load_tool_result(key)
            if found:
                counter.replayed_tool += 1
                return cached
            result = fn(*args, **kwargs)
            storage.save_tool_result(key, result)
            counter.cached_tool += 1
            return result

        BaseAIHook._copy_wrapper_introspection_metadata(fn, wrapper)
        return wrapper

    @staticmethod
    def _copy_wrapper_introspection_metadata(
        fn: Callable[..., Any],
        wrapper: Callable[..., Any],
    ) -> None:
        """
        Keep *wrapper* introspection aligned with *fn*.

        Generic logging/caching wrappers use ``*args`` and ``**kwargs``, which can
        lose or mismatch the original callable's signature and annotations.  This
        helper copies consistent metadata onto the wrapper so schema/introspection
        code sees the same callable shape as the original tool.
        """
        signature_source, annotation_source = BaseAIHook._get_wrapper_metadata_sources(fn)
        try:
            signature = inspect.signature(signature_source)
        except (ValueError, TypeError):
            return
        setattr(wrapper, "__signature__", signature)
        wrapper.__module__ = cast(
            "str",
            getattr(annotation_source, "__module__", None) or getattr(fn, "__module__", __name__),
        )

        try:
            hints = get_type_hints(annotation_source, include_extras=True)
        except (NameError, TypeError):
            wrapper.__annotations__ = getattr(annotation_source, "__annotations__", {}).copy()
            return

        resolved = {name: hints[name] for name in signature.parameters if name in hints}
        if "return" in hints:
            resolved["return"] = hints["return"]
        wrapper.__annotations__ = resolved

    @staticmethod
    def _get_wrapper_metadata_sources(
        fn: Callable[..., Any],
    ) -> tuple[Callable[..., Any], Callable[..., Any]]:
        """
        Return the best signature and annotation sources for *fn*.

        Most callables can use the same object for both. Partials need the bound
        signature from the partial itself but annotations from the underlying
        function (unwrapped through any nesting), and callable objects expose
        their useful metadata on ``obj.__call__``.
        """
        if isinstance(fn, functools.partial):
            inner: Callable[..., Any] = fn.func
            while isinstance(inner, functools.partial):
                inner = inner.func
            return fn, inner
        if inspect.ismethod(fn) or inspect.isfunction(fn):
            return fn, fn

        call = cast("Callable[..., Any]", object.__getattribute__(fn, "__call__"))
        return call, call
