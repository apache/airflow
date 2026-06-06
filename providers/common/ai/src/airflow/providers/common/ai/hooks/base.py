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
import json
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Generic, TypeVar

from airflow.providers.common.ai.utils.callables import is_async_callable
from airflow.providers.common.ai.utils.function_schema import callable_to_tool_spec
from airflow.providers.common.compat.sdk import BaseHook

AgentT = TypeVar("AgentT")


class Capability(str, Enum):
    """
    Capability tokens declared by concrete hook classes.

    A hook advertises its support by including the relevant tokens in its
    :attr:`BaseAIHook.capabilities` frozenset.
    :meth:`BaseAIHook.validate_run_request` rejects requests that use a
    feature whose token is absent.
    """

    TOOLSETS = "toolsets"
    USAGE_LIMITS = "usage_limits"
    DURABLE = "durable"


@dataclass
class AgentUsage:
    """Token and request usage from an agent run, when the backend exposes it."""

    requests: int | None = None
    tool_calls: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


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
    has zero framework-specific imports. This contract is currently validated by
    the pydantic-ai hook family and may evolve as more framework backends are added.

    :param prompt: User prompt for this invocation (plain ``str`` or a multimodal
        ``Sequence`` accepted by the backend agent's run API).
    :param output_type: Expected structured output type (default: ``str``).
    :param instructions: System-level instructions for the agent.
    :param toolsets: List of tools/toolsets the agent may call (BaseToolset instances, plain callables, or backend-native tool objects).
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
    output_type: type[Any] | None = str
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


class BaseAIHook(BaseHook, Generic[AgentT], metaclass=ABCMeta):
    """
    Abstract hook for multi-turn LLM agents.

    :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` resolves the concrete hook
    from the Airflow connection ``conn_type`` (for example ``pydanticai`` or ``pydanticai-bedrock``).

    :param llm_conn_id: Optional connection ID override (subclasses may apply a default).
    :param model_id: Optional model override; not all backends use this parameter.

    Subclasses implement :meth:`get_model`, :meth:`_build_agent`, :meth:`run_agent`, and
    :meth:`_tool_spec_to_native`.

    Shared helpers :meth:`_resolve_tools` and :meth:`_logged_callable` are provided for all hooks.
    Durable cache helpers live in ``DurableAgentMixin`` so non-durable hooks do not inherit
    backend-specific durable mechanics.
    """

    conn_name_attr = "llm_conn_id"

    capabilities: ClassVar[frozenset[Capability]] = frozenset()

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
    def get_agent_hook(cls, conn_id: str, *, hook_params: dict[str, Any] | None = None) -> BaseAIHook[Any]:
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
        """
        Return the backend model/client for :class:`~airflow.hooks.base.BaseHook` compatibility.

        Agent hooks use :meth:`get_model` internally; this shim keeps the traditional ``get_conn()``
        hook API available for callers that expect it.
        """
        return self.get_model()

    def create_agent(self, request: AgentRunRequest) -> AgentT:
        """
        Build (but do not run) the agent described by *request*.

        Responsible for resolving :attr:`AgentRunRequest.toolsets` via
        :meth:`_resolve_tools` and constructing the framework-native agent object
        with the model, tools, instructions, and output type from *request*.

        :param request: All parameters needed to configure the agent.
        :returns: Framework-native agent handle, ready to be passed to :meth:`run_agent`.
        """
        self.validate_run_request(request)
        return self._build_agent(request)

    @abstractmethod
    def _build_agent(self, request: AgentRunRequest) -> AgentT:
        """Build the framework-native agent handle after :meth:`validate_run_request` succeeds."""

    @abstractmethod
    def run_agent(self, agent: AgentT, request: AgentRunRequest) -> AgentRunResult:
        """
        Execute *agent* for *request* and return a normalized :class:`AgentRunResult`.

        Implementations with durable execution should keep durable state on their
        concrete agent handle, apply it during the run, and call ``storage.cleanup()``
        only after a successful run (keep the cache file when the run raises so
        Airflow retries can replay cached steps).

        :param agent: Framework-native agent handle produced by :meth:`create_agent`.
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

        :meth:`create_agent` calls this before delegating to the hook implementation.
        """
        hook_name = type(self).__name__
        conn_id = self.llm_conn_id or "unknown"
        if request.toolsets and Capability.TOOLSETS not in self.capabilities:
            raise ValueError(
                f"toolsets not supported for connection {conn_id!r} (conn_type resolves to {hook_name})."
            )
        if request.usage_limits is not None and Capability.USAGE_LIMITS not in self.capabilities:
            raise ValueError(
                f"usage_limits not supported for connection {conn_id!r} (conn_type resolves to {hook_name})."
            )
        if request.durable_context is not None and Capability.DURABLE not in self.capabilities:
            raise ValueError(
                f"durable execution not supported for connection {conn_id!r} (conn_type resolves to {hook_name})."
            )

    def _resolve_tools(
        self,
        toolsets: list[Any],
        enable_logging: bool,
        cache_wrapper: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
        *,
        force_sequential: bool = False,
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

        The processing pipeline for ``BaseToolset`` and callable items is built inside-out:
        *fn* → optional log wrapper → optional cache wrapper → :meth:`_tool_spec_to_native`.
        At execution time, the outer cache wrapper runs first, so durable cache hits skip
        the logging wrapper.

        :param toolsets: Mix of :class:`BaseToolset` instances, callables (functions, bound
            methods, :func:`functools.partial`, or callable objects), and native tool objects.
        :param enable_logging: When ``True``, wrap each callable with :meth:`_logged_callable`.
        :param cache_wrapper: Optional wrapper used by durable hooks to cache Airflow-resolved
            callable tools. Native backend tool objects are passed through unchanged.
        :param force_sequential: When ``True``, mark all Airflow-resolved callable tools as
            sequential. Native backend tool objects are passed through unchanged.
        """
        native: list[Any] = []
        for ts in toolsets:
            if isinstance(ts, BaseToolset):
                specs = ts.as_tools()
            elif callable(ts):
                specs = [callable_to_tool_spec(ts)]
            else:
                native.append(ts)
                continue
            for spec in specs:
                fn = spec.fn
                if enable_logging:
                    fn = self._logged_callable(fn, self.log, name=spec.name)
                if cache_wrapper is not None:
                    fn = cache_wrapper(fn)
                adapted = ToolSpec(
                    name=spec.name,
                    description=spec.description,
                    parameters=spec.parameters,
                    fn=fn,
                    sequential=spec.sequential or force_sequential,
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

        if is_async_callable(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                logger.info("::group::Tool call: %s", _tool_name)
                if kwargs:
                    logger.debug("Tool args: %s", json.dumps(kwargs, default=str))
                start = time.monotonic()
                try:
                    result = await fn(*args, **kwargs)
                    elapsed = time.monotonic() - start
                    logger.info("Tool %s returned in %.2fs", _tool_name, elapsed)
                    return result
                except Exception:
                    elapsed = time.monotonic() - start
                    logger.exception("Tool %s failed after %.2fs", _tool_name, elapsed)
                    raise
                finally:
                    logger.info("::endgroup::")

            return async_wrapper

        @functools.wraps(fn)
        def sync_wrapper(*args, **kwargs):
            logger.info("::group::Tool call: %s", _tool_name)
            if kwargs:
                logger.debug("Tool args: %s", json.dumps(kwargs, default=str))
            start = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                elapsed = time.monotonic() - start
                logger.info("Tool %s returned in %.2fs", _tool_name, elapsed)
                return result
            except Exception:
                elapsed = time.monotonic() - start
                logger.exception("Tool %s failed after %.2fs", _tool_name, elapsed)
                raise
            finally:
                logger.info("::endgroup::")

        return sync_wrapper
