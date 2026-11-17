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
"""Durable execution for pydantic-ai agents in Airflow tasks, on pydantic-ai's durable backend API."""

from __future__ import annotations

import base64
import copy
import dataclasses
import re
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias, TypeGuard, cast

from pydantic_ai.durable_exec import (
    JSON_CODEC,
    BaseDurabilityCapability,
    CallableOperationBackend,
    CapabilityOperationId,
    DurabilityCodec,
    DurabilityEngineSpec,
    DurableOperationId,
    JournalOperationNamer,
    ModelRequestId,
    RoleBasedOperationConfig,
    ToolsetCallToolId,
    ToolsetValidateToolArgumentsId,
)
from pydantic_ai.exceptions import (
    ApprovalRequired,
    CallDeferred,
    ModelRetry,
    ToolFailed,
    UnexpectedModelBehavior,
)
from pydantic_ai.messages import ModelResponse, ToolReturn
from pydantic_ai.tools import AgentDepsT, DeferredToolRequests
from pydantic_ai.toolsets import AbstractToolset, DynamicToolset, FunctionToolset, ToolsetTool
from pydantic_ai.toolsets.wrapper import WrapperToolset
from pydantic_core import PydanticSerializationError

from airflow.providers.common.ai.durable.base import TOOL_CONFIG_KEY
from airflow.providers.common.ai.durable.fingerprint import (
    fingerprint_deferred_tool_requests,
    fingerprint_model_request,
    fingerprint_tool_call,
)
from airflow.providers.common.ai.durable.journal import Unrecordable, current_journal
from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger, is_successful_tool_payload
from airflow.providers.common.ai.exceptions import DurableJournalError, DurableToolMetadataError
from airflow.providers.common.ai.utils.masking import mask_secrets
from airflow.providers.common.ai.utils.tool_metrics import record_tool_call
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset
from airflow.providers.common.ai.utils.toolsets import iter_toolsets
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_3_PLUS:
    # Waiting for input parks a task in the awaiting_input state, which older Airflow versions do not have.
    from airflow.sdk.exceptions import TaskAwaitingInput

if TYPE_CHECKING:
    from pydantic_ai.agent import EventStreamHandler
    from pydantic_ai.capabilities import WrapRunHandler

    # The operation a backend binds is typed only in these modules; ``bind`` itself is public.
    from pydantic_ai.durable_exec._operation import DurableOperation, ParamsT, ResultT, WireT
    from pydantic_ai.durable_exec._operation_backend import BoundDurableOperation
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.models import Model, ModelRequestContext, ModelRequestParameters
    from pydantic_ai.run import AgentRunResult
    from pydantic_ai.settings import ModelSettings
    from pydantic_ai.tools import RunContext, ToolDefinition
    from pydantic_ai.toolsets import ToolsetFunc

    from airflow.providers.common.ai.durable.journal import DurableRun, JournalStep, StepKind

__all__ = ["AirflowDurability"]

# What pydantic-ai's durable backend passes as ``cache_key``: a projection of the
# operation's parameters that it types as an opaque tuple. These spell out the shapes
# of the projections fingerprinted here.
_ModelRequestKey: TypeAlias = (
    "tuple[str | None, list[ModelMessage], ModelSettings | None, ModelRequestParameters, RunContext[Any]]"
)
_ToolCallKey: TypeAlias = "tuple[str, dict[str, Any], RunContext[Any]]"
_CapabilityOperationKey: TypeAlias = "tuple[str | None, dict[str, Any], RunContext[Any]]"

try:
    from pydantic_ai.mcp import MCPToolset
except ImportError:  # the ``mcp`` extra is not installed
    DURABLE_UNIT_TOOLSETS: tuple[type, ...] = (FunctionToolset, DynamicToolset)
else:
    DURABLE_UNIT_TOOLSETS = (FunctionToolset, DynamicToolset, MCPToolset)
"""Leaf toolsets pydantic-ai's durable backend runs as durable units; each needs a unique ``id``."""

# Airflow imports a Dag file as a module named ``unusual_prefix_<sha1 of the file's path>_<file name>``
# (``get_unique_dag_module_name`` in the shared ``module_loading`` library, which providers cannot import).
_DAG_MODULE_PREFIX = re.compile(r"^unusual_prefix_[0-9a-f]{40}_")


def _get_callable_toolset_id(function: object) -> str | None:
    """
    Return the id of a toolset that ``function`` builds per run: its module and qualified name.

    The hash of the file's path that Airflow puts in a Dag file's module name is left out,
    because the path can differ between workers and bundle versions. ``None`` for a callable
    without a qualified name, such as a ``functools.partial``.
    """
    module = getattr(function, "__module__", None)
    qualname = getattr(function, "__qualname__", None)
    if not isinstance(module, str) or not isinstance(qualname, str):
        return None
    return f"{_DAG_MODULE_PREFIX.sub('', module)}.{qualname}"


def _get_chosen_toolset_id(leaf: AbstractToolset[Any]) -> str | None:
    """
    Return the id pydantic-ai names a leaf toolset's durable steps after, or ``None`` when it has none.

    An ``MCPToolset`` without an id derives one from its server's hostname, but durable steps
    are only named after an id that was chosen for the toolset.
    """
    # Before ``_explicit_id`` (pydantic-ai 2.62) no toolset derived its id, so ``id`` is the chosen one.
    return getattr(leaf, "_explicit_id", leaf.id)


# The id ``_give_durable_ids`` gives the n-th ``FunctionToolset`` without one. The same agent gets the
# same ids on every attempt, so its requests to the model match the recorded ones. The dunder prefix
# keeps it apart from ids people choose. pydantic-ai keeps ids in angle brackets for roles it fills
# itself (``<agent>``), and rejects a colon in a toolset id because it builds instruction ids from it.
_ASSIGNED_ID_PREFIX = "__airflow_function_toolset_"


def _assigned_toolset_id(number: int) -> str:
    return f"{_ASSIGNED_ID_PREFIX}{number}"


# Each leaf ``_give_durable_ids`` copied, keyed by ``id()``, with its copy. The leaf is kept so
# that its ``id()`` cannot be reused by another object while the map is in use.
_GivenIds: TypeAlias = "dict[int, tuple[AbstractToolset[Any], AbstractToolset[Any]]]"


def _give_durable_ids(
    toolset: AbstractToolset[Any] | ToolsetFunc[Any], given: _GivenIds
) -> AbstractToolset[Any]:
    """
    Return ``toolset`` with an id on each leaf pydantic-ai runs as durable steps, where one can be given.

    pydantic-ai runs a ``FunctionToolset`` or ``DynamicToolset`` as durable steps only when
    it has an id. An id-less ``FunctionToolset`` gets one numbered in the order the toolsets
    are given, which its step names leave out (a function tool's step is named after the
    tool); a function that builds a toolset per run, or an id-less ``DynamicToolset``, gets
    ``_get_callable_toolset_id``. A leaf that already has an id, and an ``MCPToolset``, are
    left as they are. Leaves are copied, not changed, and ``given`` maps each leaf to its
    copy so a toolset listed twice stays one toolset to pydantic-ai.
    """
    if not isinstance(toolset, AbstractToolset):
        return DynamicToolset(toolset, id=_get_callable_toolset_id(toolset))
    if not any(_lacks_id(leaf) for leaf in iter_toolsets(toolset)):
        # Rebuilding wrappers via visit_and_replace breaks wrapper subclasses with their own __init__.
        return toolset

    def give(leaf: AbstractToolset[Any]) -> AbstractToolset[Any]:
        if not _lacks_id(leaf):
            return leaf
        if (entry := given.get(id(leaf))) is None:
            copied: AbstractToolset[Any]
            if isinstance(leaf, DynamicToolset):
                copied = DynamicToolset(
                    leaf.toolset_func,
                    per_run_step=leaf.per_run_step,
                    id=_get_callable_toolset_id(leaf.toolset_func),
                )
            else:
                numbered = sum(isinstance(other, FunctionToolset) for _, other in given.values())
                function_toolset = copy.copy(leaf)
                # FunctionToolset takes its id only in the constructor and keeps it here.
                function_toolset._id = _assigned_toolset_id(numbered)
                copied = function_toolset
            entry = given[id(leaf)] = (leaf, copied)
        return entry[1]

    return toolset.visit_and_replace(give)


def _lacks_id(leaf: AbstractToolset[Any]) -> TypeGuard[FunctionToolset[Any] | DynamicToolset[Any]]:
    return isinstance(leaf, FunctionToolset | DynamicToolset) and leaf.id is None


# Why a recorded step that matched is run live anyway: it was written by a version whose
# types have changed since, so replaying it would fail on every retry.
_UNLOADABLE = "the recorded result no longer loads, so it was written by another version"

# How an operation's step is run: looked up in the journal and recorded, or always run live.
_StepMode: TypeAlias = Literal["journal", "live"]


def _is_tool_journaled(tool_def: ToolDefinition) -> bool:
    """Whether calls of a tool are journaled; ``metadata={"airflow": False}`` runs them live on every attempt."""
    setting = (tool_def.metadata or {}).get(TOOL_CONFIG_KEY)
    if setting is None:
        return True
    if isinstance(setting, bool):
        return setting
    raise DurableToolMetadataError(
        f"Tool {tool_def.name!r} has invalid {TOOL_CONFIG_KEY!r} metadata: expected True or False, "
        f"got {type(setting).__name__}."
    )


def _resolve_tool_step_mode(
    operation_id: DurableOperationId, tool: object | None, tool_name: str
) -> _StepMode:
    # Never pydantic-ai's ``False``, which would take the call out of the backend: a live call still
    # takes its position, so the steps after it keep theirs, and a failure is still the step's own.
    if isinstance(tool, ToolsetTool) and not _is_tool_journaled(tool.tool_def):
        return "live"
    return "journal"


_STEP_MODES: RoleBasedOperationConfig[_StepMode] = RoleBasedOperationConfig(
    model="journal",
    event="journal",
    capability="journal",
    tool="journal",
    resolve_tool=_resolve_tool_step_mode,
)


@dataclass(frozen=True)
class _ActiveRun:
    """The journal run an agent's steps go to, and the ledger that keeps their replays out of its usage."""

    durable_run: DurableRun
    ledger: ReplayUsageLedger


_ACTIVE_RUN: ContextVar[_ActiveRun | None] = ContextVar("commonai_durability_run", default=None)


@contextmanager
def _active_run(active: _ActiveRun) -> Iterator[None]:
    token = _ACTIVE_RUN.set(active)
    try:
        yield
    finally:
        _ACTIVE_RUN.reset(token)


# A recorded ``bytes`` value: a dict whose only key is this, holding the bytes as base64.
_BYTES_KEY = "__airflow_bytes__"

# The kinds of pydantic-ai's recorded tool call result that hold the tool's value in ``result``.
_TOOL_RESULT_KINDS = frozenset({"tool_return", "tool_content_result"})


def _tag_bytes(value: Any) -> Any:
    """
    Replace the ``bytes`` in a tool result with base64 the journal can hold.

    The model sees bytes as base64 (pydantic-ai's ``tool_return_ta``), but ``JSON_CODEC``
    encodes them as UTF-8 text: it fails on bytes that are not UTF-8 and turns the rest into
    ``str``, so the retry would show the model something else. Tagged, they load back as the
    same bytes. Only the parts of a result that a tool fills freely are walked; a typed value
    such as ``BinaryContent`` encodes its own bytes.
    """
    if isinstance(value, bytes):
        return {_BYTES_KEY: base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {key: _tag_bytes(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_tag_bytes(item) for item in value]
    if isinstance(value, ToolReturn):
        return dataclasses.replace(
            value, return_value=_tag_bytes(value.return_value), metadata=_tag_bytes(value.metadata)
        )
    if (
        dataclasses.is_dataclass(value)
        and not isinstance(value, type)
        and getattr(value, "kind", None) in _TOOL_RESULT_KINDS
        and any(attribute.name == "result" and attribute.init for attribute in dataclasses.fields(value))
    ):
        # pydantic-ai's recorded tool call result, a shape its journals keep loading across versions.
        return dataclasses.replace(value, result=_tag_bytes(getattr(value, "result")))
    return value


def _untag_bytes(payload: Any) -> Any:
    """
    Turn what :func:`_tag_bytes` recorded back into bytes.

    A payload without tags loads as it is, and so does a dict a tool built that only looks
    like one, with a value that is not base64.
    """
    if isinstance(payload, dict):
        if len(payload) == 1 and isinstance(encoded := payload.get(_BYTES_KEY), str):
            try:
                return base64.b64decode(encoded, validate=True)
            except ValueError:  # binascii.Error, or a value that is not ASCII
                return payload
        return {key: _untag_bytes(item) for key, item in payload.items()}
    if isinstance(payload, list):
        return [_untag_bytes(item) for item in payload]
    return payload


def _mask_payload(payload: Any) -> Any:
    """Mask the secrets in an encoded payload, including inside its bytes, which base64 hides from the masker."""
    return _tag_bytes(mask_secrets(_untag_bytes(payload)))


def _is_model_response(payload: Any) -> bool:
    # Bytes are never tagged in a model response, so text the model wrote, such as tool call
    # arguments, is never read as a tag. The second shape is a streamed response from
    # pydantic-ai before 2.60, recorded with its events.
    return isinstance(payload, dict) and (
        payload.get("kind") == "response" or ("response" in payload and "events" in payload)
    )


class _JournalCodec(DurabilityCodec):
    """
    pydantic-ai's ``JSON_CODEC``, recording bytes as base64 and returning failures instead of raising them.

    A result that does not encode comes back as :class:`Unrecordable`, so the journal decides
    whether that fails the task (:meth:`JournalStep.cannot_record`), and ``load`` hands the
    live value on to the run either way.
    """

    def dump(self, tp: Any, value: Any) -> Any:
        try:
            return JSON_CODEC.dump(tp, _tag_bytes(value))
        except (PydanticSerializationError, TypeError, ValueError, RecursionError) as error:
            # RecursionError: a result that contains itself, or is nested too deep to walk.
            return Unrecordable(value, error)

    def load(self, tp: Any, payload: Any) -> Any:
        if isinstance(payload, Unrecordable):
            return payload.value
        return JSON_CODEC.load(tp, payload if _is_model_response(payload) else _untag_bytes(payload))


_JOURNAL_CODEC = _JournalCodec()


class AirflowDurability(BaseDurabilityCapability[AgentDepsT]):
    """
    Replay an agent's completed steps when Airflow retries the task it runs in.

    Attach it to a pydantic-ai ``Agent`` running inside an Airflow task. Each model
    request, tool call (function, MCP and dynamic toolsets, and Airflow's own toolsets),
    tool discovery and ``@durable_operation`` of another capability
    is recorded in the task instance's durable journal as it completes. When the task
    fails and Airflow retries it, the agent runs again from the start and every step the
    previous attempt completed is replayed from the journal instead of being run again:
    no second model call, no second tool side effect, no second charge against a spend
    limit. See :ref:`durable-execution` for how replay is verified.

    ``AgentOperator(durable=True)`` and ``@task.agent(durable=True)`` attach it for you.
    In a plain ``@task``, attach it yourself:

    .. code-block:: python

        @task(retries=3)
        def research(question: str) -> str:
            agent = Agent(
                "anthropic:claude-sonnet-4-5",
                name="researcher",
                toolsets=[SQLToolset("warehouse")],
                capabilities=[AirflowDurability()],
            )
            return agent.run_sync(question).output

    Outside a running Airflow task the capability does nothing, so the same agent runs
    normally in a test or a notebook. In a plain ``@task``, a run's steps are deleted as
    soon as the run succeeds, so that clearing the task later starts it fresh: a task that
    runs several agents replays only the run that failed, and runs the ones before it again.

    A tool that must run on every attempt, such as one that reads the current time or
    creates a temporary table, opts out with ``metadata={"airflow": False}``; for tools of
    a toolset you do not define, such as ``SQLToolset``, add the capability
    ``SetToolMetadata(tools=[...], airflow=False)``. pydantic-ai's own ``MCPToolset`` refuses the
    opt-out; the provider's ``MCPToolset`` accepts it.

    Toolsets the agent calls through pydantic-ai's durable backend need a unique ``id``
    (``FunctionToolset(id=...)``, ``MCPToolset(..., id=...)``), as do capabilities that
    contribute ``@durable_operation`` methods; the ids name the steps in the journal.
    ``AgentOperator`` and ``@task.agent`` fill in the ids of a ``FunctionToolset`` or a
    function that builds a toolset passed in ``toolsets`` or ``agent_params["toolsets"]``,
    and of a ``FunctionToolset`` held directly by a ``Toolset`` capability
    (:ref:`durable-toolset-ids`); in a plain ``@task`` you set them.

    :param models: Extra models the run may switch to, keyed by id; see pydantic-ai's
        ``BaseDurabilityCapability``.
    :param event_stream_handler: Optional handler for the run's events.
    :param name: Prefix for the names of the agent's steps. Defaults to the agent's
        ``name``; one of the two is required.
    """

    engine_spec: ClassVar[DurabilityEngineSpec] = DurabilityEngineSpec(
        engine_name="Airflow",
        durable_unit_noun="step",
        durable_container_noun="task",
        codec=_JOURNAL_CODEC,
    )

    def __init__(
        self,
        *,
        models: Mapping[str, Model] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(models=models, event_stream_handler=event_stream_handler, name=name)
        # Keyed by agent too: pydantic-ai binds a shallow copy of this capability to each
        # agent it is attached to, and the copies share this dict.
        self._fingerprint_models: dict[tuple[int, str | None], Model] = {}

    @property
    def in_durable_context(self) -> bool:
        return current_journal() is not None

    def get_durable_operation_backend(self) -> _AirflowOperationBackend:
        return _AirflowOperationBackend(self)

    async def before_model_request(
        self, ctx: RunContext[AgentDepsT], request_context: ModelRequestContext
    ) -> ModelRequestContext:
        # A bad ``airflow`` metadata value fails the task here, before the model is first asked,
        # rather than when the model calls the tool, which every retry would reach again. The
        # request's tool definitions carry the metadata other capabilities set on the toolsets.
        if self.in_durable_context:
            for tool_def in request_context.model_request_parameters.function_tools:
                _is_tool_journaled(tool_def)
        return await super().before_model_request(ctx, request_context)

    def get_wrapper_toolset(self, toolset: AbstractToolset[AgentDepsT]) -> AbstractToolset[AgentDepsT] | None:
        """Journal the leaf toolsets pydantic-ai's backend does not, then let the base wrap the rest."""
        journaled = toolset.visit_and_replace(self._journal_other_leaf)
        return super().get_wrapper_toolset(journaled) or journaled

    def _journal_other_leaf(self, toolset: AbstractToolset[AgentDepsT]) -> AbstractToolset[AgentDepsT]:
        # Function, dynamic and MCP toolsets are durable units of pydantic-ai's backend.
        # Anything else, such as Airflow's own SQL, hook and managed-agent toolsets, is
        # journaled here instead, or its calls would run again on every retry.
        if isinstance(toolset, DURABLE_UNIT_TOOLSETS):
            return toolset
        return _JournaledToolset(wrapped=toolset, durability=self)

    async def before_run(self, ctx: RunContext[AgentDepsT]) -> None:
        # The base refuses a run with a ``cancellation_token`` inside the durable container,
        # because for its engines the container runs elsewhere and cancelling it out of band
        # would break replay. Airflow's container is the task process itself: AgentOperator's
        # on_kill cancels the run there, the attempt fails, and the retry replays what the
        # journal recorded. That check is all the base hook does, so it is not called.
        return

    async def wrap_run(self, ctx: RunContext[AgentDepsT], *, handler: WrapRunHandler) -> AgentRunResult[Any]:
        journal = current_journal()
        if journal is None:
            return await super().wrap_run(ctx, handler=handler)
        durable_run = journal.start_run()
        # Keeps replays out of the usage the run counts and is limited by, which is the
        # cross-attempt total when AgentOperator passes one as ``usage=``.
        ledger = ReplayUsageLedger(run_usage=ctx.usage, usage_limits=ctx.usage_limits)
        ledger.credit_first_replay(durable_run)
        try:
            with _active_run(_ActiveRun(durable_run, ledger)):
                result = await super().wrap_run(ctx, handler=handler)
        except BaseException as error:
            if AIRFLOW_V_3_3_PLUS and isinstance(error, TaskAwaitingInput):
                # A step is waiting for a person, such as a tool approval: the task resumes from here.
                durable_run.pause()
                raise
            # pydantic-ai raises UnexpectedModelBehavior when the model's answers fail its checks
            # too often (a tool's ModelRetry, output validation): the answers are recorded, so a
            # retry has to ask the model again rather than replay them into the same error.
            durable_run.fail(error, model_at_fault=isinstance(error, UnexpectedModelBehavior))
            raise
        finally:
            ledger.settle()
        if journal.clean_up_after_run:
            durable_run.cleanup()
        else:
            # The task keeps the journal past the run (until its own work is done, or across a review).
            durable_run.complete()
        return result

    async def _model_for_fingerprint(
        self, model_id: str | None, run_context: RunContext[AgentDepsT]
    ) -> Model:
        """Return the model a request with ``model_id`` goes to, resolved once per agent and id."""
        key = (id(self.agent), model_id)
        if (model := self._fingerprint_models.get(key)) is None:
            model = self._fingerprint_models[key] = await self._resolve_model_for_request(
                model_id, run_context
            )
        return model


class _AirflowOperationNamer(JournalOperationNamer):
    """
    pydantic-ai's journal step names, without an id Airflow gave a function toolset.

    A function tool's step name ends in the tool's name, so the id Airflow numbered the
    toolset by adds nothing to it and would only make the name harder to read in the task
    log. An id the Dag author chose stays in the name, so steps recorded under it before
    this naming existed replay as they did. Only a tool call is journaled under its name:
    ``execute`` runs argument validation live.
    """

    def operation_name(self, operation_id: DurableOperationId) -> str:
        name = super().operation_name(operation_id)
        if (
            isinstance(operation_id, ToolsetCallToolId)
            and operation_id.toolset_kind == "function"
            and operation_id.toolset_id.startswith(_ASSIGNED_ID_PREFIX)
        ):
            return name.replace(f"__{operation_id.toolset_id}", "", 1)
        return name


class _AirflowOperationBackend(CallableOperationBackend[_StepMode]):
    """Runs each durable operation through the task's durable journal."""

    def __init__(self, durability: AirflowDurability[Any]) -> None:
        super().__init__(
            namer=_AirflowOperationNamer(
                durability.name, default_model_id=durability.default_model_id or "default"
            ),
            config=_STEP_MODES,
        )
        self._durability = durability
        # How pydantic-ai decodes each operation's result, by operation.
        self._loaders: dict[DurableOperationId, Callable[[object], object]] = {}

    def bind(
        self, operation: DurableOperation[ParamsT, WireT, ResultT]
    ) -> BoundDurableOperation[ParamsT, WireT, ResultT]:
        # pydantic-ai decodes the payload ``execute`` returns only after ``execute`` returns, when
        # it is too late to run the step live instead. Keeping the decoder lets ``execute`` reject
        # a recorded payload that no longer decodes, such as one another version wrote.
        self._loaders[operation.operation_id] = operation.result_codec.load
        return super().bind(operation)

    def _loads(self, operation_id: DurableOperationId, payload: object) -> bool:
        try:
            self._loaders[operation_id](payload)
        except (TypeError, ValueError):
            return False
        return True

    def _claim(
        self,
        active: _ActiveRun,
        operation_id: DurableOperationId,
        name: str,
        *,
        kind: StepKind,
        fingerprint: str | None,
    ) -> JournalStep:
        step = active.durable_run.claim(name, kind=kind, fingerprint=fingerprint)
        if step.replayed and not self._loads(operation_id, step.payload):
            active.durable_run.reject(step, reason=_UNLOADABLE)
        return step

    async def execute(
        self,
        *,
        operation_id: DurableOperationId,
        name: str,
        body: Callable[[], Awaitable[object]],
        cache_key: tuple[object, ...],
        config: _StepMode,
    ) -> object:
        active = _ACTIVE_RUN.get()
        if active is None:
            return await body()
        match operation_id:
            case ModelRequestId(streaming=streaming):
                return await self._model_request(
                    active, operation_id, name, body, cache_key, streaming=streaming
                )
            case ToolsetCallToolId():
                step = _claim_tool_call(
                    active,
                    name,
                    _tool_fingerprint(cache_key),
                    valid_payload=partial(self._loads, operation_id),
                    replayable=config == "journal",
                )
                if step.replayed:
                    return step.payload
                # The masking wrapper AgentOperator adds sits outside this durable unit, so mask
                # before recording: secrets must not reach the journal any more than the model.
                return await step.run(body, to_record=_mask_payload)
            case ToolsetValidateToolArgumentsId():
                # Local validation of the arguments the recorded call was made with: nothing to
                # save by replaying it, and it is the same on every attempt, so it takes no position.
                return await body()
            case CapabilityOperationId():
                step = self._claim(
                    active,
                    operation_id,
                    name,
                    kind="other",
                    fingerprint=_fingerprint_capability_operation(cache_key),
                )
                if step.replayed:
                    active.ledger.record_capability_replay(step.payload)
                    return step.payload
                return await step.run(body)
            case _:
                # Tool discovery, compaction, event handling, and the operations later
                # pydantic-ai versions add: replayed by name and position.
                step = self._claim(active, operation_id, name, kind="other", fingerprint=None)
                if step.replayed:
                    return step.payload
                return await step.run(body)

    async def _model_request(
        self,
        active: _ActiveRun,
        operation_id: ModelRequestId,
        name: str,
        body: Callable[[], Awaitable[object]],
        cache_key: tuple[object, ...],
        *,
        streaming: bool,
    ) -> object:
        model_id, messages, model_settings, parameters, run_context = cast("_ModelRequestKey", cache_key)
        fingerprint = await self._fingerprint_model_request(
            model_id, messages, model_settings, parameters, run_context
        )
        ledger = active.ledger
        continuation = ledger.begin_model_request(messages)
        had_request_credit = ledger.settle()
        step = self._claim(active, operation_id, name, kind="model", fingerprint=fingerprint)
        if step.replayed:
            response = _load_model_response(step.payload, streaming=streaming)
            ledger.record_model_replay(response, continuation=continuation)
            ledger.track_chain(response, parameters)
            if response.state != "suspended":
                ledger.credit_successors(active.durable_run, step.position)
            return step.payload
        ledger.record_live_model_request(had_request_credit=had_request_credit)
        payload = await step.run(body)
        if isinstance(payload, Unrecordable):
            # Left unrecorded by a journal that does not fail on it: the live response is the value.
            response = payload.value if isinstance(payload.value, ModelResponse) else payload.value.response
        else:
            response = _load_model_response(payload, streaming=streaming)
        ledger.track_chain(response, parameters)
        return payload

    async def _fingerprint_model_request(
        self,
        model_id: str | None,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        parameters: ModelRequestParameters,
        run_context: RunContext[Any],
    ) -> str | None:
        # Fingerprint the request as the model will prepare it, not the raw arguments.
        # ``prepare_request`` merges the model's own settings and applies profile
        # transforms (thinking, native tools, output mode) before the provider sees the
        # request, so a change that lives only on the connection, such as a different
        # temperature, still invalidates the recorded response. It is pure, so calling it
        # here as well as in the request itself is safe.
        model = await self._durability._model_for_fingerprint(model_id, run_context)
        prepared_settings, prepared_parameters = model.prepare_request(model_settings, parameters)
        return fingerprint_model_request(
            f"{model.system}:{model.model_name}", messages, prepared_settings, prepared_parameters
        )


@dataclass
class _JournaledToolset(WrapperToolset[Any]):
    """
    Journals the calls of a leaf toolset that pydantic-ai's durable backend does not wrap.

    Results, and the control-flow exceptions a tool raises for the model (``ModelRetry``,
    ``ToolFailed``, ``ApprovalRequired``, ``CallDeferred``), are recorded as values, so a
    retry replays them exactly. Any other exception is recorded as a failure and the call
    runs again on retry. Results are encoded as for the toolsets pydantic-ai runs as durable
    steps, bytes included, and one that cannot be encoded is handled the same way.
    """

    durability: AirflowDurability[Any] = field(repr=False, kw_only=True)

    def visit_and_replace(
        self, visitor: Callable[[AbstractToolset[Any]], AbstractToolset[Any]]
    ) -> AbstractToolset[Any]:
        # A durable unit, like pydantic-ai's own durable toolsets: a later visit, such as the
        # journaling pass of the next run, must not reach the leaf and wrap it again.
        return self

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext[Any], tool: ToolsetTool[Any]
    ) -> Any:
        active = _ACTIVE_RUN.get()
        if active is None:
            return await self.wrapped.call_tool(name, tool_args, ctx, tool)
        leaf = self.wrapped
        step = _claim_tool_call(
            active,
            f"{self.durability.name}__airflow_toolset__{leaf.id or type(leaf).__name__}.call_tool:{name}",
            fingerprint_tool_call(name, tool_args, ctx.tool_call_id),
            valid_payload=_is_journaled_tool_payload,
            # A toolset whose calls act on a system Airflow cannot observe, such as a managed
            # agent, runs them again on every attempt, and so does a tool opted out by its metadata.
            replayable=(not isinstance(leaf, AirflowToolset) or leaf.replayable)
            and _is_tool_journaled(tool.tool_def),
        )
        if step.replayed:
            if isinstance(leaf, AirflowToolset):
                record_tool_call(type(leaf).__name__, "replayed")
            return _decode_tool_payload(step.payload)
        return await _run_and_record_tool(step, lambda: leaf.call_tool(name, tool_args, ctx, tool))


def _claim_tool_call(
    active: _ActiveRun,
    name: str,
    fingerprint: str | None,
    *,
    valid_payload: Callable[[Any], bool],
    replayable: bool = True,
) -> JournalStep:
    """Claim a tool call's step and keep the ledger's count of tool calls in step with it."""
    step = active.durable_run.claim(name, kind="tool", fingerprint=fingerprint, replayable=replayable)
    if step.replayed and not valid_payload(step.payload):
        active.durable_run.reject(step, reason=_UNLOADABLE)
    if not step.replayed:
        active.ledger.record_live_tool_call(step.position)
    elif is_successful_tool_payload(step.payload):
        active.ledger.record_tool_replay(step.position)
    return step


async def _run_and_record_tool(step: JournalStep, call: Callable[[], Awaitable[Any]]) -> Any:
    try:
        with step.executing():
            result = await call()
    except ModelRetry as e:
        _record_tool_payload(step, {"kind": "model_retry", "message": e.message})
        raise
    except ToolFailed as e:
        _record_tool_payload(step, {"kind": "tool_failed", "message": e.message})
        raise
    except ApprovalRequired as e:
        _record_tool_payload(step, {"kind": "approval_required", "metadata": e.metadata})
        raise
    except CallDeferred as e:
        _record_tool_payload(step, {"kind": "call_deferred", "metadata": e.metadata})
        raise
    except Exception as error:
        step.fail(error)
        raise
    key = "tool_return" if isinstance(result, ToolReturn) else "result"
    _record_tool_payload(step, {"kind": "tool_return", key: result})
    return result


def _record_tool_payload(step: JournalStep, payload: dict[str, Any]) -> None:
    if not step.replayable:
        return
    encoded = _JOURNAL_CODEC.dump(Any, payload)
    if isinstance(encoded, Unrecordable):
        step.cannot_record(encoded.error)
    else:
        step.record(_mask_payload(encoded))


def _decode_tool_payload(payload: Any) -> Any:
    payload = _untag_bytes(payload)
    kind = payload.get("kind") if isinstance(payload, dict) else None
    match kind:
        case "tool_return" if "tool_return" in payload:
            return JSON_CODEC.load(ToolReturn, payload["tool_return"])
        case "tool_return":
            return payload["result"]
        case "model_retry":
            raise ModelRetry(payload["message"])
        case "tool_failed":
            raise ToolFailed(payload["message"])
        case "approval_required":
            raise ApprovalRequired(metadata=payload.get("metadata"))
        case "call_deferred":
            raise CallDeferred(metadata=payload.get("metadata"))
    raise DurableJournalError(f"durable execution found a tool result it cannot replay: {kind!r}")


def _load_model_response(payload: object, *, streaming: bool) -> ModelResponse:
    """
    Decode a model response payload.

    pydantic-ai before 2.60 records a streamed request as the response together with the events
    it streamed; 2.60 records the bare response, and still replays journals with the older shape.
    """
    raw = payload["response"] if streaming and isinstance(payload, dict) and "events" in payload else payload
    return JSON_CODEC.load(ModelResponse, raw)


def _is_journaled_tool_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    match payload.get("kind"):
        case "tool_return" if "tool_return" in payload or "result" in payload:
            try:
                _decode_tool_payload(payload)
            except (TypeError, ValueError):
                return False
            return True
        case "model_retry" | "tool_failed":
            return isinstance(payload.get("message"), str)
        case "approval_required" | "call_deferred":
            return True
    return False


def _fingerprint_capability_operation(cache_key: tuple[object, ...]) -> str | None:
    """
    Fingerprint a capability operation that settles deferred tool calls, such as an approval, by those calls.

    Its answer is about these calls, so it must not replay for others. Any other capability
    operation has no fingerprint, and replays by name and position.
    """
    _, arguments, _ = cast("_CapabilityOperationKey", cache_key)
    for argument in arguments.values():
        if isinstance(argument, DeferredToolRequests):
            return fingerprint_deferred_tool_requests(argument)
    return None


def _tool_fingerprint(cache_key: tuple[object, ...]) -> str | None:
    # Function and MCP calls project (name, args, ctx, tool); dynamic ones (name, args, ctx, tool_def).
    tool_name, tool_args, ctx = cast("_ToolCallKey", cache_key[:3])
    return fingerprint_tool_call(tool_name, tool_args, ctx.tool_call_id)
