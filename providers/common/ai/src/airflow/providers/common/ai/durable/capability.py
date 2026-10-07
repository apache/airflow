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

from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, cast

from pydantic_ai.durable_exec import (
    JSON_CODEC,
    BaseDurabilityCapability,
    CapabilityOperationId,
    DurabilityEngineSpec,
    DurableOperationId,
    JournalCallableOperationBackend,
    ModelRequestId,
    RoleBasedOperationConfig,
    ToolsetCallToolId,
    ToolsetValidateToolArgumentsId,
)
from pydantic_ai.exceptions import ApprovalRequired, CallDeferred, ModelRetry, ToolFailed
from pydantic_ai.messages import ModelResponse, ToolReturn
from pydantic_ai.tools import AgentDepsT
from pydantic_ai.toolsets import DynamicToolset, FunctionToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset
from pydantic_core import PydanticSerializationError

from airflow.providers.common.ai.durable.fingerprint import fingerprint_model_request, fingerprint_tool_call
from airflow.providers.common.ai.durable.journal import current_journal
from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger, is_successful_tool_payload
from airflow.providers.common.ai.exceptions import DurableJournalError
from airflow.providers.common.ai.utils.masking import mask_secrets
from airflow.providers.common.ai.utils.tool_metrics import record_tool_call
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset

if TYPE_CHECKING:
    from pydantic_ai.agent import EventStreamHandler
    from pydantic_ai.capabilities import WrapRunHandler
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.models import Model, ModelRequestParameters
    from pydantic_ai.run import AgentRunResult
    from pydantic_ai.settings import ModelSettings
    from pydantic_ai.tools import RunContext
    from pydantic_ai.toolsets import AbstractToolset, ToolsetTool

    from airflow.providers.common.ai.durable.journal import DurableRun, JournalStep

__all__ = ["AirflowDurability"]

# What pydantic-ai's durable backend passes as ``cache_key``: a projection of the
# operation's parameters that it types as an opaque tuple. These spell out the shapes
# of the two projections fingerprinted here.
_ModelRequestKey: TypeAlias = (
    "tuple[str | None, list[ModelMessage], ModelSettings | None, ModelRequestParameters, RunContext[Any]]"
)
_ToolCallKey: TypeAlias = "tuple[str, dict[str, Any], RunContext[Any]]"

try:
    from pydantic_ai.mcp import MCPToolset
except ImportError:  # the ``mcp`` extra is not installed
    DURABLE_UNIT_TOOLSETS: tuple[type, ...] = (FunctionToolset, DynamicToolset)
else:
    DURABLE_UNIT_TOOLSETS = (FunctionToolset, DynamicToolset, MCPToolset)
"""Leaf toolsets pydantic-ai's durable backend runs as durable units; each needs a unique ``id``."""

# Why a recorded step that matched is run live anyway: it was written by a version whose
# types have changed since, so replaying it would fail on every retry.
_UNLOADABLE = "the recorded result no longer loads, so it was written by another version"

_NO_CONFIG: RoleBasedOperationConfig[None] = RoleBasedOperationConfig(
    model=None, event=None, capability=None, tool=None
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


def _not_journalable(error: Exception) -> BaseException:
    # The same value fails the same way on every retry, so retrying the task cannot help.
    return DurableJournalError(
        f"durable execution could not record a step's result, because it is not JSON-serializable: {error}"
    )


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

    Toolsets the agent calls through pydantic-ai's durable backend need a unique ``id``
    (``FunctionToolset(id=...)``, ``MCPToolset(..., id=...)``), as do capabilities that
    contribute ``@durable_operation`` methods; the ids name the steps in the journal.

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
        codec=JSON_CODEC,
        serialization_failure=_not_journalable,
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
            durable_run.fail(error)
            raise
        finally:
            ledger.settle()
        if journal.clean_up_after_run:
            durable_run.cleanup()
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


class _AirflowOperationBackend(JournalCallableOperationBackend[None]):
    """Runs each durable operation through the task's durable journal."""

    def __init__(self, durability: AirflowDurability[Any]) -> None:
        super().__init__(
            agent_name=durability.name, default_model_id=durability.default_model_id, config=_NO_CONFIG
        )
        self._durability = durability

    async def execute(
        self,
        *,
        operation_id: DurableOperationId,
        name: str,
        body: Callable[[], Awaitable[object]],
        cache_key: tuple[object, ...],
        config: None,
    ) -> object:
        active = _ACTIVE_RUN.get()
        if active is None:
            return await body()
        match operation_id:
            case ModelRequestId(streaming=streaming):
                return await self._model_request(active, name, body, cache_key, streaming=streaming)
            case ToolsetCallToolId():
                step = _claim_tool_call(
                    active, name, _tool_fingerprint(cache_key), valid_payload=_is_recorded_tool_result
                )
                if step.replayed:
                    return step.payload
                # The masking wrapper AgentOperator adds sits outside this durable unit, so mask
                # before recording: secrets must not reach the journal any more than the model.
                return await step.run(body, to_record=mask_secrets)
            case ToolsetValidateToolArgumentsId():
                # Local validation of the arguments the recorded call was made with: nothing to
                # save by replaying it, and it is the same on every attempt, so it takes no position.
                return await body()
            case CapabilityOperationId():
                step = active.durable_run.claim(name, kind="other", fingerprint=None)
                if step.replayed:
                    active.ledger.record_capability_replay(step.payload)
                    return step.payload
                return await step.run(body)
            case _:
                # Tool discovery, compaction, event handling, and the operations later
                # pydantic-ai versions add: replayed by name and position.
                return await active.durable_run.run(name, kind="other", fingerprint=None, body=body)

    async def _model_request(
        self,
        active: _ActiveRun,
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
        step = active.durable_run.claim(name, kind="model", fingerprint=fingerprint)
        response = _load_model_response(step.payload, streaming=streaming) if step.replayed else None
        if step.replayed and response is None:
            active.durable_run.reject(step, reason=_UNLOADABLE)
        if response is not None:
            ledger.record_model_replay(response, continuation=continuation)
            ledger.track_chain(response, parameters)
            if response.state != "suspended":
                ledger.credit_successors(active.durable_run, step.position)
            return step.payload
        ledger.record_live_model_request(had_request_credit=had_request_credit)
        payload = await step.run(body)
        if (live := _load_model_response(payload, streaming=streaming)) is not None:
            ledger.track_chain(live, parameters)
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
    runs again on retry. A result that is not JSON-serializable fails the task, as it does
    for the toolsets pydantic-ai runs as durable steps.
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
            # agent, runs them again on every attempt.
            replayable=leaf.replayable if isinstance(leaf, AirflowToolset) else True,
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
    try:
        encoded = JSON_CODEC.dump(Any, payload)
    except (PydanticSerializationError, TypeError, ValueError) as error:
        # As for the toolsets pydantic-ai runs as durable steps: the same value fails the same
        # way on every attempt, and the model could not have read it either.
        raise _not_journalable(error) from error
    step.record(mask_secrets(encoded))


def _decode_tool_payload(payload: Any) -> Any:
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


def _load_model_response(payload: object, *, streaming: bool) -> ModelResponse | None:
    """Decode a recorded model response, or return ``None`` when it no longer loads."""
    # A streamed request records the response together with the events it streamed.
    raw = payload.get("response") if streaming and isinstance(payload, dict) else payload
    try:
        return JSON_CODEC.load(ModelResponse, raw)
    except (TypeError, ValueError):
        return None


def _is_recorded_tool_result(payload: object) -> bool:
    # pydantic-ai decodes what its own toolsets recorded with a type it keeps private, so only
    # the field it dispatches on can be checked here.
    return isinstance(payload, dict) and isinstance(payload.get("kind"), str)


def _is_journaled_tool_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    match payload.get("kind"):
        case "tool_return" if "tool_return" in payload:
            try:
                JSON_CODEC.load(ToolReturn, payload["tool_return"])
            except (TypeError, ValueError):
                return False
            return True
        case "tool_return":
            return "result" in payload
        case "model_retry" | "tool_failed":
            return isinstance(payload.get("message"), str)
        case "approval_required" | "call_deferred":
            return True
    return False


def _tool_fingerprint(cache_key: tuple[object, ...]) -> str | None:
    # Function and MCP calls project (name, args, ctx, tool); dynamic ones (name, args, ctx, tool_def).
    tool_name, tool_args, ctx = cast("_ToolCallKey", cache_key[:3])
    return fingerprint_tool_call(tool_name, tool_args, ctx.tool_call_id)
