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
"""
Replay an OpenAI Agents SDK agent's completed model and tool calls when Airflow retries its task.

.. note:: Experimental; see :ref:`howto/stability`.
"""

from __future__ import annotations

import copy
import json
from typing import TYPE_CHECKING, Any, NoReturn

from pydantic import TypeAdapter, ValidationError
from pydantic_core import to_jsonable_python

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from agents import (
        Agent,
        FunctionTool,
        Model,
        ModelProvider,
        ModelResponse,
        RunConfig,
        Runner,
        ToolOutputFileContent,
        ToolOutputImage,
        ToolOutputText,
        Usage,
    )
    from agents.exceptions import (
        MaxTurnsExceeded,
        ModelBehaviorError,
        ModelRefusalError,
        OutputGuardrailTripwireTriggered,
        ToolInputGuardrailTripwireTriggered,
    )
    from agents.items import TResponseOutputItem
    from agents.tool import (
        default_tool_error_function,
        resolve_function_tool_failure_error_function,
        set_function_tool_failure_error_function,
    )
    from openai.types.responses import ResponseFunctionToolCall
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.durable.fingerprint import _digest, fingerprint_tool_call
from airflow.providers.common.ai.durable.journal import _EXECUTING_STEP, current_journal
from airflow.providers.common.ai.exceptions import DurableJournalError, UnsupportedToolDeferralError
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync
from airflow.providers.common.ai.utils.masking import dumps_masked, mask_secrets
from airflow.providers.common.ai.utils.task_logger import get_task_logger
from airflow.providers.common.compat.sdk import Param, get_current_context
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_4_PLUS
from airflow.providers.standard.operators.python import PythonOperator

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from agents import (
        AgentOutputSchemaBase,
        Handoff,
        ModelSettings,
        ModelTracing,
        RunResult,
        RunState,
        Tool,
        ToolApprovalItem,
    )
    from agents.items import TResponseInputItem, TResponseStreamEvent
    from agents.retry import ModelRetryAdvice, ModelRetryAdviceRequest
    from agents.tool_context import ToolContext

    from airflow.providers.common.ai.durable.journal import DurableRun, JournalStep
    from airflow.providers.standard.utils.hitl import InputResponse

__all__ = ["run_durably", "run_durably_sync"]

log = get_task_logger()

_OUTPUT_ITEMS: TypeAdapter[list[TResponseOutputItem]] = TypeAdapter(list[TResponseOutputItem])

# Errors that come from the model's answers rather than from a step. The answers were
# recorded, so a retry must ask the model again instead of replaying them into the same error.
_MODEL_AT_FAULT = (
    ModelBehaviorError,
    ModelRefusalError,
    MaxTurnsExceeded,
    OutputGuardrailTripwireTriggered,
    ToolInputGuardrailTripwireTriggered,
)

# The tool results the SDK hands the model as structured content rather than as text.
_STRUCTURED_OUTPUTS = (ToolOutputText, ToolOutputImage, ToolOutputFileContent)

# Set on a tool call's context by the SDK when its default failure_error_function turned the
# tool's exception into the text the model sees.
_DEFAULT_FAILURE_HANDLED = "_function_tool_default_failure_handled"

# Arguments that keep the conversation on OpenAI's servers, which a replayed model call never reaches.
_SERVER_SIDE_STATE = ("previous_response_id", "conversation_id", "auto_previous_response_id")

# Set on the copy of a function tool that reports through the journal, so that reserving a position
# skips tools nothing wraps, such as those of MCP servers or of a handoff agent with a named model,
# and on the copy of an agent whose calls replay, so that only its approvals pause the task.
_DURABLE_MARKER = "_airflow_durable"


async def run_durably(
    starting_agent: Agent[Any],
    input: str | list[TResponseInputItem],
    *,
    run_config: RunConfig | None = None,
    **kwargs: Any,
) -> RunResult:
    """
    Run an agent like ``Runner.run``, so that a retry of the task replays what already completed.

    Each model call and each function tool call the run makes is recorded in the task's
    durable journal as it completes. When the task fails and Airflow retries it, the agent
    runs again from the start, and every call that asks what the previous attempt asked,
    in the same order, returns the recorded result instead of calling the model or the tool
    again. From the first call that differs, such as a model answer that changed, that call
    and every call after it run live. Once the run succeeds what was recorded is deleted, so
    clearing the task later starts the agent over.

    .. code-block:: python

        from agents import Agent

        from airflow.providers.common.ai.durable.openai_agents import run_durably_sync
        from airflow.providers.common.ai.tools.openai_agents import AirflowTools
        from airflow.sdk import task


        @task(retries=3)
        def research(question: str) -> str:
            agent = Agent(name="analyst", model=model, tools=AirflowTools(warehouse))
            return run_durably_sync(agent, question).final_output

    Recorded are the calls of every model given by name or in ``run_config``, the model and
    function tools of the starting agent and of the agents in its ``handoffs`` list, and an
    agent used through ``as_tool``, as one call. These run live on every attempt: tools
    other than function tools (hosted tools such as web search, the tools of
    ``mcp_servers``, shell and computer tools), a function tool whose ``needs_approval`` is
    a function, since the SDK checks its arguments only through its own invoker, the
    function tools of an agent reached through ``handoff()``, and its model too unless the
    agent's ``model`` is a model name or not set, and streamed runs. A tool call that fails,
    including one whose exception ``failure_error_function`` turns into text for the model,
    is not recorded either, so a retry runs it again. A replayed model call reports no
    usage, since it costs nothing.

    A ``session`` makes every retry run live: the SDK adds the failed try's prompt to it
    before the first model call, so that call no longer asks what it asked then.

    A tool call that needs approval (``needs_approval``) is asked with the standard
    provider's ``request_input()``, which pauses the task in Required Actions. The answer
    resumes the task, the run replays up to the call, and the call is approved, or rejected
    with the reviewer's reason as its result. Steps are deleted only once a run ends without
    a pending approval.

    Outside a task there is no journal, and this is ``Runner.run``.

    :param starting_agent: The agent to run.
    :param input: The prompt, as for ``Runner.run``.
    :param run_config: As for ``Runner.run``.
    :param kwargs: Passed to ``Runner.run``, such as ``max_turns`` or ``context``.
    :raises ValueError: When ``previous_response_id``, ``conversation_id`` or
        ``auto_previous_response_id`` is passed: a replayed call never reaches the
        conversation OpenAI keeps for them.
    :raises UnsupportedToolDeferralError: When the run stops for a tool call that needs
        approval and cannot pause for it: before Airflow 3.4 or without a standard provider
        that has ``request_input()``, with a ``session``, or for a call by an agent whose
        model is not recorded. The task fails without retrying.
    """
    if (journal := current_journal()) is None:
        return await Runner.run(starting_agent, input, run_config=run_config, **kwargs)
    if stateful := [name for name in _SERVER_SIDE_STATE if kwargs.get(name)]:
        raise ValueError(
            f"run_durably() cannot replay a run that keeps its conversation on OpenAI's servers "
            f"({', '.join(stateful)}): a replayed model call never reaches it."
        )
    durable_run = journal.start_run()
    config = copy.copy(run_config) if run_config is not None else RunConfig()
    config.model_provider = _DurableModelProvider(config.model_provider, durable_run)
    if isinstance(config.model, Model):
        config.model = _DurableModel(config.model, durable_run)
    agent = _get_durable_agent(starting_agent, durable_run, {})

    async def run(run_input: str | list[TResponseInputItem] | RunState[Any]) -> RunResult:
        try:
            return await Runner.run(agent, run_input, run_config=config, **kwargs)
        except BaseException as error:
            durable_run.fail(error, model_at_fault=isinstance(error, _MODEL_AT_FAULT))
            raise

    result = await run(input)
    # Each answer comes from a person through Required Actions. Asking pauses the task, which
    # then runs again from the top: the run replays up to here and gets the answer back.
    answered: set[str] = set()
    while result.interruptions:
        result = await run(
            _answer_approvals(result, durable_run, answered, has_session=kwargs.get("session") is not None)
        )
    if _answer_left_unread(durable_run, answered):
        _did_not_come_back(durable_run)
    # Only now: a run paused for approval replays what it recorded when the task resumes.
    if journal.clean_up_after_run:
        durable_run.cleanup()
    return result


def run_durably_sync(
    starting_agent: Agent[Any],
    input: str | list[TResponseInputItem],
    *,
    run_config: RunConfig | None = None,
    **kwargs: Any,
) -> RunResult:
    """Run :func:`run_durably` from synchronous code, such as a plain ``@task``."""
    return run_coroutine_sync(run_durably(starting_agent, input, run_config=run_config, **kwargs))


def _answer_approvals(
    result: RunResult, durable_run: DurableRun, answered: set[str], *, has_session: bool
) -> RunState[Any]:
    """
    Return the run's state with every pending tool call approved or rejected by a person.

    A call is asked through ``request_input()``, which pauses the task the first time and
    returns the answer once the task resumes; its key goes into ``answered``. A call whose
    result is already recorded was approved and ran in an earlier run of the task, so it is
    approved again without asking.

    :raises UnsupportedToolDeferralError: When the run cannot pause for the approval.
    :raises DurableJournalError: When the task resumed with an answer but the run did not come
        back to the call it answers.
    """
    if (request_input := _get_request_input()) is None:
        _cannot_pause(
            result,
            durable_run,
            "pausing a task for input needs Airflow 3.4+ and a standard provider that has request_input()",
        )
    if (reason := _why_approval_cannot_pause(result, has_session)) is not None:
        _cannot_pause(result, durable_run, reason)
    durable_run.pause()
    state = result.to_state()
    for item in result.interruptions:
        call = item.raw_item
        if durable_run.has_recorded(call.call_id):
            log.info(
                "Durable: %s ran after it was approved in an earlier run of this task; approving it again",
                item.tool_name,
            )
            state.approve(item)
            continue
        # The call comes back the same on every resume, as the answer that made it replays.
        # Its fingerprint keeps an answer from applying to a different call that reuses its id.
        key = f"{durable_run.key}:{call.call_id}:{_build_tool_fingerprint(call.name, call.arguments, call.call_id)}"
        try:
            answer = request_input(
                subject=f"{item.agent.name} wants to call {item.tool_name}",
                body=_describe_call(item),
                options=[LLMApprovalMixin.APPROVE, LLMApprovalMixin.REJECT],
                # An empty default, not None: some UI versions show a null one as "[object Object]".
                params={
                    "reason": Param(
                        "",
                        type=["string", "null"],
                        description="Sent to the agent when you reject the call (optional).",
                    )
                },
                key=key,
            )
        except RuntimeError as error:
            # Raised by request_input() when the answer the task resumed with is for a call that
            # this run never reached; asking again would never end.
            _did_not_come_back(durable_run, error)
        answered.add(key)
        responder = LLMApprovalMixin._describe_responder(dict(answer))
        if answer["chosen_options"] == [LLMApprovalMixin.APPROVE]:
            log.info("Durable: %s approved by %s", item.tool_name, responder)
            state.approve(item)
        else:
            log.info("Durable: %s rejected by %s", item.tool_name, responder)
            # A field left empty comes back as None or "", and some UI versions send the param's spec.
            reason = answer["params_input"].get("reason")
            reason = reason.strip() if isinstance(reason, str) else ""
            state.reject(item, rejection_message=reason or None)
    return state


def _answer_left_unread(durable_run: DurableRun, answered: set[str]) -> bool:
    """Whether the task resumed with an answer to an approval of this run that the run never asked again for."""
    try:
        # Optional: the answer log comes with request_input(), newer than the standard provider common.ai requires.
        from airflow.providers.standard.utils.hitl import answer_log_var
    except ImportError:
        return False
    answer_log = answer_log_var.get()
    return (
        answer_log is not None
        and answer_log.latest.startswith(f"{durable_run.key}:")
        and answer_log.latest not in answered
    )


def _did_not_come_back(durable_run: DurableRun, cause: BaseException | None = None) -> NoReturn:
    """
    Fail the task resumed with an answer whose call the run never reached again.

    ``cause`` is set when the run stopped at another call that needs approval (a clear asks
    about that one), and ``None`` when it finished without one (a clear replays the result).
    """
    then = (
        "Clear the task to ask about the call it stopped at instead."
        if cause is not None
        else "It finished without that call; clearing the task replays that result without asking."
    )
    error = DurableJournalError(
        "The task resumed with an answer to a tool approval, but the agent run did not come back "
        "to the call it answers: a step before that call gave a different result this time, such "
        "as a tool that always runs live, a prompt built from live data, or a change to the Dag "
        f"while the task waited. {then}"
    )
    durable_run.fail(error)
    raise error from cause


def _get_request_input() -> Callable[..., InputResponse] | None:
    """Return the standard provider's ``request_input()``, or ``None`` where it cannot pause a task."""
    if not AIRFLOW_V_3_4_PLUS:
        return None
    try:
        # Optional: request_input() is newer than the standard provider release common.ai requires.
        from airflow.providers.standard.operators.hitl import request_input
    except ImportError:
        return None
    return request_input


def _cannot_pause(result: RunResult, durable_run: DurableRun, reason: str) -> NoReturn:
    tools = ", ".join(sorted({f"{item.tool_name} ({item.agent.name})" for item in result.interruptions}))
    error = UnsupportedToolDeferralError(
        f"The agent stopped to ask for approval to call {tools}, and run_durably() cannot "
        f"pause the task for it: {reason}. Remove the approval requirement (needs_approval, "
        "or require_approval on an MCP server), or call Runner.run and handle "
        "result.interruptions yourself."
    )
    # Counted against the model's last answer, so a clear asks the model again from there:
    # what was recorded after it, such as an agent tool's answer given while it waited for
    # approval, never replays, and the turns before it still do.
    durable_run.fail(error, model_at_fault=True)
    raise error


def _why_approval_cannot_pause(result: RunResult, has_session: bool) -> str | None:
    if has_session:
        return (
            "a run with a session asks the model again on every resume, so the call it paused on "
            "never comes back to receive the answer"
        )
    for item in result.interruptions:
        if not isinstance(item.raw_item, ResponseFunctionToolCall) or not getattr(
            item.agent, _DURABLE_MARKER, False
        ):
            return (
                f"{item.tool_name} is not a function tool of an agent whose model run_durably() "
                "records, such as an agent used through as_tool() or reached through handoff(), "
                "so its call would not come back the same when the task resumes"
            )
    task = get_current_context()["task"]
    if not isinstance(task, PythonOperator):
        return f"request_input() can pause a @task or PythonOperator, and this is a {type(task).__name__}"
    return None


def _describe_call(item: ToolApprovalItem) -> str:
    """Describe the call for the reviewer in Markdown: the agent, the tool and its masked arguments."""
    arguments = dumps_masked(_parse_arguments(item.raw_item.arguments), indent=2)
    tool = next(
        (tool for tool in item.agent.tools if isinstance(tool, FunctionTool) and tool.name == item.tool_name),
        None,
    )
    description = f" — {tool.description}" if tool is not None and tool.description else ""
    return (
        f"**Agent:** {item.agent.name}\n\n**Tool:** {item.tool_name}{description}\n\n"
        f"**Arguments**\n\n```json\n{arguments}\n```"
    )


def _get_durable_agent(agent: Agent[Any], durable_run: DurableRun, seen: dict[int, Agent[Any]]) -> Agent[Any]:
    """Return a copy of ``agent`` whose model, function tools and handoff agents go through the journal."""
    if (done := seen.get(id(agent))) is not None:
        return done
    durable_agent = agent.clone()
    # Its calls replay the same on resume, so an approval it asks for can pause the task.
    setattr(durable_agent, _DURABLE_MARKER, True)
    # Recorded before the handoffs are copied, so agents that hand off to each other copy once.
    seen[id(agent)] = durable_agent
    if isinstance(agent.model, Model):
        durable_agent.model = _DurableModel(agent.model, durable_run)
    durable_agent.tools = [_get_durable_tool(tool, durable_run) for tool in agent.tools]
    durable_agent.handoffs = [
        _get_durable_agent(target, durable_run, seen) if isinstance(target, Agent) else target
        for target in agent.handoffs
    ]
    return durable_agent


def _get_durable_tool(tool: Tool, durable_run: DurableRun) -> Tool:
    # A tool whose approval depends on its arguments is left as it is: the SDK checks that the
    # arguments it approved are the ones the tool runs with only through its own invoker.
    if not isinstance(tool, FunctionTool) or callable(tool.needs_approval):
        return tool
    # A shallow copy, not dataclasses.replace: the tool's schema is already prepared.
    durable_tool = copy.copy(tool)
    setattr(durable_tool, _DURABLE_MARKER, True)
    invoke = tool.on_invoke_tool
    # Exceptions a custom failure_error_function turned into text, by tool call.
    handled: dict[str, Exception] = {}
    formatter = resolve_function_tool_failure_error_function(tool)
    bind = getattr(invoke, "__agents_bind_function_tool__", None)
    if formatter is not None and formatter is not default_tool_error_function and bind is not None:

        def remember(ctx: Any, error: Exception) -> Any:
            handled[ctx.tool_call_id] = error
            return formatter(ctx, error)

        set_function_tool_failure_error_function(durable_tool, remember)
        invoke = bind(durable_tool)

    async def on_invoke_tool(ctx: ToolContext[Any], arguments: str) -> Any:
        if _is_inside_a_call(durable_run):
            return await invoke(ctx, arguments)
        name = f"tool:{tool.name}"
        fingerprint = _build_tool_fingerprint(tool.name, arguments, ctx.tool_call_id)
        step = durable_run.claim_reserved(ctx.tool_call_id, name, kind="tool", fingerprint=fingerprint)
        if step.replayed:
            return step.payload
        try:
            with step.executing():
                output = await invoke(ctx, arguments)
        except Exception as error:
            step.fail(error)
            raise
        # A failure the SDK turned into text for the model is not recorded, so a retry runs the
        # call again, as it does a call that raised.
        if (handled_error := handled.pop(ctx.tool_call_id, None)) is not None:
            step.fail(handled_error)
        elif not getattr(ctx, _DEFAULT_FAILURE_HANDLED, False):
            # Secrets must not reach the journal any more than the model.
            step.record(mask_secrets(_as_sent(output)))
        return output

    durable_tool.on_invoke_tool = on_invoke_tool
    return durable_tool


def _parse_arguments(arguments: str) -> Any:
    """Return a tool call's arguments as the model sent them: parsed JSON, or the raw text when it is not JSON."""
    try:
        return json.loads(arguments) if arguments else {}
    except json.JSONDecodeError:
        return arguments


def _build_tool_fingerprint(name: str, arguments: str, call_id: str) -> str | None:
    parsed = _parse_arguments(arguments)
    return fingerprint_tool_call(
        name,
        # As recorded model answers are masked, so a replayed call's arguments can be too.
        mask_secrets(parsed if isinstance(parsed, dict) else {"arguments": parsed}),
        call_id,
    )


def _as_sent(output: Any) -> Any:
    """
    Return a tool's result in the form the SDK sends it to the model, as a JSON-compatible value.

    The SDK sends a ``ToolOutputText``, ``ToolOutputImage`` or ``ToolOutputFileContent``, a
    ``dict`` with a ``type``, or a list of them, as structured content, and anything else as
    ``str(output)``. Recording anything else as it is would replay it as a different text
    (a model's fields as a dict, a tuple as a list), so the next model call would no longer
    match the one the failed try made.
    """
    if isinstance(output, str):
        return output
    items = output if isinstance(output, list | tuple) else [output]
    if items and all(
        isinstance(item, _STRUCTURED_OUTPUTS) or (isinstance(item, dict) and "type" in item) for item in items
    ):
        return to_jsonable_python(output)
    return str(output)


def _is_inside_a_call(durable_run: DurableRun) -> bool:
    """
    Whether a recorded call of ``durable_run`` is running, such as an agent called through ``as_tool``.

    That call is recorded as one step, and a retry replays it whole, so what it does inside
    runs live rather than taking positions of the run it belongs to.
    """
    step = _EXECUTING_STEP.get()
    return step is not None and step.durable_run is durable_run


class _DurableModelProvider(ModelProvider):
    """Wrap every model the run resolves by name in a :class:`_DurableModel`."""

    def __init__(self, provider: ModelProvider, durable_run: DurableRun) -> None:
        self.provider = provider
        self.durable_run = durable_run
        self.models: dict[str | None, _DurableModel] = {}

    def get_model(self, model_name: str | None) -> Model:
        if model_name not in self.models:
            self.models[model_name] = _DurableModel(self.provider.get_model(model_name), self.durable_run)
        return self.models[model_name]


class _DurableModel(Model):
    """Replay a model call the previous attempt recorded, or make it and record the response."""

    def __init__(self, model: Model, durable_run: DurableRun) -> None:
        self.wrapped = model
        self.durable_run = durable_run
        # The step of a call that raised, which the SDK's own model retry may make again.
        self.failed_step: JournalStep | None = None

    async def get_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> ModelResponse:
        if _is_inside_a_call(self.durable_run):
            return await self.wrapped.get_response(
                system_instructions, input, model_settings, tools, output_schema, handoffs, tracing, **kwargs
            )
        fingerprint = _fingerprint_request(
            self.wrapped, system_instructions, input, model_settings, tools, output_schema, handoffs, kwargs
        )
        step = self._retried(fingerprint) or self.durable_run.claim(
            "model", kind="model", fingerprint=fingerprint
        )
        if step.replayed:
            try:
                replayed = ModelResponse(
                    output=_OUTPUT_ITEMS.validate_python(step.payload["output"]),
                    usage=Usage(),
                    response_id=step.payload.get("response_id"),
                )
            except (ValidationError, KeyError, TypeError) as error:
                self.durable_run.reject(step, reason=f"the recorded model response cannot be read: {error}")
            else:
                self._reserve_tool_steps(replayed, tools)
                return replayed

        async def call() -> ModelResponse:
            return await self.wrapped.get_response(
                system_instructions, input, model_settings, tools, output_schema, handoffs, tracing, **kwargs
            )

        try:
            response = await step.run(call, to_record=_record_response)
        except Exception:
            self.failed_step = step
            raise
        self._reserve_tool_steps(response, tools)
        return response

    def _reserve_tool_steps(self, response: ModelResponse, tools: list[Tool]) -> None:
        """
        Claim a position for each of the response's tool calls, in the order the model made them.

        A call whose tool never runs (held for approval, stopped by a guardrail) leaves its position
        empty, which a retry runs live without disturbing the positions after it. The steps the
        previous response reserved and no tool took are dropped.
        """
        durable_names = {tool.name for tool in tools if getattr(tool, _DURABLE_MARKER, False)}
        self.durable_run.reserve(
            [
                (
                    item.call_id,
                    f"tool:{item.name}",
                    _build_tool_fingerprint(item.name, item.arguments, item.call_id),
                )
                for item in response.output
                if isinstance(item, ResponseFunctionToolCall) and item.name in durable_names
            ],
            kind="tool",
        )

    def _retried(self, fingerprint: str | None) -> JournalStep | None:
        """
        Return the step of the call that just raised, when this call is the SDK retrying it.

        The SDK's model retry asks again with the same request and nothing in between; it is
        the same call, so it takes the same position, and the next try lines up with this one.
        """
        failed, self.failed_step = self.failed_step, None
        if failed is None or failed.fingerprint != fingerprint:
            return None
        return failed if self.durable_run.position == failed.position + 1 else None

    def stream_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> AsyncIterator[TResponseStreamEvent]:
        # A streamed response is not recorded: it reaches the caller event by event.
        return self.wrapped.stream_response(
            system_instructions, input, model_settings, tools, output_schema, handoffs, tracing, **kwargs
        )

    def _supports_default_prompt_cache_key(self) -> bool:
        # The runner asks the model whether to add a prompt cache key; the answer is the wrapped model's.
        supports = getattr(self.wrapped, "_supports_default_prompt_cache_key", None)
        return bool(supports()) if callable(supports) else False

    def get_retry_advice(self, request: ModelRetryAdviceRequest) -> ModelRetryAdvice | None:
        return self.wrapped.get_retry_advice(request)

    async def _cleanup_on_run_end(self, owner: object) -> None:
        await self.wrapped._cleanup_on_run_end(owner)

    async def close(self) -> None:
        await self.wrapped.close()


def _record_response(response: ModelResponse) -> dict[str, Any]:
    # Only the fields the response set, so a replayed item turns into the same next request as
    # the live one: the SDK carries every field of an item into the conversation it sends.
    output = [item.model_dump(mode="json", exclude_unset=True) for item in response.output]
    # Masked, as the journal's other steps are; a replay hands the model its own masked answer.
    return {"output": mask_secrets(output), "response_id": response.response_id}


def _fingerprint_request(
    model: Model,
    system_instructions: str | None,
    input: str | list[TResponseInputItem],
    model_settings: ModelSettings,
    tools: list[Tool],
    output_schema: AgentOutputSchemaBase | None,
    handoffs: list[Handoff],
    options: dict[str, Any],
) -> str | None:
    """
    Fingerprint what a model call asks: the model, instructions, conversation, settings and tools.

    ``None`` when the request cannot be serialized; the call then replays on position alone,
    and only right after a call that replayed.
    """
    try:
        settings = to_jsonable_python(model_settings)
        # Generated once per run, so it differs on every attempt without changing the request.
        if isinstance(extra := settings.get("extra_args"), dict):
            extra.pop("prompt_cache_key", None)
        payload = {
            "model": f"{type(model).__module__}.{type(model).__qualname__}:{getattr(model, 'model', None)}",
            "instructions": system_instructions,
            # Masked as the recorded steps are, so a replayed conversation matches a live one.
            "input": _without_none(mask_secrets(to_jsonable_python(input))),
            "settings": settings,
            "tools": [_describe_tool(tool) for tool in tools],
            "output": output_schema.json_schema()
            if output_schema and not output_schema.is_plain_text()
            else None,
            "handoffs": [[handoff.tool_name, handoff.input_json_schema] for handoff in handoffs],
            "prompt": to_jsonable_python(options.get("prompt")),
        }
        return _digest(payload)
    except (TypeError, ValueError):
        log.warning(
            "Durable: could not fingerprint the model request; its recorded response replays "
            "without verification"
        )
        return None


def _without_none(value: Any) -> Any:
    """
    Drop ``None`` fields, so an item that leaves a field unset matches one that sets it to ``None``.

    Which of the two the SDK sends depends on how the item was built, not on what was asked.
    """
    if isinstance(value, dict):
        return {key: _without_none(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_without_none(item) for item in value]
    return value


def _describe_tool(tool: Tool) -> Any:
    """Return what the model is told about a tool, for the request fingerprint."""
    if isinstance(tool, FunctionTool):
        return [tool.name, tool.description, tool.params_json_schema, tool.strict_json_schema]
    # A hosted tool's configuration, such as a file search's vector stores, without its callbacks.
    return [type(tool).__name__, to_jsonable_python(tool, fallback=lambda value: type(value).__name__)]
