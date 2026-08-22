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
from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    return_schema_kwargs,
    serialize_for_llm,
)
from airflow.providers.common.compat.sdk import Stats

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic_ai._run_context import RunContext

log = logging.getLogger(__name__)

_PROMPT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "prompt": {
            "type": "string",
            "description": "The question or instruction to send to this agent.",
        }
    },
    "required": ["prompt"],
}


class BaseManagedAgentToolset(AbstractToolset[Any]):
    """
    Base class exposing a vendor-managed agent as a single pydantic-ai tool.

    A managed agent runs its own reasoning loop on the vendor's infrastructure
    (Snowflake Cortex Agents, Amazon Bedrock AgentCore, Azure AI Foundry hosted
    agents, Vertex AI Agent Engine). Airflow submits one request and reads one
    answer, so the Airflow-side agent features -- toolsets, human-in-the-loop
    review, durable step replay -- apply to the *calling* agent and never reach
    inside the managed agent.

    Subclasses implement :meth:`agent_ref` and :meth:`invoke`. Tool naming,
    argument validation, result serialisation and logging are handled here so
    every provider's implementation presents the same surface to the model.

    :param tool_name: Name the calling model sees, and the identifier it emits
        when calling the tool. A verb phrase naming the specialist reads best,
        e.g. ``ask_bookings_analyst``.
    :param description: What this agent knows and when to consult it. Optional --
        it falls back to ``tool_name`` rendered as prose, matching how
        ``HookToolset`` handles a method with no docstring. Worth writing anyway:
        it is what tells the model to consult the agent rather than answer from
        its own knowledge, and it is the only place to state a scope limit the
        name cannot carry ("cannot see revenue figures"). Since the argument
        schema is always a bare prompt, the name and this string are the whole
        of what the model knows about the agent.
    :param timeout: Seconds to wait for a single invocation. ``None`` defers to
        the platform default, which subclasses supply -- a number chosen here
        would silently disagree with the vendor operator's documented timeout
        for the same service.
    :param max_retries: How many times the calling model may rephrase after the
        remote agent raises ``ModelRetry``. ``0`` turns the first ``ModelRetry``
        into a hard error, which disables that recovery path entirely.
    """

    #: Whether a completed invocation may be replayed from the durable cache
    #: instead of re-invoked. Off by default because a managed agent may act on
    #: systems Airflow cannot observe, so replaying a cached answer could skip a
    #: side effect. Read-only agents should opt in.
    replayable: bool = False

    def __init__(
        self,
        *,
        tool_name: str,
        description: str | None = None,
        timeout: float | None = None,
        max_retries: int = 1,
    ) -> None:
        if not tool_name:
            raise ValueError("tool_name must be a non-empty string.")
        if max_retries < 0:
            raise ValueError(f"max_retries must not be negative, got {max_retries}.")
        cls = type(self)
        if (
            cls.invoke is BaseManagedAgentToolset.invoke
            and cls.invoke_sync is BaseManagedAgentToolset.invoke_sync
        ):
            raise TypeError(
                f"{cls.__name__} must implement invoke_sync() for a blocking vendor SDK, "
                "or override invoke() for a natively async client."
            )
        self._tool_name = tool_name
        # Same fallback as HookToolset uses for a method with no docstring.
        self._description = (description or "").strip() or tool_name.replace("_", " ").capitalize()
        self._timeout = timeout
        self._max_retries = max_retries

    @property
    @abstractmethod
    def agent_ref(self) -> dict[str, str]:
        """
        Normalised identity of the remote agent.

        Must contain ``platform`` and ``name``, e.g.
        ``{"platform": "snowflake.cortex", "name": "ANALYTICS.REVENUE.BOOKINGS_ANALYST"}``.
        Logged whenever this toolset's tool is called, so the resolved remote
        identity behind a task appears in that task's log even though the Dag
        only names a connection.
        """

    async def invoke(self, prompt: str) -> Any:
        """
        Send ``prompt`` to the remote agent and return the agent's answer.

        Override this when the client is natively async -- the Azure SDK's
        ``aio`` variants, the async Google client, ``aiobotocore``. When it is
        synchronous -- boto3 for Bedrock AgentCore, the Snowflake connector for
        Cortex -- implement :meth:`invoke_sync` instead and let the default
        implementation here run it in a worker thread, which keeps a blocking
        call off the event loop that the whole agent run shares. Several vendors
        ship both, so the choice follows the client rather than the platform.

        Return the answer, not the transport envelope -- whatever the calling
        model should actually read. Unwrapping is the implementation's job.

        Failures sort into three buckets, and conflating them is the most common
        way an implementation goes wrong:

        * ``pydantic_ai.exceptions.ModelRetry`` -- the remote agent rejected the
          request in a way rephrasing could fix. The calling model sees the
          message and tries again, bounded by its ``usage_limits``.
        * :class:`~airflow.providers.common.ai.exceptions.ManagedAgentInvocationError`
          -- terminal. Bad credentials, missing agent, revoked quota. Neither a
          rephrase nor a task retry helps, so fail fast.
        * Anything transient (429, 5xx, connection reset, read timeout) -- let it
          propagate unchanged. Airflow's task-level retry is the right layer; a
          rephrase does nothing for a 503.

        **Release anything you allocate, on every path.** Platforms that require a
        session bill for its lifetime, so an implementation that opens one here
        must close it in a ``finally`` -- including when ``ModelRetry`` propagates,
        which is a return path the calling model treats as recoverable and will
        therefore hit repeatedly. A tool call has no post-task cleanup hook to
        fall back on: if the worker dies mid-call the handle is lost, and nothing
        will reap the remote session. Implementations whose sessions are long
        enough for that to matter belong in that provider's own operator, where
        deferral and :class:`~airflow.sdk.bases.resumablejobmixin.ResumableJobMixin`
        can reconnect to the existing job instead of leaking it.

        :param prompt: The question or instruction to send to the remote agent.
        """
        return await asyncio.to_thread(self.invoke_sync, prompt)

    def invoke_sync(self, prompt: str) -> Any:
        """
        Blocking variant of :meth:`invoke`, run in a worker thread.

        This is the hook to implement when the managed agent's client is
        synchronous. Call it normally -- the base class keeps it off the event
        loop, so a call that takes minutes does not stall the calling agent's
        other tool calls.

        The contract is :meth:`invoke`'s: return the answer rather than the
        transport envelope, sort failures into the same three buckets, and
        release anything allocated on every path.

        :param prompt: The question or instruction to send to the remote agent.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement invoke_sync() for a blocking vendor SDK, "
            "or override invoke() for a natively async client."
        )

    @property
    def id(self) -> str:
        return f"managed-agent-{self._tool_name}"

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tool_def = ToolDefinition(
            name=self._tool_name,
            description=self._description,
            parameters_json_schema=_PROMPT_SCHEMA,
            # HookToolset sets sequential=True because its tools call synchronous
            # hook methods straight from the event loop. Here a blocking SDK goes
            # through invoke_sync(), which the base class runs in a worker thread,
            # and each call is an independent request to a remote service -- so
            # two calls the model issues in one turn really can run at once.
            sequential=False,
            **return_schema_kwargs({"type": "string"}),
        )
        return {
            self._tool_name: ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                # How many times the calling model may rephrase after ``invoke``
                # raises ``ModelRetry``. One by default, matching HookToolset: a
                # managed agent invocation is expensive, so the budget is small.
                # Zero disables the ``ModelRetry`` path entirely -- the first one
                # becomes a hard error -- so raise it only when the remote agent's
                # rejections are genuinely worth re-prompting.
                max_retries=self._max_retries,
                args_validator=build_args_validator(_PROMPT_SCHEMA),
            )
        }

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        ref = self.agent_ref
        log.info("Consulting managed agent %s on %s", ref.get("name"), ref.get("platform"))
        result = await self.invoke(tool_args["prompt"])
        return serialize_for_llm(result)


class FailoverManagedAgentToolset(BaseManagedAgentToolset):
    """
    Present several interchangeable managed agents to the model as one tool.

    Active/passive failover for a managed agent: members are tried in order and
    the first answer wins. Because this is itself a
    :class:`BaseManagedAgentToolset`, the calling model sees a single tool and
    has no say in which provider serves the request -- the policy stays
    deterministic Python rather than a prompt instruction a model may ignore.
    Groups nest, so a group can itself be a member of another group.

    Members must satisfy two preconditions that this class cannot check:

    *Substitutability.* The same agent deployed twice, not two specialists with
    different data. Two containerised agents built from one image (Bedrock
    AgentCore and Azure AI Foundry hosted agents, say) qualify; agents backed by
    different corpora or bound to one platform's own objects -- a Cortex Agent
    over Snowflake semantic models -- do not, because there is no equivalent to
    fail over *to*.

    *Statelessness per invocation.* Server-side conversation state is the norm
    rather than the exception across managed-agent platforms -- optional on some
    (Cortex ``thread_id``), mandatory on others, where a session must be created
    and torn down around every exchange. Each member here is invoked with a bare
    prompt and no thread reference, so a failover silently starts a fresh
    conversation on the standby. That is correct for a one-shot consultation and
    wrong for a multi-turn one: failover discards the thread rather than resuming
    it elsewhere. Since most platforms fall on the stateful side, treat one-shot
    as something a group is deliberately restricted to, not a safe default.

    :param members: Interchangeable toolsets, tried in order. At least two.
    :param failover_on: Exception types that move to the next member. Defaults
        to ``Exception`` because ``common.ai`` cannot enumerate the cloud SDKs'
        exception trees (``requests``, ``botocore`` and the Azure SDK share no
        common base), so the safe default is broad. It can be narrowed when the
        members' exception types are known. ``ModelRetry`` is always re-raised
        and never triggers failover, whatever this is set to.
    """

    def __init__(
        self,
        *,
        members: Sequence[BaseManagedAgentToolset],
        failover_on: tuple[type[BaseException], ...] = (Exception,),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if len(members) < 2:
            raise ValueError(
                "A failover group needs at least two members; "
                f"got {len(members)}. Use the member toolset directly instead."
            )
        # Copied, not aliased: the loop in invoke() relies on the group being
        # non-empty, and a caller holding the original list could otherwise empty
        # it after construction.
        self._members = tuple(members)
        self._failover_on = failover_on
        # Replay is only safe if every member is safe to replay: the cache cannot
        # know which member produced the answer it holds.
        self.replayable = all(m.replayable for m in members)

    @property
    def agent_ref(self) -> dict[str, str]:
        return {
            "platform": "failover",
            "name": " -> ".join(m.agent_ref.get("name", "?") for m in self._members),
        }

    async def invoke(self, prompt: str) -> Any:
        last = len(self._members) - 1
        for position, member in enumerate(self._members):
            ref = member.agent_ref
            try:
                result = await member.invoke(prompt)
            except ModelRetry:
                # The model can fix this by rephrasing, and the standby would
                # reject the same prompt identically. Failing over would spend
                # the standby's budget to reproduce the same error.
                raise
            except self._failover_on:
                if position == last:
                    raise
                standby = self._members[position + 1].agent_ref
                log.warning(
                    "Managed agent %s on %s failed; failing over to %s",
                    ref.get("name"),
                    ref.get("platform"),
                    standby.get("name"),
                    exc_info=True,
                )
                # Metrics, not just logs: a failover is a success-shaped event, so
                # without a counter a primary that has been down for a week looks
                # identical to a healthy one. Tagged by platform rather than agent
                # name to keep cardinality bounded.
                Stats.incr(
                    "managed_agent.failover",
                    tags={
                        "tool": self._tool_name,
                        "from_platform": ref.get("platform", "unknown"),
                        "to_platform": standby.get("platform", "unknown"),
                    },
                )
                continue
            served_by_standby = position > 0
            if served_by_standby:
                log.info("Managed agent request served by standby %s", ref.get("name"))
            # Emitted on every answer so the standby-served fraction is a ratio of
            # this counter, not something that has to be scanned out of XCom.
            Stats.incr(
                "managed_agent.served",
                tags={
                    "tool": self._tool_name,
                    "platform": ref.get("platform", "unknown"),
                    "role": "standby" if served_by_standby else "primary",
                    "position": str(position),
                },
            )
            return result
