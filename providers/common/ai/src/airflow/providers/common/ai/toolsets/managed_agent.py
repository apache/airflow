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

from airflow.providers.common.ai.exceptions import ManagedAgentRejected
from airflow.providers.common.ai.managed_agents.contract import (
    ManagedAgentClient,
    ManagedAgentRef,
    ManagedAgentRequest,
)
from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    return_schema_kwargs,
    serialize_for_llm,
)
from airflow.providers.common.compat.sdk import Stats

if TYPE_CHECKING:
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

    A managed agent runs its own reasoning loop on the vendor's infrastructure. Airflow
    submits one request and reads one answer, so the Airflow-side agent features --
    toolsets, human-in-the-loop review, durable step replay -- apply to the *calling* agent
    and never reach inside the managed agent.

    Most code should not subclass this. Use :class:`ManagedAgentToolset` over a
    :class:`~airflow.providers.common.ai.managed_agents.contract.ManagedAgentClient`, which
    every provider hook that adopts
    :class:`~airflow.providers.common.ai.managed_agents.contract.BaseManagedAgentHook`
    produces via ``hook.agent(...)``. Subclass this directly only for an agent that has no
    hook at all. Subclasses implement :attr:`agent_ref` and :meth:`invoke_sync` (or override
    the async :meth:`invoke`); tool naming, argument validation, result serialization,
    logging and metrics are handled here so every implementation presents the same surface
    to the model.

    :param tool_name: Name the calling model sees, and the identifier it emits when calling
        the tool. A verb phrase naming the specialist reads best, e.g. ``ask_bookings_analyst``.
    :param description: What this agent knows and when to consult it. Optional -- it falls
        back to ``tool_name`` rendered as prose, matching how ``HookToolset`` handles a method
        with no docstring. Worth writing anyway: it is what tells the model to consult the
        agent rather than answer from its own knowledge, and it is the only place to state a
        scope limit the name cannot carry ("cannot see revenue figures"). Since the argument
        schema is always a bare prompt, the name and this string are the whole of what the
        model knows about the agent.
    :param timeout: Seconds to wait for a single invocation. ``None`` defers to the platform
        default. Exposed as :attr:`timeout` so an implementation can honor it.
    :param max_retries: How many times the calling model may rephrase after the remote agent
        rejects a request. ``0`` turns the first rejection into a hard error.
    """

    #: Whether ``durable=True`` may replay a completed invocation from its cache instead of
    #: re-invoking. Off by default because a managed agent may act on systems Airflow cannot
    #: observe, so replaying a cached answer could skip a side effect. Read-only agents may
    #: opt in.
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
    def timeout(self) -> float | None:
        """Seconds to wait for one invocation, or ``None`` for the platform default."""
        return self._timeout

    @property
    @abstractmethod
    def agent_ref(self) -> ManagedAgentRef:
        """
        Normalized identity of the remote agent.

        Logged after every successful call, so the resolved remote identity behind a task
        appears in that task's log even though the Dag only names a connection. Resolution
        is never on the call's critical path: a failure here is logged, not raised.
        """

    async def invoke(self, prompt: str) -> Any:
        """
        Send ``prompt`` to the remote agent and return the agent's answer.

        Override this when the vendor call is already asynchronous. When it blocks, implement
        :meth:`invoke_sync` instead and let the default implementation here run it in a worker
        thread, which keeps it off the event loop that the whole agent run shares.

        Return the answer, not the transport envelope. Failures sort into three classes:
        ``ModelRetry`` (the model can fix it by rephrasing; a hook-backed client raises
        :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected` instead and
        :class:`ManagedAgentToolset` translates it),
        :class:`~airflow.providers.common.ai.exceptions.ManagedAgentInvocationError`
        (terminal), and anything transient, which should propagate unchanged so Airflow's
        task-level retry handles it.

        **Release anything you allocate, on every path.** Platforms that require a session
        bill for its lifetime, so an implementation that opens one here must close it in a
        ``finally``. A tool call has no post-task cleanup hook to fall back on.

        :param prompt: The question or instruction to send to the remote agent.
        """
        return await asyncio.to_thread(self.invoke_sync, prompt)

    def invoke_sync(self, prompt: str) -> Any:
        """
        Blocking variant of :meth:`invoke`, run in a worker thread.

        A thread cannot be cancelled, so set a timeout on the underlying request: a caller
        that stops waiting does not stop this call.

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
            # HookToolset sets sequential=True because its tools call synchronous hook
            # methods straight from the event loop. Here a blocking SDK goes through
            # invoke_sync(), which runs in a worker thread, and each call is an independent
            # request to a remote service -- so two calls the model issues in one turn
            # really can run at once.
            sequential=False,
            **return_schema_kwargs({"type": "string"}),
        )
        return {
            self._tool_name: ToolsetTool(
                toolset=self,
                tool_def=tool_def,
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
        result = await self.invoke(tool_args["prompt"])
        # Identity is resolved after the call, never before it: a toolset whose identity comes
        # from a misconfigured connection must not fail a call that would have succeeded, and
        # a failover group's identity joins every member's, standbys included.
        ref = self._safe_agent_ref()
        log.info(
            "Consulted managed agent %s",
            f"{ref.name} on {ref.platform}"
            if ref is not None
            else f"<unresolved identity> for tool {self._tool_name}",
        )
        # Emitted once per answer so managed-agent call volume is observable next to the
        # ``managed_agent.failover`` counter. Tagged by platform to bound cardinality.
        Stats.incr(
            "managed_agent.served",
            tags={"tool": self._tool_name, "platform": ref.platform if ref is not None else "unknown"},
        )
        return serialize_for_llm(result)

    def _safe_agent_ref(self) -> ManagedAgentRef | None:
        """Resolve identity for a log line or a metric tag without letting resolution fail the call."""
        try:
            return self.agent_ref
        except Exception:
            log.warning("Managed agent identity could not be resolved", exc_info=True)
            return None


class ManagedAgentToolset(BaseManagedAgentToolset):
    """
    Expose any managed-agent client as one tool.

    This is the toolset to use. It accepts any
    :class:`~airflow.providers.common.ai.managed_agents.contract.ManagedAgentClient`. The client is usually a
    :class:`~airflow.providers.common.ai.managed_agents.contract.BoundManagedAgent` from a
    vendor hook's ``agent()`` method, or a
    :class:`~airflow.providers.common.ai.managed_agents.failover.FailoverManagedAgentClient`
    over several of them::

        from airflow.providers.amazon.aws.hooks.bedrock_managed_agent import (
            BedrockAgentCoreManagedAgentHook,
        )
        from airflow.providers.common.ai.toolsets import ManagedAgentToolset

        claims = BedrockAgentCoreManagedAgentHook(aws_conn_id="aws_prod").agent(RUNTIME_ARN)
        toolset = ManagedAgentToolset(
            claims,
            tool_name="ask_claims_agent",
            description="Reviews an insurance claim and returns a coverage determination.",
        )

    The model receives ``response.text``. The vendor envelope in ``response.raw`` is for
    Python callers of the client and never reaches the model.

    :param client: The agent to consult.
    :param tool_name: See :class:`BaseManagedAgentToolset`.
    :param description: See :class:`BaseManagedAgentToolset`.
    :param timeout: Passed to the client on every request as ``ManagedAgentRequest.timeout``.
        ``None`` means whatever the vendor client defaults to, which for Agent Engine is no
        deadline at all.
    :param max_retries: See :class:`BaseManagedAgentToolset`.
    :param replayable: Whether the durable cache may replay a completed call. Only set it
        for an agent that is read-only.
    :param vendor_options: Sent with every request as ``ManagedAgentRequest.vendor_options``,
        for per-agent settings the vendor hook accepts there (Agent Engine's ``class_method``,
        for instance). The hook decides which keys are allowed.
    """

    def __init__(
        self,
        client: ManagedAgentClient,
        *,
        tool_name: str,
        description: str | None = None,
        timeout: float | None = None,
        max_retries: int = 1,
        replayable: bool = False,
        vendor_options: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            tool_name=tool_name, description=description, timeout=timeout, max_retries=max_retries
        )
        if not callable(getattr(client, "invoke", None)) or not hasattr(type(client), "ref"):
            raise TypeError(
                f"{type(client).__name__} is not a ManagedAgentClient. Pass hook.agent(...) rather than "
                "the hook, or an object with `ref`, `capabilities` and `invoke`."
            )
        self._client = client
        self._vendor_options = dict(vendor_options or {})
        self.replayable = replayable

    @property
    def client(self) -> ManagedAgentClient:
        return self._client

    @property
    def agent_ref(self) -> ManagedAgentRef:
        return self._client.ref

    def invoke_sync(self, prompt: str) -> str:
        request = ManagedAgentRequest(
            prompt=prompt, timeout=self._timeout, vendor_options=dict(self._vendor_options)
        )
        try:
            response = self._client.invoke(request)
        except ManagedAgentRejected as exc:
            # The contract keeps pydantic-ai out of the vendor hooks; this is the one place
            # a rejection becomes something the calling model can act on.
            raise ModelRetry(str(exc)) from exc
        return response.text
