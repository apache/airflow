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
The contract a provider hook implements to expose a vendor-managed agent.

A managed agent runs its own reasoning loop on the vendor's infrastructure -- Snowflake
Cortex Agents, Amazon Bedrock AgentCore, Azure AI Foundry hosted agents, Vertex AI Agent
Engine. Airflow submits a request and reads an answer. This module defines the shape of that
exchange once, so that every consumer in ``common.ai`` (the toolset, the failover group)
is written against one interface rather than one per cloud.

The design follows ``DbApiHook`` in ``common.sql``: a small base mixed into each vendor's
own hook, with the agent as an *argument* to every method, because a hook is scoped to a
connection and one connection reaches many agents. Vendor providers adopt it the way they
adopt ``BaseMessageQueueProvider`` from ``common.messaging``: behind an optional extra, in a
module whose import of this contract is guarded, so a provider that floors Airflow 2 never
has to raise its floor.

This module imports nothing from pydantic-ai on purpose. A vendor hook's guarded import of
the contract must stay cheap.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar, Protocol, runtime_checkable


@dataclass(frozen=True)
class ManagedAgentRef:
    """
    Normalized identity of a remote agent.

    :param platform: A stable, dotted platform id such as ``aws.bedrock_agentcore`` or
        ``gcp.vertex_agent_engine``. Used as a metric tag, so keep the set small.
    :param name: The vendor's canonical identifier for the agent: an ARN, a full resource
        name, ``DATABASE.SCHEMA.NAME``.
    :param version: The resolved version or revision when the platform exposes one. Recorded
        so a behaviour change can be attributed to a deployment rather than to Airflow.
    """

    platform: str
    name: str
    version: str | None = None


@dataclass(frozen=True)
class ManagedAgentCapabilities:
    """
    What a ``(hook, agent)`` pair can do.

    Consumers check these and refuse rather than degrade: a bound agent rejects a request that
    carries a ``session_id`` when ``sessions`` is False, and a failover group never offers
    sessions at all, because failing over discards the conversation the primary was holding.
    """

    sessions: bool = False
    """Whether ``ManagedAgentRequest.session_id`` continues a conversation."""
    structured_output: bool = False
    """Whether ``ManagedAgentResponse.structured`` can carry a typed value."""
    usage: bool = False
    """Whether ``ManagedAgentResponse.usage`` is populated."""
    trace: bool = False
    """Whether ``ManagedAgentResponse.trace_ref`` is populated."""


@dataclass(frozen=True)
class ManagedAgentRequest:
    """
    One request to a managed agent.

    Exactly one of ``prompt`` and ``messages`` must be set. Everything the contract does not
    type travels in ``vendor_options``, which the hook passes through to the vendor call.
    Hooks reject options that would re-target the call (the agent identity, the connection),
    because a model-facing caller must not be able to change what it is talking to.
    """

    prompt: str | None = None
    messages: Sequence[dict[str, Any]] | None = None
    session_id: str | None = None
    timeout: float | None = None
    """Seconds to wait for the vendor call. The hook must enforce it on the request itself."""
    vendor_options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (self.prompt is None) == (self.messages is None):
            raise ValueError("ManagedAgentRequest needs exactly one of prompt or messages.")

    def as_messages(self) -> list[dict[str, Any]]:
        """Return the request as a message list, for vendors that only accept messages."""
        if self.messages is not None:
            return list(self.messages)
        return [{"role": "user", "content": [{"type": "text", "text": self.prompt}]}]


@dataclass(frozen=True)
class ManagedAgentUsage:
    """Usage the vendor reported for one invocation. Every field is optional because vendors differ."""

    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass(frozen=True)
class ManagedAgentResponse:
    """
    One answer from a managed agent.

    ``text`` is what a calling model should read; the hook unwraps the vendor envelope to
    produce it. ``raw`` is that envelope, always populated and never handed to a model, so a
    Python caller loses nothing.
    """

    text: str
    raw: Any
    structured: Any | None = None
    session_id: str | None = None
    usage: ManagedAgentUsage | None = None
    trace_ref: str | None = None
    """A vendor request, invocation or trace id, for joining Airflow's record to the vendor's."""


class BaseManagedAgentHook(ABC):
    """
    Mixin a vendor hook adopts to expose its managed agents through the common contract.

    Mixed in beside the vendor's own base and never replacing it::

        class BedrockAgentCoreManagedAgentHook(BedrockAgentCoreHook, BaseManagedAgentHook): ...

    It therefore has no ``__init__`` and makes no assumption about ``get_conn``. The agent is
    an argument to every method, the way a statement is an argument to ``DbApiHook.run``.

    Method names are chosen to collide with nothing on the shipped vendor hooks. That matters
    more than it looks: ``SnowflakeCortexAgentHook`` already defines ``run_agent`` and
    ``describe_agent``, and an abstract method that a vendor base happens to define is
    silently satisfied with the wrong signature.

    Implementations sort failures into three classes, and conflating them is the most common
    way an adoption goes wrong:

    * :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected` -- the agent
      rejected the request in a way rephrasing could fix. The toolset turns it into a
      pydantic-ai ``ModelRetry`` so the calling model tries again.
    * :class:`~airflow.providers.common.ai.exceptions.ManagedAgentInvocationError` --
      terminal: bad credentials, missing agent, revoked quota. Nothing on the agent side
      recovers it; whether the task retries is the task's retry policy.
    * Anything transient (429, 5xx, connection reset, read timeout) -- propagate unchanged.
      Airflow's task-level retry is the right layer; a rephrase does nothing for a 503.
    """

    agent_platform: ClassVar[str]
    """The ``platform`` every :class:`ManagedAgentRef` from this hook carries."""

    @abstractmethod
    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        """Normalize ``agent`` into a platform-qualified reference. Must not make a network call."""

    @abstractmethod
    def agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        """Report what ``agent`` on this connection can do. Must not make a network call."""

    @abstractmethod
    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        """Send ``request`` to ``agent`` and return its answer. Blocking."""

    def agent(self, agent: str) -> BoundManagedAgent:
        """Bind one agent on this connection. This is what the ``common.ai`` toolsets consume."""
        return BoundManagedAgent(hook=self, agent=agent)


@runtime_checkable
class ManagedAgentClient(Protocol):
    """
    What ``common.ai``'s consumers are typed against.

    A :class:`BoundManagedAgent` satisfies it. So does
    :class:`~airflow.providers.common.ai.managed_agents.failover.FailoverManagedAgentClient`,
    and so can anything that needs no Airflow connection at all.
    """

    @property
    def ref(self) -> ManagedAgentRef: ...

    @property
    def capabilities(self) -> ManagedAgentCapabilities: ...

    def invoke(self, request: ManagedAgentRequest) -> ManagedAgentResponse: ...


@dataclass(frozen=True)
class BoundManagedAgent:
    """
    A ``(hook, agent)`` pair. Forwards to the hook and resolves identity lazily.

    This is also where the contract's "refuse rather than degrade" rule is enforced for every
    adopter: a request that asks for something the pair's capabilities do not include is
    rejected before the hook is called.
    """

    hook: BaseManagedAgentHook
    agent: str

    @property
    def ref(self) -> ManagedAgentRef:
        return self.hook.resolve_agent(self.agent)

    @property
    def capabilities(self) -> ManagedAgentCapabilities:
        return self.hook.agent_capabilities(self.agent)

    def invoke(self, request: ManagedAgentRequest) -> ManagedAgentResponse:
        if request.session_id is not None and not self.capabilities.sessions:
            raise ValueError(
                f"{self.agent} does not keep conversation state, so session_id cannot be honored. "
                "Refusing rather than silently starting a fresh conversation."
            )
        return self.hook.invoke_agent(self.agent, request)


def describe(client: ManagedAgentClient) -> str:
    """
    Render a client's identity for a log line without letting identity resolution fail the caller.

    A standby whose connection is misconfigured must not fail a call the primary served.
    """
    try:
        ref = client.ref
    except Exception as exc:
        return f"<unresolved: {exc}>"
    return f"{ref.name} on {ref.platform}"
