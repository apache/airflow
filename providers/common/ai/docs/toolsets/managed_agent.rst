 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _managed-agent-toolsets:

Vendor-managed agents: ``BaseManagedAgentToolset``
==================================================

Cloud vendors now run agents on your behalf: Snowflake Cortex Agents, Amazon
Bedrock AgentCore runtimes, Azure AI Foundry hosted agents, Vertex AI Agent
Engine. Their reasoning loops execute on the vendor's infrastructure, so they
are not something ``AgentOperator`` runs; they are something an Airflow task
*consults*.

:class:`~airflow.providers.common.ai.toolsets.managed_agent.BaseManagedAgentToolset`
is the contract for exposing one of those as a tool. Each provider package
ships its own subclass, so credentials keep flowing through that provider's
existing hook and no new connection types are needed.

A subclass implements two members:

``agent_ref``
    Normalized identity of the remote agent (``platform`` and ``name``), logged
    on every call so a run can be audited for which agents it consulted.

``invoke_sync(prompt)``
    Send the prompt, return the agent's answer. Return the *answer*, not the
    transport envelope.

Tool naming, argument validation, result serialisation, and logging are handled
by the base class, so every provider's implementation presents the same surface
to the calling model.

``tool_name`` is the required identifier: it is what the model emits when it
calls the tool, and the Dag author chooses it. ``description`` is optional and
falls back to the tool name rendered as prose, the same way ``HookToolset``
derives one from a method name when there is no docstring.

.. note::

    Writing a description is still worth the line. It is what tells the model to
    consult the agent rather than answer from its own knowledge, and it is the
    only place to record a scope limit the name cannot carry, such as "cannot see
    revenue figures". Because the argument schema is always a bare prompt, the
    name and the description are the whole of what the model knows about the
    agent.

Sync or async?
--------------

An agent run is driven by a single event loop, shared by the model requests and
every toolset's tool calls. A call that blocks that loop stalls all of them for
as long as the remote agent reasons -- seconds to minutes, not milliseconds --
and it does so silently, with no error to point at.

Implement ``invoke_sync`` when the vendor call is blocking. The base class runs
it in a worker thread, keeping the loop free::

    class MyManagedAgentToolset(BaseManagedAgentToolset):
        @property
        def agent_ref(self) -> dict[str, str]:
            return {"platform": "example.cloud", "name": self._agent_id}

        def invoke_sync(self, prompt: str) -> str:
            return self._hook.conn.invoke(agent=self._agent_id, prompt=prompt)

Override ``invoke(prompt)`` -- the async coroutine -- when the call is already
asynchronous. Implement one hook or the other; a subclass that implements
neither is rejected when it is constructed.

.. warning::

    A thread cannot be cancelled. A caller that stops waiting for
    ``invoke_sync`` does not stop the call -- it keeps a worker thread and its
    socket until the call returns. Set a timeout on the underlying request so
    that is bounded.

Toolset or operator?
--------------------

Most managed-agent platforms do not offer a plain one-request-one-answer API. Some
require polling a job; others require creating a session and tearing it down around
each exchange. A toolset can do either, but only by blocking inside
``invoke()``: it cannot defer to the Triggerer, and it has no post-task hook to
clean up with if the worker dies mid-call.

That draws a boundary worth respecting:

.. list-table::
    :header-rows: 1
    :widths: 45 55

    * - Shape
      - Surface to use
    * - A short consultation *inside* an agent's reasoning, where failing the task
        would discard the calling agent's accumulated context
      - A managed agent toolset
    * - Long-running submitted work as a pipeline step in its own right
      - That provider's own operator, with deferral or
        :class:`~airflow.sdk.bases.resumablejobmixin.ResumableJobMixin`

``ResumableJobMixin`` exists for exactly the second case: it persists the external
job ID to the task state store before polling, so a worker crash reconnects to the
running job instead of submitting a duplicate. A toolset cannot offer that, because
the retry boundary is the task, not the tool call: on retry the agent loop restarts
and re-issues the call. Durable execution covers the *completed* call (see
``replayable`` below); it does not cover a call that was still in flight.

Error handling
--------------

Failures sort into three buckets, and conflating them is the most common way an
implementation goes wrong:

.. list-table::
    :header-rows: 1
    :widths: 22 30 48

    * - Raise
      - When
      - Who recovers
    * - ``ModelRetry``
      - The agent rejected the request in a way rephrasing could fix.
      - The calling model, bounded by its ``usage_limits``.
    * - ``ManagedAgentInvocationError``
      - Terminal: bad credentials, missing agent, revoked quota.
      - Nobody: the task fails fast instead of burning retries.
    * - *let it propagate*
      - Transient: 429, 5xx, connection reset, read timeout.
      - Airflow's task-level retry. A rephrase does nothing for a 503.

Durable execution
-----------------

``replayable`` is ``False`` by default. A managed agent may act on systems
Airflow cannot observe, so replaying a cached answer on retry could skip a side
effect. Implementations whose agent is read-only should set it to ``True`` to
avoid paying for the same invocation twice.

Deferral
--------

A toolset call runs in the worker and cannot defer to the Triggerer: it blocks
for the duration of the call. See `Toolset or operator?`_ above for when that is
acceptable and when the provider's own deferrable operator is the right surface
instead.

Failover between interchangeable agents
---------------------------------------

:class:`~airflow.providers.common.ai.toolsets.managed_agent.FailoverManagedAgentToolset`
composes several managed agents into one tool, trying them in order until one
answers. It is itself a ``BaseManagedAgentToolset``, so the calling model sees a
single tool and has no say in which provider serves the request, so the policy
stays deterministic Python rather than a prompt instruction a model may ignore.
Groups nest.

.. code-block:: python

    from airflow.providers.common.ai.toolsets import FailoverManagedAgentToolset

    resilient = FailoverManagedAgentToolset(
        tool_name="ask_claims_agent",
        description="Reviews an insurance claim and returns a coverage determination.",
        members=[bedrock_claims_agent, foundry_claims_agent],  # same image, two clouds
    )

Members must satisfy two preconditions the class cannot check.

**Substitutability.** The same agent deployed twice, not two specialists with
different data. Two containerised agents built from one image qualify; agents
bound to one platform's own objects (a Cortex Agent over Snowflake semantic
models) do not, because there is nothing equivalent to fail over *to*.

**Statelessness per invocation.** Server-side conversation state is the norm
across managed-agent platforms, not the exception: optional on some (Cortex
``thread_id``), mandatory on others where a session is created and torn down
around each exchange. Each member is invoked with a bare prompt and no thread
reference, so a failover silently starts a fresh conversation on the standby:
correct for a one-shot consultation, wrong for a multi-turn one. Treat one-shot
as a restriction a group is deliberately held to, not a safe default.

The three error buckets do real work here:

- ``ManagedAgentInvocationError`` and transient failures move to the next member.
- ``ModelRetry`` is re-raised immediately and never triggers failover. A prompt
  the primary could not parse will not parse on the standby either, so failing
  over would spend the standby's budget reproducing the same error.
- The last member's exception propagates unchanged, so a total outage still fails
  the task rather than returning something misleading.

``failover_on`` defaults to ``Exception`` because ``common.ai`` cannot enumerate
the cloud SDKs' exception trees: ``requests``, ``botocore`` and the Azure SDK
share no common base. It can be narrowed when the members' exception types are
known.

``replayable`` on a group is ``True`` only when every member is, because the
durable cache cannot know which member produced the answer it holds.

.. note::

    For a **standalone** agent call, prefer plain Airflow task-level failover:
    two tasks, the second with ``trigger_rule=TriggerRule.ALL_FAILED``. That keeps
    which provider served the request visible in the grid at no code cost, and
    makes failover rate a task metric. This class is for the case a task boundary
    cannot express: a managed agent consulted as a tool *inside* a longer agent
    run, where failing the task would discard the calling agent's accumulated
    context and re-run every earlier tool call.

Two counters make failover visible, because a failover is a *success-shaped*
event: without them a primary that has been down for a week looks identical to a
healthy one:

.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Metric
      - Tags
    * - ``managed_agent.failover``
      - ``from_platform``, ``to_platform`` (one per failover transition)
    * - ``managed_agent.served``
      - ``platform`` and ``role`` (``primary`` / ``standby``), one per answer

The standby-served fraction is a ratio over ``managed_agent.served`` alone, so
"are we quietly running on the standby?" is a dashboard question rather than a log
grep. Both are tagged by platform rather than agent name to keep cardinality
bounded.

One limitation remains: which member served a *particular* answer is in the task
log but not in XCom. ``agent_ref`` on a group describes the group, not the
responder, because the responder is not known until after the call. The counters
cover the operational question; per-answer provenance for an audit trail would
need ``AgentOperator`` to collect per-toolset metadata.

When to choose it
-----------------

**Choose it when** the reasoning itself belongs on the vendor's infrastructure:
the agent is already deployed there, grounded in data that never leaves, and
Airflow's job is to submit one request and read one answer.
:ref:`managed-agent-toolsets` covers the shape.

Read the first bullet before planning around this route.

**What it cannot do**

- It is an extension point, not a toolset you can use as-is. This provider ships
  the base class only:
  :class:`~airflow.providers.common.ai.toolsets.managed_agent.BaseManagedAgentToolset`
  declares ``agent_ref`` abstract and raises ``TypeError`` at construction unless
  a subclass implements ``invoke_sync`` or ``invoke``. No vendor subclass exists
  in this repository: the Snowflake Cortex, Bedrock AgentCore, Azure AI Foundry
  and Vertex AI Agent Engine names in its docstring describe the shape it expects,
  not implementations that ship. Using this route means writing that subclass.
- It has no allow-list to offer. The whole toolset is one tool: a prompt goes in,
  an answer comes out. Whatever governs what the remote agent may touch lives on
  the vendor's side, which is the trade you are making.
- It does not define where the credential comes from. The base class leaves
  that decision to the subclass; :ref:`managed-agent-toolsets` frames the
  intended shape as routing authentication through the provider's own hook.
- Durable replay is off by default. ``replayable`` is ``False`` because a managed
  agent may act on systems Airflow cannot observe, so replaying from the cache
  could skip a side effect. Read-only agents can opt in.
- Its failover variant relies on two preconditions the code cannot check.
  :class:`~airflow.providers.common.ai.toolsets.managed_agent.FailoverManagedAgentToolset`
  requires members that are genuinely interchangeable (the same agent deployed
  twice, not two specialists over different data) and one-shot exchanges, because
  each member is invoked with a bare prompt and no thread reference, so a failover
  silently starts a fresh conversation rather than resuming the old one.

**A real example.** There is none. No example Dag, no system test, and the only
code in the documentation is an illustrative subclass on this page. Budget
for writing and testing the subclass yourself.

**Credentials and where it runs.** Both are the subclass's decision. Reasoning
runs on the vendor's infrastructure; the worker sends a request and waits.
