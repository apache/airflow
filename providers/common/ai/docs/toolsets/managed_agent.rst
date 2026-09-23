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

Cloud vendors now run agents on your behalf — Snowflake Cortex Agents, Amazon
Bedrock AgentCore runtimes, Azure AI Foundry hosted agents, Vertex AI Agent
Engine. Their reasoning loops execute on the vendor's infrastructure, so they
are not something ``AgentOperator`` runs; they are something an Airflow task
*consults*.

Two pieces make that consultation vendor-neutral. The **contract**, in
:mod:`airflow.providers.common.ai.managed_agents`, is what a vendor hook
implements: a request goes in, an answer comes out, and the agent is an argument
rather than a hook of its own, the way a statement is an argument to
``DbApiHook.run``. The **toolset**,
:class:`~airflow.providers.common.ai.toolsets.managed_agent.ManagedAgentToolset`,
is what a Dag passes to ``AgentOperator``: it presents any client of that
contract to the calling model as one tool with a bare-prompt schema.

.. code-block:: python

    from airflow.providers.amazon.aws.hooks.bedrock_managed_agent import BedrockAgentCoreManagedAgentHook
    from airflow.providers.common.ai.operators.agent import AgentOperator
    from airflow.providers.common.ai.toolsets import ManagedAgentToolset

    claims = BedrockAgentCoreManagedAgentHook(aws_conn_id="aws_prod", region_name="us-east-1").agent(
        "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/claims"
    )

    AgentOperator(
        task_id="triage",
        llm_conn_id="anthropic_default",
        prompt="Review claim 4411 and decide whether to pay it.",
        toolsets=[
            ManagedAgentToolset(
                claims,
                tool_name="ask_claims_agent",
                description="Reviews an insurance claim and returns a coverage determination.",
            )
        ],
    )

Credentials keep flowing through the vendor's own hook and connection; no new
connection types are involved. The vendor hooks that adopt the contract today are
:class:`~airflow.providers.amazon.aws.hooks.bedrock_managed_agent.BedrockAgentCoreManagedAgentHook`
(install ``apache-airflow-providers-amazon[common.ai]``) and
:class:`~airflow.providers.google.cloud.hooks.vertex_ai.managed_agent.AgentEngineManagedAgentHook`
(install ``apache-airflow-providers-google[common.ai]``). Each provider documents
how it maps the contract onto its service.

``tool_name`` is the required identifier — it is what the model emits when it
calls the tool, and the Dag author chooses it. ``description`` is optional and
falls back to the tool name rendered as prose, the same way ``HookToolset``
derives one from a method name when there is no docstring.

.. note::

    Writing a description is still worth the line. It is what tells the model to
    consult the agent rather than answer from its own knowledge, and it is the
    only place to record a scope limit the name cannot carry — "cannot see
    revenue figures". Because the argument schema is always a bare prompt, the
    name and the description are the whole of what the model knows about the
    agent.

What the model receives
-----------------------

The model receives ``ManagedAgentResponse.text``: the answer, unwrapped from the
vendor's transport envelope by the hook. The envelope itself is on
``ManagedAgentResponse.raw`` for Python callers of the client and never reaches
the model, so a vendor's citations, tool traces or metadata are neither lost nor
pasted into a model's context. A Python caller who wants them calls the client
directly::

    response = claims.invoke(ManagedAgentRequest(prompt="Summarize claim 4411"))
    response.text  # what a model would have seen
    response.raw  # the vendor envelope

``ManagedAgentRequest.vendor_options`` carries anything the contract does not
type through to the vendor call; ``ManagedAgentToolset(vendor_options=...)``
sends the same options on every request, for per-agent settings such as Agent
Engine's ``class_method`` or AgentCore's ``text_key``. Hooks reject options that would re-target the call,
such as another agent identity, account or connection, and refuse a
``session_id`` when the agent keeps no conversation state.

Error handling
--------------

Failures sort into three buckets, and conflating them is the most common way a
vendor adoption goes wrong:

.. list-table::
    :header-rows: 1
    :widths: 22 30 48

    * - Raise
      - When
      - Who recovers
    * - :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected`
      - The agent rejected the request in a way rephrasing could fix.
      - The calling model. The toolset turns it into a pydantic-ai ``ModelRetry``,
        bounded by ``max_retries`` (one rephrase by default; ``0`` makes it fatal).
    * - :class:`~airflow.providers.common.ai.exceptions.ManagedAgentInvocationError`
      - Terminal: bad credentials, missing agent, malformed request, exhausted quota.
      - No rephrase; a failover group moves to its next member. Airflow's own task
        retries still apply, so pair the task with a retry rule that stops on it if
        retrying would only repeat the failure.
    * - *let it propagate*
      - Transient: 429, 5xx, connection reset, read timeout.
      - Airflow's task-level retry. A rephrase does nothing for a 503.

A vendor hook raises :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected`
rather than ``ModelRetry`` so that the contract module imports nothing from
pydantic-ai; the toolset is the one place a rejection becomes something the
calling model can act on. Neither platform adopted here has a rephrase-class
error. AgentCore reports a container's own complaints inside a successful body,
and Agent Engine's ``INVALID_ARGUMENT`` means an author-side mistake such as a
wrong input key, so both hooks raise terminal errors or let transient ones
propagate.

Toolset or operator?
--------------------

A toolset call runs in the worker and cannot defer to the Triggerer — it blocks
for the duration of the call, in a worker thread so the agent's event loop keeps
running. Some managed-agent platforms are built around a long-running job or a
session rather than one request and one answer, and a toolset serves those
poorly.

.. list-table::
    :header-rows: 1
    :widths: 45 55

    * - Shape
      - Surface to use
    * - A short consultation *inside* an agent's reasoning, where failing the task
        would discard the calling agent's accumulated context
      - :class:`~airflow.providers.common.ai.toolsets.managed_agent.ManagedAgentToolset`
    * - Long-running submitted work as a pipeline step in its own right
      - That provider's own operator, with deferral or
        :class:`~airflow.sdk.bases.resumablejobmixin.ResumableJobMixin`; for
        Agent Engine query jobs, ``RunQueryJobOperator``

``ResumableJobMixin`` exists for exactly the second case: it persists the external
job ID to the task state store before polling, so a worker crash reconnects to the
running job instead of submitting a duplicate. A toolset cannot offer that, because
the retry boundary is the task, not the tool call. Set ``timeout`` on the toolset
so a call that stops being waited for is bounded: a thread cannot be cancelled,
and the hooks apply the request's timeout to the vendor call itself.

Durable execution
-----------------

``replayable`` is ``False`` by default, and ``AgentOperator(durable=True)``
honors it: a managed-agent call is re-invoked on retry rather than served from
the step cache, because the agent may have acted on systems Airflow cannot
observe and replaying a cached answer could skip a side effect. Set
``replayable=True`` only for an agent that is read-only.

Failover between interchangeable agents
---------------------------------------

:class:`~airflow.providers.common.ai.managed_agents.failover.FailoverManagedAgentClient`
composes several clients into one, trying them in order until one answers. It is
itself a client, so a toolset over it presents a single tool and the calling
model has no say in which provider serves the request — the policy stays
deterministic Python rather than a prompt instruction a model may ignore. Groups
nest.

.. code-block:: python

    from airflow.providers.common.ai.managed_agents import FailoverManagedAgentClient
    from airflow.providers.common.ai.toolsets import ManagedAgentToolset

    resilient = ManagedAgentToolset(
        FailoverManagedAgentClient([bedrock_claims, vertex_claims]),  # same agent, two clouds
        tool_name="ask_claims_agent",
        description="Reviews an insurance claim and returns a coverage determination.",
    )

Members must be **substitutable**: the same agent deployed twice, not two
specialists with different data. Two containerized agents built from one image
qualify; an agent bound to one platform's own objects — a Cortex Agent over
Snowflake semantic models — does not, because there is nothing equivalent to fail
over *to*. The group cannot check this.

What it can check is **conversation state**. A failover starts a fresh conversation
on the standby, which is correct for a one-shot consultation and wrong for a
multi-turn one. The group therefore never reports ``capabilities.sessions`` and
refuses a request that carries a ``session_id``, whatever its members support;
a conversation belongs to one member, addressed directly.

The three error buckets do real work here:

- :class:`~airflow.providers.common.ai.exceptions.ManagedAgentInvocationError`
  and transient failures move to the next member.
- :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected` is
  re-raised immediately and never triggers failover. A prompt the primary could
  not parse will not parse on the standby either.
- The last member's exception propagates unchanged, so a total outage still fails
  the task rather than returning something misleading.

``failover_on`` defaults to ``Exception`` because ``common.ai`` cannot enumerate
the cloud SDKs' exception trees — ``requests``, ``botocore`` and the Google SDK
share no common base. It can be narrowed when the members' exception types are
known.

.. note::

    For a **standalone** agent call, prefer plain Airflow task-level failover:
    two tasks, the second with ``trigger_rule=TriggerRule.ALL_FAILED``. That keeps
    which provider served the request visible in the grid at no code cost, and
    makes failover rate a task metric. This class is for the case a task boundary
    cannot express — a managed agent consulted as a tool *inside* a longer agent
    run, where failing the task would discard the calling agent's accumulated
    context and re-run every earlier tool call.

Two counters make failover visible, because a failover is a *success-shaped*
event — without them a primary that has been down for a week looks identical to a
healthy one:

.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Metric
      - Tags
    * - ``managed_agent.failover``
      - ``from_platform``, ``to_platform`` — one per failover transition, emitted by
        the group
    * - ``managed_agent.served``
      - ``tool``, ``platform`` — one per answer, emitted by the toolset; a toolset
        over a group reports ``platform=failover``, and either counter reports
        ``unknown`` when an agent's identity could not be resolved

``managed_agent.failover`` rising while ``managed_agent.served`` stays flat means
the primary is down, and that is a dashboard question rather than a log grep. The
counters do not say which member answered: that is in the task log, where each
failover warning names the members involved and their positions, but not in XCom.
Both counters are tagged by platform rather than agent name to keep cardinality
bounded.

Implementing the contract for a new vendor
------------------------------------------

A vendor hook adopts
:class:`~airflow.providers.common.ai.managed_agents.contract.BaseManagedAgentHook`
as a mixin beside its own base and implements three methods: ``resolve_agent``
(normalize the agent identifier into a platform-qualified reference, without a
network call), ``agent_capabilities`` (what the pair can do, so consumers can
refuse rather than degrade) and ``invoke_agent`` (send a request, return an
answer, sort failures into the three buckets). ``hook.agent(...)`` then returns a
bound client the toolset accepts.

Because ``common.ai`` requires Airflow 3 and most vendor providers still support
Airflow 2, the adoption lives in a module of its own whose import of the contract
is guarded, and the provider declares ``common.ai`` as an optional extra. That is
the arrangement the Amazon provider already uses for ``common.messaging``'s
``BaseMessageQueueProvider``; the Amazon and Google adoptions in this release are
the templates.

The only toolset most code needs is ``ManagedAgentToolset``. Subclass
:class:`~airflow.providers.common.ai.toolsets.managed_agent.BaseManagedAgentToolset`
directly only for an agent that has no hook at all; it takes an ``agent_ref`` and
an ``invoke_sync`` (or an async ``invoke``) and supplies the same tool surface.
A thread cannot be cancelled, so an ``invoke_sync`` must set a timeout on its own
request.

When to choose it
-----------------

**Choose it when** the reasoning itself belongs on the vendor's infrastructure —
the agent is already deployed there, grounded in data that never leaves, and
Airflow's job is to submit one request and read one answer.

**What it cannot do**

- It has no allow-list to offer. The whole toolset is one tool: a prompt goes in,
  an answer comes out. Whatever governs what the remote agent may touch lives on
  the vendor's side, which is the trade you are making.
- It cannot defer. The call blocks a worker thread for as long as the remote agent
  reasons. Long-running submitted work belongs in the vendor's own operator.
- Durable replay is off by default. ``replayable`` is ``False`` because a managed
  agent may act on systems Airflow cannot observe, so replaying from the cache
  could skip a side effect. Read-only agents can opt in.
- Its failover variant,
  :class:`~airflow.providers.common.ai.managed_agents.failover.FailoverManagedAgentClient`,
  requires members that are genuinely interchangeable — the same agent deployed
  twice, not two specialists over different data. It refuses a request that
  carries a ``session_id``, because a failover starts a fresh conversation on the
  standby.

**Which vendors.** Amazon Bedrock AgentCore through
:class:`~airflow.providers.amazon.aws.hooks.bedrock_managed_agent.BedrockAgentCoreManagedAgentHook`
and Vertex AI Agent Engine through
:class:`~airflow.providers.google.cloud.hooks.vertex_ai.managed_agent.AgentEngineManagedAgentHook`,
each behind that provider's ``common.ai`` extra. Other vendors adopt the same
contract; `Implementing the contract for a new vendor`_ describes how.

**Credentials and where it runs.** Credentials flow through the vendor hook's own
connection. Reasoning runs on the vendor's infrastructure; the worker sends a
request and waits.
