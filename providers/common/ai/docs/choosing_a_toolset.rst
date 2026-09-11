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

.. _howto/choosing-a-toolset:

Choosing a Toolset
==================

:doc:`toolsets` documents how to configure each toolset. This page answers the
question that comes before that one: you have a system you want an agent to
reach, so which route do you take, and what does each route give up?

Read the table below by what you already have, not by what a toolset is called.
When two routes both work, the deciding factor is rarely what each one can do —
it is what each one cannot do, and every route has a short list.

More than one row can be true at once, and the rows are not exclusive: one agent
can carry several toolsets. Two questions break the ties. *Whose credential is
it?* — prefer the route whose credential is an Airflow connection somebody on
your side already reviewed. *Whose tool list is it?* — prefer the route whose
exposed surface you chose rather than inherited. The pair that most often
overlaps is an Airflow hook and a vendor MCP server reaching the same target;
both questions point at the hook, because its credential is the connection and
``allowed_methods`` is a list you write. Reach for the server when its tools
cover work the hook does not expose, or when the alternative is re-wrapping that
API by hand.

Those two questions do not separate ``HookToolset`` from ``SQLToolset`` when the
target is a DBAPI database, because both answer them the same way. A third one
does: *is the work a fixed operation or an open-ended question?* A named method
you can enumerate in advance is a hook. A question the agent has to express as
SQL is a query, and ``SQLToolset`` answers it with schema discovery, bounded
results and an ``allowed_tables`` walk you can switch on, none of which
``HookToolset`` has an equivalent of.

Start with what you have
------------------------

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - What you have
     - Route
   * - A target that already has an Airflow connection, and a hook method that
       already does the thing
     - ``HookToolset``
   * - A question that is a query, against a DBAPI database
     - ``SQLToolset``
   * - Files on an object store — Parquet, CSV, Avro, Iceberg — rather than rows
       in a database
     - ``DataFusionToolset``
   * - A vendor that already ships a server built for agents, whose tools you
       would otherwise re-wrap by hand
     - ``MCPToolset``
   * - Procedural knowledge — how to carry out a task — rather than an endpoint
       to call
     - ``AgentSkillsToolset``
   * - Work that means running code the model wrote, not calling a tool you chose
     - ``SandboxToolset``
   * - Reasoning that should happen on the vendor's own infrastructure
     - A subclass of ``BaseManagedAgentToolset`` that you write

The rest of this page takes those seven in turn. Each entry gives the case for
choosing it, what it cannot do, an example that exists in this repository, and
where its credentials and its work come from.

``HookToolset``
---------------

**Choose it when** the target already has an Airflow connection and a hook, and
what you want the agent to do is already a method on that hook. This is the
cheapest route — no new server, no new credential, no new query dialect — and
the only one that reaches any provider hook with synchronous methods without
anyone writing an adapter first. :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset` is a
reflection-based adapter, so the work is choosing the method list.

**What it cannot do**

- It allow-lists method *names*, not arguments. Once ``read_key`` is exposed,
  the agent picks the key; the :ref:`defense-layer table <toolset-defense-layers>`
  states this outright. Choose methods whose worst case you accept, not methods
  you intend to constrain later.
- Its calls act as barriers. The tools are registered with ``sequential=True``
  because hook methods perform synchronous I/O, so a slow call holds up every
  other tool the model emitted in that step, not only this toolset's. This is
  not specific to ``HookToolset`` — see :ref:`toolset-call-barriers`.
- It returns exactly one shape. Every result goes through ``serialize_for_llm``
  and comes back as a JSON-encoded string; there is no structured error type and
  no ``ModelRetry`` wrapper, so a hook exception fails the agent run, and the
  task with it, instead of giving the model something it can correct.
  ``SQLToolset``, by contrast, hands the database's own error back as a retry.
- Its ``call_tool`` calls the method and serializes what comes back. The code
  contains no path that awaits a coroutine result, and none that checks for one,
  so an ``async def`` hook method is not a case this adapter is written to
  handle. Treat synchronous methods as the supported set.

**A real example.** The read-only S3 pair from the ``HookToolset`` guidance in
:doc:`toolsets`:

.. code-block:: python

    HookToolset(
        s3_hook,
        allowed_methods=["list_keys", "read_key"],
        tool_name_prefix="s3_",
    )

**Credentials and where it runs.** The hook instance is yours, so the credential
is whatever connection that hook resolves — the toolset never looks one up
itself. Calls run in the Airflow worker process.

``SQLToolset``
--------------

**Choose it when** the question is a query and the data is in a DBAPI database.
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` gives the agent
four tools — list tables, get schema, query, check query. Set ``allowed_tables``
and that allow-list is enforced by parsing the SQL rather than by matching
strings; see :ref:`allowed-tables-enforcement` for how the walk handles CTEs,
subqueries and joins.

**What it cannot do**

- ``allowed_tables`` is an application-level guardrail, not a replacement for
  database permissions. Its own docstring says so, and names the residual gap:
  an engine or query the parser reads differently. Point ``db_conn_id`` at a
  least-privilege role whose grants match the allow-list.
- It cannot bound the fetch for every driver. Hooks that hand their handler
  something other than a DBAPI cursor — ``ExasolHook`` and its pyexasol
  statement, for instance — fall back to a full fetch. The payload handed to the
  model is still bounded; the transfer is not. See :ref:`bounded-query-results`.
- Its parser-level closure is opt-in, not the default. ``allowed_tables``
  defaults to ``None`` and the table walk returns immediately while it is unset,
  so out of the box the agent reaches every table the connection can see.
  ``DESCRIBE`` and ``SHOW`` pass as well, on dialects that parse them, because
  read-only metadata statements are allowed deliberately. Set ``allowed_tables``
  and the walk turns fail-closed: dynamic SQL, the ``TABLE <name>`` shorthand and
  any function sqlglot does not recognize are rejected rather than allowed, so a
  legitimate bespoke function needs naming in ``allowed_functions``. Statements
  that modify data, ``COPY`` among them, are rejected either way while
  ``allow_writes`` is ``False``.
- It does not classify failures. Every exception from a tool becomes one
  ``ModelRetry``, so a connection error and a typo in a column name are treated
  the same way until the retry budget runs out and the task fails for Airflow to
  retry.

**A real example.** ``example_pydantic_ai_hook.py`` builds an agent around
``SQLToolset`` inside a plain ``@task`` function, with no operator involved:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_hook.py
    :language: python
    :start-after: [START howto_task_with_toolsets]
    :end-before: [END howto_task_with_toolsets]

**Credentials and where it runs.** ``db_conn_id`` is resolved through
``BaseHook.get_connection``, and the connection must supply a ``DbApiHook``.
Queries run from the Airflow worker process against the database. Its tool
calls act as barriers, as they do for the other routes that build their own
tools; see :ref:`toolset-call-barriers`.

``DataFusionToolset``
---------------------

**Choose it when** the data is files on an object store rather than rows in a
database — Parquet, CSV, Avro or Iceberg — and you want the agent to ask SQL
questions of them without loading them anywhere first. Each
``DataSourceConfig`` registers one table, and several can be registered so the
agent can join across them.

**What it cannot do**

- It has no table allow-list. ``allow_writes=False`` is the only guard, and it
  blocks non-SELECT statements, not reach: the defense-layer table records that
  this toolset "does not prevent the agent from reading any registered data
  source". The registration list is therefore the whole boundary — register
  exactly what the agent may read.
- It bounds the payload, not the scan. DataFusion has already materialized the
  full result before ``max_rows`` and ``max_result_bytes`` apply, so those limits
  protect the model's context, not the cost of the query.
- It cannot tell failure kinds apart precisely. The DataFusion Python bindings
  expose no native exception types, so the retry decision is made by matching the
  error message against regular expressions — which a wording change upstream can
  quietly defeat.

**A real example.** The same bucket as the ``HookToolset`` entry above, reached
the other way. Rather than exposing ``list_keys`` and ``read_key`` and leaving
the agent to reassemble files, this registers the prefix as a table and lets it
write SQL:

.. code-block:: python

    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.sql.config import DataSourceConfig

    toolset = DataFusionToolset(
        datasource_configs=[
            DataSourceConfig(
                conn_id="aws_default",
                table_name="sales",
                uri="s3://my-bucket/data/sales/",
                format="parquet",
            ),
        ],
        max_rows=100,
    )

Which of the two fits depends on the question. "Read me this object" is a hook
method. "What were last quarter's returns by region" is a query, and expressing
it through ``list_keys`` and ``read_key`` means the model does the aggregation in
its context window instead of the engine doing it.

**Credentials and where it runs.** Each ``DataSourceConfig`` carries its own
``conn_id``, so object-store access is an Airflow connection. DataFusion is an
embedded engine: the query runs inside the worker process, not on a remote
cluster. Its tool calls act as barriers, as they do for the other routes that
build their own tools; see :ref:`toolset-call-barriers`.

``MCPToolset``
--------------

**Choose it when** someone already publishes a server built for agents that
covers your target. You inherit a tool surface that was designed to be called by
a model — retry semantics and error wording are decided upstream, and a
destructive tool can simply be absent — instead of maintaining a per-API wrapper
yourself.

**What it cannot do**

- It has no tool-level allow-list at all.
  :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` forwards
  ``get_tools`` and ``call_tool`` straight to the underlying server, so whatever
  the server exposes, the agent gets. ``tool_prefix`` renames; it does not
  filter. The defense-layer table is explicit that a server can expose shell,
  filesystem or network access.
- It cannot guarantee the credential came from a connection. ``mcp_conn_id`` is
  the default path, but ``token_provider`` and ``env_provider`` are your own
  callables and are free to read an environment variable, a file, or an entirely
  different secret store. That is the point of them — it also means the
  connection is no longer the whole story for anyone auditing the Dag.
- ``stdio`` transport is not isolation. It starts a child process on the worker
  host. It is not a sandbox, and when no environment is supplied the child
  inherits a small allowlist of variables rather than the full parent
  environment — so it is both less contained and less predictable than it looks.

**A real example.** ``example_mcp.py`` drives setup from a connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_mcp.py
    :language: python
    :start-after: [START howto_toolset_mcp_connection]
    :end-before: [END howto_toolset_mcp_connection]

and puts several servers on one agent, prefixed so their tool names stay apart:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_mcp.py
    :language: python
    :start-after: [START howto_toolset_mcp_multiple]
    :end-before: [END howto_toolset_mcp_multiple]

No MCP server for object storage ships with this provider. If one exists for
your target, it is a third route alongside the two above — and the two questions
from the start of this page settle it. Its tool list is the server's rather than
yours, and its token comes from wherever ``mcp_conn_id`` or your own callable
says. So where a hook already reaches the same target, the hook wins; the server
wins where it covers work the hook does not expose.

**Credentials and where it runs.** ``mcp_conn_id`` supplies host, credentials and
transport, unless a provider callable overrides that. ``http`` and ``sse`` reach
a remote server; ``stdio`` runs a child process on the worker host.

``AgentSkillsToolset``
----------------------

**Choose it when** what the agent is missing is procedural knowledge rather than
an endpoint — how this team writes a report, which checks run before a release,
what the house conventions are. A skill is a directory of instructions and
optional scripts, and
:class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset` makes it
discoverable. See :ref:`agent-skills` for the layout.

**What it cannot do**

- ``exclude_resources`` does not hide a file from the skill's own scripts. It
  keeps matches out of resource discovery and out of ``read_skill_resource``,
  and the parameter's documentation says plainly that it does not stop
  ``run_skill_script`` from reading them off disk. For genuinely sensitive files,
  pair it with ``exclude_tools={"run_skill_script"}``. (The parameter needs
  ``pydantic-ai-skills>=1.2.0``, which the ``skills`` extra already pins.)
- It does not move script execution anywhere safer. The toolset's own wording for
  ``exclude_tools`` calls ``run_skill_script`` "on-worker script execution", and
  nothing in this toolset routes those scripts into a sandbox — so unless you
  exclude the tool, a skill's scripts run in the worker process with the worker's
  reach. If that is not acceptable, exclude the tool or put the work behind
  ``SandboxToolset`` instead.
- It re-fetches a Git source on every run. ``GitSkills`` is resolved and
  shallow-cloned on the worker when the run starts, and the checkout is deleted
  when it ends; nothing is kept between runs, so a large or slow repository pays
  that clone once per run. A local directory is read in place and
  costs nothing.

**A real example.** ``example_agent_skills.py`` loads skills from a local
directory:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_local]
    :end-before: [END howto_operator_agent_skills_local]

The two skills it ships, ``aip-tracker`` and ``sql-reporting``, are procedural by
nature: neither adds an endpoint the agent could not already reach. That is the
signal you are on the right route.

**Credentials and where it runs.** A local directory needs no credential. A
private repository goes through
:class:`~airflow.providers.common.ai.skills.GitSkills` and its ``conn_id``,
resolved by the Git provider's ``GitHook``; plain ``http://`` is refused when a
``conn_id`` is set, so a credential is never sent in the clear. Cloning, reading
and any script execution happen on the worker.

``SandboxToolset``
------------------

**Choose it when** the work is running code the model wrote, rather than calling
a tool you picked in advance — exploratory analysis, installing a package for one
task, producing a file. Every other route on this page answers "call this thing";
this one answers "here is somewhere to work".

**What it cannot do**

- The only backend that ships is not built for production. ``SbxSandboxBackend``
  drives Docker Sandboxes, and its own documentation in this provider says to use
  it for local development, calling a worker-driven run "off-label use". It wants
  the ``sbx`` binary on the host, an authenticated Docker account, a one-time
  ``sbx policy init``, and on Linux KVM or nested virtualization — which an
  unprivileged container cannot provide. Anything beyond local development means
  writing another backend behind
  :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`.
- It does not contain the agent. Only what these tools do runs in the sandbox;
  the agent loop, the model calls, and every other toolset on the same agent stay
  in the worker with the worker's credentials. It contains model-written code, so
  pairing it with a credential-bearing toolset on the same agent puts the
  credential back within reach. :ref:`sandbox-boundaries` sets this out in full.
- It cannot apply a per-sandbox network rule on the backend that ships today.
  ``sbx`` governs egress through a host-level ``sbx policy``, and the backend says
  so rather than letting a Dag author believe a per-sandbox restriction is in
  force.
- It does not guarantee reclamation. A failed teardown is logged as a warning
  rather than raised, deliberately, so that a teardown blip cannot fail a
  finished run. Nothing else picks up the slack: the backend that ships has no
  server-side TTL to fall back on, so a worker killed outright leaves the microVM
  and its workspace directory behind. They are named ``airflow-sandbox-*`` so an
  operator can find and remove them — budget for that sweep.

**A real example.** ``tests/system/common/ai/example_sandbox_toolset_sbx.py``
in this provider is a runnable Dag against a real ``sbx`` host — the only
toolset here with a system test. It is reachable from the System Tests entry in
the sidebar.

**Credentials and where it runs.** This is the one route that does not end at an
Airflow connection. Airflow puts none of its context, connections, variables or
worker environment into the sandbox; only what you pass through
:class:`~airflow.providers.common.ai.sandbox.SandboxSpec` goes in. Authorization
is host-level instead — ``sbx login`` and ``sbx policy init``, performed on the
machine, outside anything Airflow can see. Work runs in a per-session microVM
on the worker host. Its tool calls act as barriers, as they do for the other
routes that build their own tools; see :ref:`toolset-call-barriers`.

``BaseManagedAgentToolset``
---------------------------

**Choose it when** the reasoning itself belongs on the vendor's infrastructure —
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
  in this repository — the Snowflake Cortex, Bedrock AgentCore, Azure AI Foundry
  and Vertex AI Agent Engine names in its docstring describe the shape it expects,
  not implementations that ship. Using this route means writing that subclass.
- It has no allow-list to offer. The whole toolset is one tool: a prompt goes in,
  an answer comes out. Whatever governs what the remote agent may touch lives on
  the vendor's side, which is the trade you are making.
- It does not define where the credential comes from. The base class leaves
  authentication to the subclass, so unlike every other route here, nothing in
  this provider guarantees an Airflow connection is involved.
- It is not replayable by default. ``replayable`` is ``False`` because a managed
  agent may act on systems Airflow cannot observe, so a durable replay could skip
  a side effect. Read-only agents can opt in.
- Its failover variant relies on two preconditions the code cannot check.
  :class:`~airflow.providers.common.ai.toolsets.managed_agent.FailoverManagedAgentToolset`
  requires members that are genuinely interchangeable — the same agent deployed
  twice, not two specialists over different data — and one-shot exchanges, because
  each member is invoked with a bare prompt and no thread reference, so a failover
  silently starts a fresh conversation rather than resuming the old one.

**A real example.** There is none. No example Dag, no system test, and the only
code in the documentation is an illustrative subclass in :doc:`toolsets`. Budget
for writing and testing the subclass yourself.

**Credentials and where it runs.** Both are the subclass's decision. Reasoning
runs on the vendor's infrastructure; the worker sends a request and waits.

Where the credentials come from
-------------------------------

Airflow connections and hooks are the access-governance machinery this provider
already has: a connection lives in a secret backend rather than in Dag code, and
its scope is set outside the Dag. A hook turns that scope into typed methods.
Where a route ends up getting its credential is therefore a decision worth
making on purpose rather than inheriting.

.. list-table::
   :widths: 28 44 28
   :header-rows: 1

   * - Route
     - Credential source
     - Where the tool call runs
   * - ``HookToolset``
     - Whatever connection the hook you supply resolves
     - Worker process
   * - ``SQLToolset``
     - ``db_conn_id``, via ``BaseHook.get_connection``
     - Worker process, against the database
   * - ``DataFusionToolset``
     - ``conn_id`` on each ``DataSourceConfig``
     - Worker process (embedded engine)
   * - ``MCPToolset``
     - ``mcp_conn_id``, unless ``token_provider`` or ``env_provider`` supplies
       one from elsewhere
     - Remote server, or a child process on the worker host for ``stdio``
   * - ``AgentSkillsToolset``
     - ``GitSkills.conn_id`` for private repositories; none for local directories
     - Worker process
   * - ``SandboxToolset``
     - None from Airflow. Host-level ``sbx login`` and ``sbx policy init``
     - A microVM on the worker host
   * - ``BaseManagedAgentToolset``
     - Undefined by the base class; the subclass decides
     - The vendor's infrastructure

Two rows are worth pausing on. ``SandboxToolset`` deliberately takes no Airflow
credential — that is the whole point of it, and it substitutes a host-level
boundary for the connection-level one. ``BaseManagedAgentToolset`` does not
substitute anything; it simply leaves the question to whoever writes the
subclass. Neither is a defect, but in both cases the access decision has moved
somewhere Airflow cannot see it, and somebody has to make that decision again in
the new place. The :ref:`defense-layer table <toolset-defense-layers>` is the
right companion when you do.

.. _toolset-call-barriers:

Tool calls as barriers
----------------------

One behaviour cuts across these routes rather than telling them apart.
``HookToolset``, ``SQLToolset``, ``DataFusionToolset`` and ``SandboxToolset``
each build their own tool definitions and set ``sequential=True`` on them, which
pydantic-ai treats as a barrier: the tool runs alone, tools the model emitted
before it finish first, and tools emitted after it start only once it returns.
A slow call on any of those four therefore holds up the rest of that step, not
just its own toolset. ``BaseManagedAgentToolset`` sets ``sequential=False``
deliberately, because the wait it introduces is remote. ``MCPToolset`` and
``AgentSkillsToolset`` define no tools of their own — they pass through whatever
the upstream toolset declares — so the setting is not theirs to make. Do not read
this as a reason to choose one route over another; read it as something to expect
from all four.

Layering
--------

:class:`~airflow.providers.common.ai.toolsets.logging.LoggingToolset` and the
durable-execution ``CachingToolset`` are not alternatives to anything above. Both
are wrappers: they take a toolset and return a toolset, adding per-call logging
or replay from a durable cache. Choose a route first, then decide whether to wrap
it. The same applies to
:func:`~airflow.providers.common.ai.toolsets.langchain_bridge.airflow_toolset_to_langchain_tools`,
which converts a chosen toolset for a different agent framework rather than
offering another way to reach a system; :doc:`toolsets` covers both.

Running the agent outside the data system
-----------------------------------------

A recurring alternative to everything on this page is to push the reasoning into
the system that holds the data and let it run there. The trade is worth stating
plainly, because it is the same trade the last row of the table makes.

Running the agent where the data lives keeps the loop short, and it is the right
answer when the work never leaves that system. What it costs is coupling: the
agent can only reason over what that system holds, its credentials are governed
by that system's model rather than Airflow's, and failures are that system's to
explain. Running the agent in the worker instead means the model calls, the agent
loop and every tool are orchestrated by Airflow, where they get retries, logging
and observability like any other task. It also means one agent can hold a
database, an object store and a vendor API in the same run, with each route's
credential resolved through a connection that was reviewed once.

This provider does not treat either as the default. The rule of thumb in
:doc:`index` is the dividing line: if Airflow should *run* the AI step, and the
model should stay swappable, use ``common.ai``; if the Dag *submits work to* a
vendor-managed service and waits for the result, use that vendor's provider —
and ``BaseManagedAgentToolset`` exists for the case where you want the second
behaviour from inside an agent that is otherwise doing the first.

Next
----

- :doc:`toolsets` — how to configure each toolset, and the full
  :ref:`defense-layer table <toolset-defense-layers>`.
- :doc:`security` — the provider's security guidance.
- :doc:`examples` — the example Dags referenced above.
