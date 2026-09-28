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

.. _howto/toolsets:

Toolsets
========

Choosing a toolset
------------------

Each toolset's guide documents how to configure it. This section answers the
question that comes before that one: you have a system you want an agent to
reach, so which route do you take, and what does each route give up?

Read the table below by what you already have, not by what a toolset is called.
When two routes both work, the deciding factor is rarely what each one can do.
It is what each one cannot do, and every route has a short list.

More than one row can be true at once, and the rows are not exclusive: one agent
can carry several toolsets. Two questions break the ties. *Whose credential is
it?* Prefer the route whose credential is an Airflow connection somebody on
your side already reviewed. *Whose tool list is it?* Prefer the route whose
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
^^^^^^^^^^^^^^^^^^^^^^^^

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
   * - Files on an object store (Parquet, CSV, Avro) or a catalog-managed
       table format such as Iceberg, rather than rows in a database
     - ``DataFusionToolset``
   * - A vendor that already ships a server built for agents, whose tools you
       would otherwise re-wrap by hand
     - ``MCPToolset``
   * - Procedural knowledge (how to carry out a task) rather than an endpoint
       to call
     - ``AgentSkillsToolset``
   * - Work that means running code the model wrote, not calling a tool you chose
     - ``SandboxToolset``
   * - Reasoning that should happen on the vendor's own infrastructure
     - A subclass of ``BaseManagedAgentToolset`` that you write

The hook, SQL, DataFusion, MCP, Agent Skills and managed-agent guides each have a
*When to choose it* section giving the case for choosing it, what it cannot do, an
example that exists in this repository, and where its credentials and its work come
from. :doc:`../sandbox/index` carries the same section for ``SandboxToolset``.

Toolset guides
--------------

.. toctree::
    :titlesonly:

    Airflow hooks as tools <hook>
    SQL databases <sql>
    Files with DataFusion <datafusion>
    MCP servers <mcp>
    Agent Skills <skills>
    Sandboxed execution <../sandbox/index>
    Vendor-managed agents <managed_agent>
    LangChain tools <langchain>
    Tool call logging <logging>

The toolsets
------------

Airflow's 350+ provider hooks already have typed methods, rich docstrings,
and managed credentials. Toolsets expose them as pydantic-ai tools so that
LLM agents can call them during multi-turn reasoning.

Six toolsets are exported directly from the ``airflow.providers.common.ai.toolsets``
package root:

- :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`: generic
  adapter for any Airflow Hook. Guide: :doc:`hook`.
- :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`: curated
  4-tool database toolset. Guide: :doc:`sql`.
- :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset`: connect to
  `MCP servers <https://modelcontextprotocol.io/>`__ configured via Airflow
  connections. Guide: :doc:`mcp`.
- :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`: give
  the agent a shell and a filesystem inside an isolated sandbox, off the
  Airflow worker. Guide: :doc:`../sandbox/index`.
- :class:`~airflow.providers.common.ai.toolsets.managed_agent.BaseManagedAgentToolset`:
  base class that provider packages subclass to expose a **vendor-managed
  agent**, one whose reasoning loop runs on a cloud provider's infrastructure.
  Guide: :doc:`managed_agent`.
- :class:`~airflow.providers.common.ai.toolsets.managed_agent.FailoverManagedAgentToolset`:
  composes several interchangeable managed agents behind a single tool.
  See :ref:`managed-agent-toolsets`.

Three more toolsets (:doc:`datafusion`, :doc:`logging`, :doc:`skills`) are not
re-exported from the package root, so import each of them from its own submodule::

    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.ai.toolsets.logging import LoggingToolset
    from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset

All of these toolsets implement pydantic-ai's
`AbstractToolset <https://ai.pydantic.dev/toolsets/>`__ interface and can be
passed to any pydantic-ai ``Agent``, including via
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.

.. note::

    ``AgentOperator`` accepts **any** ``AbstractToolset`` implementation, not
    just the Airflow-native toolsets above. pydantic-ai's own ``MCPToolset``
    (built over a FastMCP transport) and third-party toolsets work too. The
    Airflow-native toolsets add connection management, secret backend
    integration, and the connection UI, but you are not locked in.

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
       for ``sbx``; ambient Modal token on the worker for Modal
     - A microVM on the worker host (``sbx``), or Modal's infrastructure off
       the worker (Modal)
   * - ``BaseManagedAgentToolset``
     - Undefined by the base class; the subclass decides
     - The vendor's infrastructure

Two rows are worth pausing on. ``SandboxToolset`` deliberately takes no Airflow
credential; that is the whole point of it, and it substitutes a backend-level
boundary, on the host or at the vendor, for the connection-level one. ``BaseManagedAgentToolset`` does not
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
``AgentSkillsToolset`` define no tools of their own: they pass through whatever
the upstream toolset declares, so the setting is not theirs to make. Do not read
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
offering another way to reach a system; :doc:`logging` and :doc:`langchain` cover
both.

Using toolsets outside ``AgentOperator``
----------------------------------------

Toolsets are standard pydantic-ai ``AbstractToolset`` implementations with no
dependency on ``AgentOperator`` or ``@task.agent``. You can use them anywhere
you can run Python within Airflow -- ``@task`` functions, ``PythonOperator``
callables, or any custom operator's ``execute()`` method -- by creating a
``pydantic_ai.Agent`` yourself:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_hook.py
    :language: python
    :start-after: [START howto_task_with_toolsets]
    :end-before: [END howto_task_with_toolsets]

This works because toolsets resolve Airflow connections lazily via
``BaseHook.get_connection()``, which is available in any task execution
context.

This approach gives you full control over the agent lifecycle -- you can call
``agent.run_sync()`` multiple times, swap models at runtime, or combine
results from several agents in a single task. The tradeoff is that you lose
the durable execution (step-level caching with retry replay), HITL review
integration, and automatic tool call logging that ``AgentOperator`` provides.

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
:doc:`../index` is the dividing line: if Airflow should *run* the AI step, and the
model should stay swappable, use ``common.ai``; if the Dag *submits work to* a
vendor-managed service and waits for the result, use that vendor's provider,
and ``BaseManagedAgentToolset`` exists for the case where you want the second
behaviour from inside an agent that is otherwise doing the first.

See also
--------

- :doc:`../agent_security`: defense layers, ``allowed_tables`` enforcement, ``HookToolset``
  guidelines and the production checklist.
- :doc:`../security`: the provider's security policy.
- :doc:`../examples`: the example Dags referenced above.
