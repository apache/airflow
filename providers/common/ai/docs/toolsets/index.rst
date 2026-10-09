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

A toolset is a group of tools an agent may call. Pass toolsets to
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator` or
``@task.agent`` in ``toolsets=[...]``. The model sees each tool's name, description
and arguments, and what each call returns. The connection a toolset authenticates
with is resolved in the worker and is not part of any of that.

.. list-table::
   :widths: 22 40 38
   :header-rows: 1

   * - Toolset
     - What the agent gets
     - What you limit it with
   * - :doc:`HookToolset <hook>`
     - The methods you list from any Airflow hook, one tool per method
     - ``allowed_methods`` (required), ``pinned_arguments``
   * - :doc:`SQLToolset <sql>`
     - ``list_tables``, ``get_schema``, ``query`` and ``check_query`` against a DBAPI
       database
     - ``allowed_tables``, ``allow_writes`` (off by default), ``max_rows``,
       ``max_result_bytes``
   * - :doc:`ObjectStorageToolset <object_storage>`
     - ``list_files``, ``get_file_info`` and ``read_file`` under one object-storage path.
       It cannot write.
     - ``path``, the root every requested path is checked against;
       ``max_read_bytes``, ``max_output_bytes``
   * - :doc:`DataFusionToolset <datafusion>`
     - SQL over Parquet, CSV and Avro files and Iceberg tables, run in the worker
     - The tables you register in ``datasource_configs``, ``allow_writes`` (off by
       default), ``max_rows``
   * - :doc:`MCPToolset <mcp>`
     - Every tool the MCP server behind ``mcp_conn_id`` exposes
     - ``.filtered()`` to offer only some of them
   * - :doc:`AgentSkillsToolset <skills>`
     - ``SKILL.md`` instruction bundles that the model loads when it needs one
     - ``exclude_tools={"run_skill_script"}`` to stop skill scripts running on the
       worker; ``exclude_resources``
   * - :doc:`SandboxToolset <../sandbox/index>`
     - A shell and a filesystem in a sandbox isolated from the worker process
     - ``SandboxSpec`` (``block_network`` is on by default, ``allow_egress_to``),
       command timeouts
   * - :doc:`ManagedAgentToolset <managed_agent>`
     - One tool that sends a prompt to an agent running on a cloud vendor's
       infrastructure
     - ``timeout`` for each call; what the remote agent may touch is set at the
       vendor

Two controls work on every toolset. ``.approval_required()`` pauses the task before a
matching call runs, until a person approves or rejects it on the **Required Actions**
page. It needs Airflow 3.3 or later, pauses a task instance at most once per Dag run,
and does not combine with ``durable=True`` or a ``SandboxToolset`` that provisions its
own sandbox (:doc:`../tool_approval` lists every limit). ``.filtered()`` drops tools
from what the model is offered, as the :ref:`MCP guide <howto/toolset:mcp-filtered>`
shows.

:doc:`logging` and :doc:`langchain` do not reach a system of their own: one wraps a
toolset to log its calls, the other converts a toolset for a LangChain agent.

.. toctree::
    :titlesonly:
    :hidden:

    Airflow hooks as tools <hook>
    SQL databases <sql>
    Files on object storage <object_storage>
    Files with DataFusion <datafusion>
    MCP servers <mcp>
    Agent Skills <skills>
    Sandboxed execution <../sandbox/index>
    Vendor-managed agents <managed_agent>
    LangChain tools <langchain>
    Tool call logging <logging>

Importing toolsets
------------------

Six toolsets import from the ``airflow.providers.common.ai.toolsets`` package root:
``HookToolset``, ``SQLToolset``, ``ObjectStorageToolset``, ``MCPToolset``,
``SandboxToolset`` and ``ManagedAgentToolset``. Import the other three from their own
submodules::

    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.ai.toolsets.logging import LoggingToolset
    from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset

Every toolset here implements pydantic-ai's
`AbstractToolset <https://ai.pydantic.dev/toolsets/>`__ interface, so it works in any
pydantic-ai ``Agent``. ``AgentOperator`` accepts any ``AbstractToolset`` too, such as
pydantic-ai's own ``MCPToolset`` or a third-party toolset; those miss the connection
handling the toolsets on this page add.

Choosing a toolset
------------------

Pick a route by what you already have. More than one row of the table below can be
true at once, and the rows are not exclusive: one agent can carry several toolsets.
Two questions break the ties. *Whose credential is it?* Prefer the route whose
credential is an Airflow connection somebody on your side already reviewed. *Whose
tool list is it?* Prefer the route whose exposed surface you chose rather than
inherited. The pair that most often overlaps is an Airflow hook and a vendor MCP
server reaching the same target; both questions point at the hook, because its
credential is the connection and ``allowed_methods`` is a list you write. Reach for
the server when its tools cover work the hook does not expose, or when the
alternative is re-wrapping that API by hand.

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
   * - Files on an object store that the agent should read the way a person would:
       browse a directory, open a report, look at the first rows of a Parquet file
     - ``ObjectStorageToolset``
   * - Files on an object store (Parquet, CSV, Avro) or a catalog-managed
       table format such as Iceberg, to query with SQL rather than read
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
     - ``ManagedAgentToolset`` over a vendor hook that implements the managed-agent contract

The hook, SQL, object storage, DataFusion, MCP, Agent Skills and managed-agent guides each have a
*When to choose it* section giving the case for choosing it, what it cannot do, an
example that exists in this repository, and where its credentials and its work come
from. :doc:`../sandbox/index` carries the same section for ``SandboxToolset``.

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
   * - ``ObjectStorageToolset``
     - ``conn_id``; with ``conn_id=None``, the store's default credentials, such as
       the worker's AWS role
     - Worker process, against the store
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
   * - ``ManagedAgentToolset``
     - The vendor hook's own connection
     - The vendor's infrastructure

Two rows are worth pausing on. ``SandboxToolset`` deliberately takes no Airflow
credential; that is the whole point of it, and it substitutes a backend-level
boundary, on the host or at the vendor, for the connection-level one. ``ManagedAgentToolset``
authenticates through the vendor hook's connection, but what the remote agent may
touch is governed on the vendor's side. Neither is a defect, but in both cases the
access decision has moved somewhere Airflow cannot see it, and somebody has to make
that decision again in
the new place. The :ref:`defense-layer table <toolset-defense-layers>` is the
right companion when you do.

.. _toolset-call-barriers:

Tool calls as barriers
----------------------

``HookToolset``, ``SQLToolset``, ``DataFusionToolset`` and ``SandboxToolset``
each build their own tool definitions and set ``sequential=True`` on them, which
pydantic-ai treats as a barrier: the tool runs alone, tools the model emitted
before it finish first, and tools emitted after it start only once it returns.
A slow call on any of those four therefore holds up the rest of that step, not
just its own toolset. Expect this from all four; it is not a reason to prefer
one of them. ``ManagedAgentToolset`` sets ``sequential=False``
deliberately, because the wait it introduces is remote. ``ObjectStorageToolset``
leaves it unset, so its calls are not barriers, but each of them still waits for a
hook, SQL or DataFusion call in progress: the hook, SQL, DataFusion and object storage
toolsets run their blocking work in worker threads under one lock per task process.
``MCPToolset`` and ``AgentSkillsToolset`` define no tools of their own: they pass
through whatever the upstream toolset declares, so the setting is not theirs to
make.

.. _toolset-retry-budget:

How often the model may correct a failed call
---------------------------------------------

When the model calls a tool with arguments that fail its schema, or the tool asks the
model to try again (``ModelRetry``), the error goes back to the model so it can correct
the call. What counts differs by toolset:

- ``SQLToolset`` and ``DataFusionToolset`` turn every query error into ``ModelRetry``,
  so a misspelled column and a dropped connection both count.
- ``HookToolset`` counts invalid arguments and a call that supplies a pinned
  argument. An exception from the hook itself fails the run straight away.
- ``ObjectStorageToolset`` counts invalid arguments only. A path that does not exist or
  cannot be read goes back to the model as a failed result without using the budget;
  bound repeated failed reads with ``usage_limits``.
- ``AgentSkillsToolset`` counts a failed call to a skills tool, such as a resource name
  that does not exist.

These toolsets allow as many corrections as the agent's tool retry budget, pydantic-ai's
``retries`` (one by default), the same way pydantic-ai's own toolsets do. Pass
``max_retries`` to a toolset to give its tools a budget of their own. Once the budget is
used up the run fails, and Airflow's task retries take over.

.. code-block:: python

    AgentOperator(
        task_id="revenue_agent",
        prompt="What was last week's revenue?",
        llm_conn_id="pydanticai_default",
        toolsets=[SQLToolset(db_conn_id="warehouse")],
        agent_params={"retries": {"tools": 3}},
    )

An integer ``retries`` sets both the tool budget and the output-validation budget; a
dict such as ``{"tools": 3}`` or ``{"output": 3}`` raises only one of them. These toolsets
used to allow exactly one correction whatever ``retries`` said, so an agent that sets
``retries`` now applies it to them too: ``retries=0`` fails the run on the first bad
query, and a large integer ``retries`` meant for output validation also lets a failing
database be queried that many times. Pass ``max_retries=1`` to a toolset to keep the old
behaviour. Outside a pydantic-ai agent (the LangChain, Strands and Google ADK bridges)
there is no agent budget, so each tool gets one correction unless its toolset sets
``max_retries``.

Layering
--------

:class:`~airflow.providers.common.ai.toolsets.logging.LoggingToolset` is not an
alternative to anything above. It is a wrapper: it takes a toolset and returns a
toolset that logs each call. Choose a route first, then decide whether to wrap
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
the HITL review integration and automatic tool call logging that
``AgentOperator`` provides. Durable replay is still available: attach
``AirflowDurability`` to the agent (see :doc:`../durable_execution`).

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
and ``ManagedAgentToolset`` exists for the case where you want the second
behaviour from inside an agent that is otherwise doing the first.

See also
--------

- :doc:`../agent_security`: defense layers, ``allowed_tables`` enforcement, ``HookToolset``
  guidelines and the production checklist.
- :doc:`../security`: the provider's security policy.
- :doc:`../examples`: the example Dags referenced above.
