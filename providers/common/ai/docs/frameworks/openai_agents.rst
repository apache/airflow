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

.. _howto/frameworks:openai_agents:

OpenAI Agents SDK
=================

You have an agent built with the `OpenAI Agents SDK
<https://openai.github.io/openai-agents-python/>`__ and you want it to run as an Airflow
task: on a schedule, after an upstream task, or when an asset updates, reading data
through connections your deployment already manages. The agent, its runner, handoffs,
guardrails and your own function tools stay plain OpenAI Agents SDK. Airflow adds these,
each of which you can use without the others:

- :func:`~airflow.providers.common.ai.durable.openai_agents.run_durably_sync` runs the
  agent so that a task retry replays the model and tool calls the failed try completed,
  instead of paying for them again.
- :class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTools` gives the agent
  access to your warehouse, object storage and other systems through Airflow
  connections, limited by each toolset, with Airflow's secret masker applied to every
  result.
- :class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTraceProcessor`
  records the SDK's traces as OpenTelemetry spans under the task's span instead of
  sending them to OpenAI.

If you are not tied to the SDK,
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator` runs a Pydantic AI
agent with the same toolsets, and adds a human review step and durable execution
switched on with one argument. See :ref:`howto/operator:agent`.

.. note::

    ``AirflowTools``, ``AirflowTraceProcessor`` and ``run_durably`` are experimental:
    they may change in a minor release of this provider. The tool interface they build
    on, :class:`~airflow.providers.common.ai.tools.AirflowTool` and
    :class:`~airflow.providers.common.ai.tools.ToolProvider`, is stable. See :ref:`howto/stability`.

Installation
------------

Install the provider with the ``sql`` extra, which the examples' SQL toolset needs, and
the SDK:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[sql]>=1.0.0" "openai-agents>=0.20.0"

``openai-agents`` 0.21 and later require ``openai`` 3. Where ``openai`` is held below 3,
the resolver picks ``openai-agents`` 0.20, the last release that accepts ``openai`` 2.
``AirflowTools``, ``AirflowTraceProcessor`` and ``run_durably`` work with both.

Try the tools from a script
---------------------------

``AirflowTools`` needs no Dag, scheduler or metadata database. A Python script that can
see an Airflow connection can give an agent a toolset's tools, so you can try an agent
on your machine before you put it in a task. Define the connection in an environment
variable:

.. code-block:: bash

    export AIRFLOW_CONN_CRM='{"conn_type": "sqlite", "host": "/tmp/crm.db"}'

Then build the agent as you would without Airflow, with the toolset's tools in
``tools``. This one uses the SDK's default OpenAI client, which reads
``OPENAI_API_KEY``:

.. code-block:: python

    from agents import Agent, Runner

    from airflow.providers.common.ai.tools.openai_agents import AirflowTools
    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    agent = Agent(
        name="analyst",
        model="gpt-5-mini",
        instructions="Use list_tables, get_schema and query to answer.",
        tools=AirflowTools(SQLToolset(db_conn_id="crm")),
    )
    print(Runner.run_sync(agent, "Which customer has the highest ARR?").final_output)

The script expects ``/tmp/crm.db`` to be a SQLite database the agent can explore, such as
one with a ``customers`` table.

On Airflow 3, a script reads connections from environment variables and from a
configured secrets backend. It does not see a connection that exists only in the
metadata database, such as one created with ``airflow connections add``. ``SQLToolset``
hands that failure to the model like any other database error, so the model sees this
for a connection ID the script cannot find, and the run fails once the tool's retry limit
is used up:

.. code-block:: text

    {"error": "The list_tables tool failed: The conn_id `crm_missing` isn't defined\nUse the list_tables and get_schema tools to inspect the database, then fix the query and try again."}

Inside a task, every connection your deployment defines is available.

Run the agent in a task
-----------------------

In a Dag, build the agent inside the ``@task`` function, so connections are read when
the task runs rather than when the Dag file is parsed. Here the model's API key, and
optionally the base URL of another OpenAI-compatible endpoint such as a gateway or a
self-hosted server, come from an Airflow connection too:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_openai_agents_agent.py
    :language: python
    :start-after: [START example_openai_agents_agent]
    :end-before: [END example_openai_agents_agent]

The example reads two connections: ``openai_default``, whose password is the API key and
whose host, if set, is the base URL, and ``sql_default``, a database connection. Set the
``LLM_CONN_ID`` and ``DB_CONN_ID`` environment variables to use others.

The example uses ``OpenAIChatCompletionsModel``, so the connection's host can point at
a server that implements only the Chat Completions API. Against OpenAI itself,
``OpenAIResponsesModel`` with the same client uses the Responses API, the SDK's default.

``AirflowTools`` is a list of ``FunctionTool`` objects, so your own tools go next to it:
``tools=[*AirflowTools(SQLToolset(db_conn_id="crm")), my_tool]``.

.. _howto/frameworks:openai-agents-data-access:

Give the agent access to your data
----------------------------------

An agent in a data pipeline needs the warehouse, the buckets and the internal APIs your
other tasks already reach. Give it those through Airflow toolsets rather than tools you
write yourself. Each toolset logs in through an Airflow connection resolved in the
worker, so the credential is not part of the prompt or the tool schema. If a tool result
echoes it anyway, the secret masker replaces it with ``***``, as it does for every secret
Airflow has registered. Each toolset also limits what the agent can do with its
connection.

This agent answers a revenue question from two tables of a warehouse and one prefix of a
bucket:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_openai_agents_data_access.py
    :language: python
    :start-after: [START example_openai_agents_data_access]
    :end-before: [END example_openai_agents_data_access]

To run it, install ``apache-airflow-providers-amazon[s3fs]`` for the ``s3://`` path, and
create three connections: ``openai_default`` for the model, ``warehouse_agent_reader``
for a ``DbApiHook`` database such as Postgres, and ``aws_reports_reader`` for AWS.

What each toolset gives the agent, and what limits it:

.. list-table::
    :header-rows: 1
    :widths: 22 40 38

    * - Toolset
      - Tools the model gets
      - What you limit it with
    * - :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`
      - ``list_tables``, ``get_schema``, ``query`` and ``check_query`` against any
        database with a ``DbApiHook``
      - ``allowed_tables``, ``allow_writes`` (off by default), ``max_rows``,
        ``max_result_bytes``
    * - :class:`~airflow.providers.common.ai.toolsets.object_storage.ObjectStorageToolset`
      - ``list_files``, ``get_file_info`` and ``read_file`` under one path in S3, GCS,
        Azure Blob Storage or any store ``ObjectStoragePath`` opens. It cannot write.
      - The root path, ``max_files``, ``max_read_bytes``, ``max_output_bytes``
    * - :class:`~airflow.providers.common.ai.toolsets.datafusion.DataFusionToolset`
        (experimental)
      - ``list_tables``, ``get_schema`` and ``query``: SQL over Parquet, CSV and Avro
        files and Iceberg tables, run in the worker
      - The tables you register, ``allow_writes`` (off by default), ``max_rows``
    * - :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`
      - The methods you list from any Airflow hook, one tool per method
      - ``allowed_methods`` (required), ``pinned_arguments``; see
        :ref:`howto/frameworks:openai-agents-hooks`

A query that names a table outside ``allowed_tables``, or a path outside the storage
root, never reaches the database or the store. The model gets the reason back, and can
try again. Limits such as ``max_rows`` and ``max_read_bytes`` work differently: the query
or read still runs, and the result is cut to fit. With the example's two toolsets over a
SQLite database that also holds a ``salaries`` table, and a local directory with a
``payroll`` folder beside ``finance``, these calls were refused with these messages:

``query`` with ``SELECT employee, salary FROM salaries``
    .. code-block:: text

        {"error": "The query tool failed: Query references tables that are not in the allowed tables list: salaries. Use list_tables to see the allowed tables.\nUse the list_tables and get_schema tools to inspect the database, then fix the query and try again."}

``read_file`` with ``../payroll/salaries.csv``
    .. code-block:: text

        {"error": "'../payroll/salaries.csv' leaves the storage root; '..' is not allowed."}

The SQL refusal counts against the tool's retry limit, one correction in a row by
default, so a model that sends a second refused query in a row fails the task (see
:ref:`howto/frameworks:openai-agents-failures`). The object storage refusal does not count,
so bound repeated refused reads with ``max_turns``.

Back each check with the connection's own permissions: a database role granted
``SELECT`` on the two tables, and credentials that can read only the reports prefix. The
database and the store enforce those themselves, including for a query or path that a
toolset's check lets through.

``SQLToolset`` and ``DataFusionToolset`` refuse statements that change data unless you
pass ``allow_writes=True``, and ``ObjectStorageToolset`` cannot write at all. To store what
the agent produced, write it from your own task code, ideally in the next task, as
:ref:`howto/frameworks:openai-agents-durable` shows.

.. _howto/frameworks:openai-agents-hooks:

Other systems through their hooks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a system no toolset covers, ``HookToolset`` turns methods of its Airflow hook into
tools. It is only as narrow as the methods you list, so check each one before you expose
it:

- What it does. A ``HookToolset`` can do whatever its listed methods do, writes and
  deletes included; nothing in it is read-only by default.
- Which of its arguments choose the resource. Pin those with ``pinned_arguments``, so the
  model never sees them and a call that sets one is refused. Then check that no argument
  you leave open can widen the call again. ``HttpHook.run`` fails this test: pinning
  ``endpoint`` still leaves ``extra_options``, which can set a proxy or turn off TLS
  verification for the request.
- What it returns. The model sees the result serialized as JSON, or as ``str()`` of an
  object that is not JSON-compatible. ``HttpHook.run`` returns a ``requests.Response``,
  which the model would see as ``<Response [200]>``.

:ref:`hook-toolset-restricted` shows a hook that passes these checks: ``S3Hook`` with
``list_keys`` and ``read_key`` and the bucket pinned. The same toolset works in
``AirflowTools(...)``.

``.filtered()`` and ``.approval_required()``, which pydantic-ai toolsets offer, do not
reach the SDK through ``AirflowTools``. They return wrappers that ``AirflowTools`` cannot
read, and building it fails:

.. code-block:: text

    AttributeError: 'FilteredToolset' object has no attribute 'airflow_tools'

Choose the tools with ``allowed_tables``, ``allowed_methods`` and ``pinned_arguments``
instead. There is no per-call approval on this path; see
:ref:`howto/frameworks:openai-agents-limits`. Each toolset's page covers the rest of its
options: :ref:`howto/toolsets`.

Tools
-----

Where an agent's tools come from decides how they behave in a task:

.. list-table::
    :header-rows: 1
    :widths: 25 20 30 25

    * - Tool
      - Result masked
      - A failure the model can correct
      - Replayed on a durable retry
    * - ``AirflowTools``
      - Yes
      - Returned to the model as ``{"error": ...}``; for a toolset's tools, up to a retry limit
      - Yes, an ``{"error": ...}`` result too; a call that raised runs again
    * - Your own ``@function_tool``
      - No
      - As its ``failure_error_function`` decides
      - Yes, unless it raised, even when ``failure_error_function`` turned the
        exception into text
    * - ``mcp_servers``
      - No
      - As the SDK handles it
      - No, runs live
    * - Hosted tools (web search, file search), shell and computer tools
      - No
      - As the SDK handles it
      - No, runs live

Toolsets for ``AirflowTools``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``AirflowTools`` accepts any toolset that implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, such as
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`,
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset` and
:class:`~airflow.providers.common.ai.toolsets.object_storage.ObjectStorageToolset`, and
adds one function tool per toolset tool. Each tool:

- Keeps the toolset tool's name, description and argument schema. The schema goes to
  the model as it is, with ``strict_json_schema=False``: the SDK's strict mode would
  mark every argument required, including the ones a toolset leaves optional.
- Runs through the toolset itself, so connection resolution, SQL validation,
  ``allowed_tables``, ``allowed_methods`` and result bounds behave as each toolset's
  page describes. See :ref:`howto/toolsets`.
- Passes every result through Airflow's secret masker before it goes back to the
  model.
- Writes a ``Tool call: <name>`` group to the task log with the call's duration.

A toolset's connection ID is used as written: it is not rendered as a Jinja template,
so pass the value itself.

A :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` gives the agent
a sandbox to run code in, and its tools work only inside ``with sandbox:``, which
destroys the sandbox when the block ends. A call outside the block fails with
``SandboxToolset is not open``. Build and run the agent inside it:

.. code-block:: python

    from agents import Agent, Runner

    from airflow.providers.common.ai.sandbox.modal import ModalSandboxBackend
    from airflow.providers.common.ai.tools.openai_agents import AirflowTools
    from airflow.providers.common.ai.toolsets import SandboxToolset

    with SandboxToolset(ModalSandboxBackend()) as sandbox:
        agent = Agent(name="coder", model="gpt-5-mini", tools=AirflowTools(sandbox))
        answer = Runner.run_sync(agent, "Plot last month's signups.").final_output

See :ref:`sandbox-other-frameworks` for the backends and what the sandbox contains.

The SDK routes a call by tool name, so two toolsets that define the same name raise
``ValueError`` when you build ``AirflowTools``:

.. code-block:: text

    ValueError: More than one tool is named check_query, get_schema, list_tables, query. Give the toolsets different tool_prefix values, where they take one, so the model can call each of them.

.. _howto/frameworks:openai-agents-failures:

What the model sees when a call fails
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A failure the model can correct, such as a query that names a column the table does not
have, reaches it as ``{"error": ...}`` with the database's message, and the model can
try again. This is what the model received for a query against a ``customers`` table
that has an ``arr`` column but no ``revenue`` column:

.. code-block:: text

    {"error": "The query tool failed: no such column: revenue\nUse the list_tables and get_schema tools to inspect the database, then fix the query and try again."}

``SQLToolset`` refuses statements that change data unless it was built with
``allow_writes=True``, and the refusal goes back to the model the same way:

.. code-block:: text

    {"error": "The query tool failed: Statement type 'Delete' is not allowed. Allowed types: Select, Union, Intersect, Except, Describe, Show\nUse the list_tables and get_schema tools to inspect the database, then fix the query and try again."}

A toolset's tool may fail this way once in a row by default; pass ``max_retries`` to the
toolset to allow more. The count starts over after a call that succeeds and on every run
of the agent. A tool still failing after that raises
:class:`~airflow.providers.common.ai.tools.ToolCallError` out of ``Runner.run`` and
``Runner.run_sync``, so the task fails and Airflow's own retry takes over. Catch it as
``ToolCallError``; the traceback names a private subclass of it. For the same query sent
twice in a row, the message is:

.. code-block:: text

    query kept failing after 1 correction(s): The query tool failed: no such column: revenue
    Use the list_tables and get_schema tools to inspect the database, then fix the query and try again.

A refusal the tool reports as final, such as a file that does not exist, comes back as
``{"error": ...}`` without counting against that limit, and so do arguments that are not
a JSON object. Bound those with the ``max_turns`` argument of ``Runner.run``. Any other
failure, such as a hook method raising, also raises ``ToolCallError`` with its message
masked, and the task fails.

A function tool you write yourself is handled by the SDK instead. When it raises,
``openai-agents`` releases before 0.23 send the exception's text to the model unmasked.
From 0.23.1 they send ``An error occurred while running the tool. Please try again.``
with no detail, which gives the model nothing to correct.

Your own functions as Airflow tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Results of a tool you write with the SDK's ``@function_tool`` do not pass through the
masker. A result that carries a registered secret, such as an API response that echoes a
connection's token, would then send it to the model, the model provider, the SDK's
traces, and possibly the final answer, even though the task log shows ``***``. To give
one of your own functions the same treatment as a toolset, wrap it as an
:class:`~airflow.providers.common.ai.tools.AirflowTool` and pass it to ``AirflowTools``
next to the toolsets:

.. code-block:: python

    import asyncio

    from agents import Agent

    from airflow.providers.common.ai.tools import AirflowTool, ToolResult
    from airflow.providers.common.ai.tools.openai_agents import AirflowTools
    from airflow.providers.common.ai.toolsets.sql import SQLToolset


    async def lookup_customer(arguments: dict) -> ToolResult:
        # crm_client is your own client; its get() blocks, so run it off the event loop.
        customer = await asyncio.to_thread(crm_client.get, arguments["customer_id"])
        return ToolResult(content=customer)


    lookup = AirflowTool(
        name="lookup_customer",
        description="Look up one customer in the CRM by id.",
        parameters={
            "type": "object",
            "properties": {"customer_id": {"type": "integer"}},
            "required": ["customer_id"],
        },
        function=lookup_customer,
    )
    warehouse = SQLToolset(db_conn_id="warehouse")
    agent = Agent(name="support", model="gpt-5-mini", tools=AirflowTools(warehouse, lookup))

Return ``ToolResult(content=..., is_error=True)`` for a failure the model can correct,
and raise for anything else. An ``AirflowTool`` you build has no retry limit of its own,
so only ``max_turns`` bounds how often the model retries it. The masker knows the
secrets Airflow has registered, such as connection passwords and sensitive connection
extras. It does not recognize a credential that exists only in the data a tool returns,
such as an API key stored in a table row. To keep one from the agent, deny the
connection's database role or cloud identity access to it.

Agents as tools and handoffs
----------------------------

Give each agent the ``AirflowTools`` it needs. An agent that takes over through a
handoff runs its own tools, and they behave as described above.

``Agent.as_tool()`` turns any exception from the inner agent's run into text for the
outer model by default, so a tool failure that should fail the task would reach the
orchestrating model only as a message. When the inner agent holds ``AirflowTools``,
pass ``failure_error_function=None``:

.. code-block:: python

    analyst = Agent(
        name="analyst",
        model="gpt-5-mini",
        tools=AirflowTools(SQLToolset(db_conn_id="warehouse")),
    )
    lead = Agent(
        name="lead",
        model="gpt-5-mini",
        tools=[analyst.as_tool("ask_analyst", "Ask the SQL analyst.", failure_error_function=None)],
    )

The ``ToolCallError`` then fails the outer run, and with it the task. The same applies to
a function tool of your own that runs another agent: let the exception propagate.

MCP servers
-----------

Connect an MCP server with the SDK's own ``mcp_servers=`` and one of its ``MCPServer``
classes, such as ``MCPServerStreamableHttp``. ``MCPToolset`` does not work through
``AirflowTools``, because an MCP session belongs to the event loop that opened it.
``AirflowTools(MCPToolset(...))`` raises:

.. code-block:: text

    airflow.providers.common.ai.exceptions.MCPToolsetNotSupportedError: MCPToolset works in Pydantic AI agents and through the LangChain bridge, not through AirflowTools. Pass the server to the agent's mcp_servers with one of the SDK's MCPServer classes, such as MCPServerStreamableHttp, instead.

Results from ``mcp_servers`` do not pass through Airflow's secret masker, and under
``run_durably`` their calls run again on every retry.

Tracing
-------

The SDK has a tracing system of its own rather than OpenTelemetry. By default it sends
every run's traces to OpenAI's Traces backend using ``OPENAI_API_KEY`` from the
environment, whatever model the agent runs, including a self-hosted one. The traces
hold the prompts and the model's output. When the worker has no ``OPENAI_API_KEY``, as
when the key comes from a connection, the SDK logs ``OPENAI_API_KEY is not set, skipping
trace export`` instead. Tool results from ``AirflowTools`` are masked
before the model sees them; nothing else in those traces is.

``AirflowTraceProcessor`` records each trace as OpenTelemetry spans under the task's
span instead. Replace the SDK's processors with it, so nothing goes to OpenAI, and run
the agent inside
:func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing`, so the spans
carry the task's identity:

.. code-block:: python

    from agents import Runner, set_trace_processors

    from airflow.providers.common.ai.tools.openai_agents import AirflowTraceProcessor
    from airflow.providers.common.ai.tools.tracing import agent_framework_tracing

    set_trace_processors([AirflowTraceProcessor()])
    with agent_framework_tracing():
        answer = Runner.run_sync(agent, question).final_output

``set_trace_processors`` replaces every processor registered with the SDK. To keep the
export to OpenAI, or a processor another instrumentation registered, add this one with
``agents.add_trace_processor`` instead.

The spans use the names of the OpenTelemetry GenAI conventions: ``invoke_agent`` for an
agent, ``chat`` for a model call with its model and token counts, and ``execute_tool``
for a tool call. The SDK's other spans, such as one per turn of the agent loop or one per
handoff, are named ``openai_agents.<kind>``. Prompts, completions, and tool arguments and
results are left out unless ``[common.ai] otel_export_enabled`` and ``capture_content``
are both on, and pass through Airflow's secret masker when they are recorded; see
:doc:`../observability`.

To turn the SDK's tracing off altogether, call ``agents.set_tracing_disabled(True)`` or
set ``OPENAI_AGENTS_DISABLE_TRACING=1`` in the worker's environment, and
``AirflowTraceProcessor`` then receives nothing either. To turn it off for one run, pass
``run_config=RunConfig(tracing_disabled=True)``, as the durable example below does.

.. _howto/frameworks:openai-agents-durable:

Durable execution
-----------------

When a task that calls ``Runner.run`` fails and Airflow retries it, the agent starts
again from the prompt: every model call is paid for again, and every tool call happens
again. On a task with ``retries``, run the agent with ``run_durably_sync``, or
:func:`~airflow.providers.common.ai.durable.openai_agents.run_durably` from async code,
instead. They take the same arguments as ``Runner.run_sync`` and ``Runner.run``, except
``previous_response_id``, ``conversation_id`` and ``auto_previous_response_id``, which
they refuse with ``ValueError`` inside a task: with them OpenAI keeps the conversation,
and a replayed call never reaches it. This Dag runs the agent from the task example
with ``run_durably_sync``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_openai_agents_durable.py
    :language: python
    :start-after: [START example_openai_agents_durable]
    :end-before: [END example_openai_agents_durable]

Each model call and function tool call is recorded in the task instance's durable
journal as it completes (see :doc:`../durable_execution`). On Airflow 3.3 and later the
journal is kept in the task state store. On earlier versions, set ``[common.ai]
durable_cache_path`` to where it is kept; without it the call raises ``ValueError:
Durable execution on Airflow before 3.3 needs [common.ai] durable_cache_path to be set``.
Outside a task there is nothing to replay into, and the agent runs as ``Runner.run`` runs
it.

What was recorded is deleted when the agent's run succeeds, so clearing the task later
starts over. That happens when ``run_durably_sync`` returns, not when the task
succeeds: if the task fails after the agent has answered, for example while writing the
answer somewhere, the retry runs the whole agent again. Write the answer in a
downstream task, so that a failed write retries on its own. Here ``build_agent()``
stands for building the agent as the task example does:

.. code-block:: python

    from airflow.providers.common.ai.durable.openai_agents import run_durably_sync
    from airflow.sdk import ObjectStoragePath, task


    @task(retries=2)
    def answer(question: str) -> str:
        return run_durably_sync(build_agent(), question).final_output


    @task(retries=5)
    def publish(text: str) -> None:
        ObjectStoragePath("s3://reports/answer.md", conn_id="aws_default").write_text(text)


    publish(answer("Which customer has the highest ARR?"))

What a retry replays
^^^^^^^^^^^^^^^^^^^^

On a retry the agent runs again from the start, and each call that asks what the failed
try asked, in the same order, returns what it returned then. That covers the calls of
every model given by name or in ``run_config``, the model and function tools of the
starting agent and of the agents in its ``handoffs`` list, and an agent used through
``as_tool``, which replays as one call. Recorded tool results pass through Airflow's
secret masker before they are saved, and a replayed model call reports no token usage.

From the first call that differs, that call and every call after it run live:

- A retry whose prompt, instructions, model settings or tools differ from the failed
  try's runs everything live from the first call that sees the difference.
- When the run failed because of the model's answers (``MaxTurnsExceeded``,
  ``ModelBehaviorError``, ``ModelRefusalError``, or an output or tool input guardrail),
  replaying those answers would fail it the same way, so the retry asks the model again
  from its last request.

What always runs live
^^^^^^^^^^^^^^^^^^^^^

- A call that was running when the try failed, so tools that change data must still be
  safe to repeat.
- A tool call that raised, including one whose exception ``failure_error_function``
  turned into text for the model. An ``{"error": ...}`` result from ``AirflowTools`` is
  not a raise: it is recorded, and the retry replays it. A ``ModelBehaviorError`` that a tool raises, such as
  arguments that fail validation with ``failure_error_function=None``, counts as the
  tool's failure: the tool call runs again, but the answer that asked for it replays.
- Tools other than function tools: hosted tools, the tools of ``mcp_servers``, and shell
  and computer tools.
- A function tool whose ``needs_approval`` is a function: the SDK checks that the
  arguments a call runs with are the ones it approved, and a replayed result would skip
  that check.
- The function tools of an agent reached through ``handoff()``, and its model too unless
  the agent's ``model`` is a model name or not set.
- A streamed run (``Runner.run_streamed``).
- Every call, when the run has a ``session``: the SDK adds the failed try's prompt to it
  before the first model call, so that call no longer asks what it asked then.

A ``SandboxToolset`` without ``attach_to`` is destroyed when its ``with`` block ends,
which a failed try also ends, so the retry starts with a new, empty sandbox that the
replayed conversation does not describe.

.. _howto/frameworks:openai-agents-limits:

Sessions, approvals and streaming
---------------------------------

These SDK features interact with Airflow in ways the SDK documentation does not cover:

- A ``session`` works, but under ``run_durably`` it makes every retry run live, as
  above.
- A tool's ``needs_approval`` is not connected to Airflow's approval UI. The run stops
  at the first call that needs approval and returns with ``final_output`` set to
  ``None`` and the pending call in ``result.interruptions``, and nothing in Airflow
  resumes it. To have a person approve the agent's answer, add the standard
  provider's ``ApprovalOperator`` as the next task.
- A streamed run (``Runner.run_streamed``) is not recorded for durable retries.
