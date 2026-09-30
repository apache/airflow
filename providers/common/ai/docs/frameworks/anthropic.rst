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

.. _howto/frameworks:anthropic:

Anthropic SDK tool runner
=========================

If your agent is a loop on the Anthropic Python SDK's
``client.beta.messages.tool_runner``, you can run it in an Airflow task as it is and
give it the toolsets this provider ships.
:class:`~airflow.providers.common.ai.tools.anthropic.AirflowTools` turns
``SQLToolset``, ``HookToolset`` and most other toolsets into tools the runner accepts.
If you have no loop yet, ``AgentOperator`` runs one for you, with durable replay and
human review; see :doc:`index`.

.. note::

    ``AirflowTools`` and the framework-neutral tool interface it builds on are
    experimental. They may change in a minor release of this provider.
    See :ref:`howto/stability`.

Run a tool runner in a task
---------------------------

This task answers a question about a database. The client's API key, and a gateway URL
if you route through one, come from an Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_anthropic_tool_runner.py
    :language: python
    :start-after: [START example_anthropic_tool_runner]
    :end-before: [END example_anthropic_tool_runner]

Against a SQLite database with one table, ``orders``, the model called two tools. Here
are the task log's tool-call lines, timestamps trimmed, and the value the task returned:

.. code-block:: text

    [info] ::group::Tool call: list_tables
    [info] Tool list_tables returned in 0.00s
    [info] ::group::Tool call: query
    [info] Tool query returned in 0.67s
    [info] Done. Returned value was: The database contains **one table**:

    | Table | Row count |
    |--------|-----------|
    | `orders` | 4 |

Only ``AirflowTools`` and ``tools.run(runner)`` come from Airflow. The system prompt,
model parameters and ``max_iterations`` are the runner's own, and so are tools you
write with the SDK's ``@beta_tool`` decorator; pass those alongside Airflow's, as
``tools=[*tools.tools, my_tool]``. ``tools.run`` fails the task only for Airflow's tools:
the runner still hands an exception from one of your own tools to the model.

To reach Claude on Amazon Bedrock, Claude Platform on AWS, Google Vertex AI or Microsoft
Foundry, build the
client with ``AnthropicHook(conn_id=...).get_conn()`` from the Anthropic provider
instead. It reads the platform, region and credentials from an Anthropic connection,
and every client it returns has the same ``tool_runner``.

What each tool does
-------------------

``AirflowTools`` accepts any toolset that implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, and individual
:class:`~airflow.providers.common.ai.tools.AirflowTool` objects. ``MCPToolset`` is the
exception; give the runner the MCP server through the SDK instead. Each tool:

- Keeps the toolset tool's name, description and argument schema.
- Runs through the toolset, so connection resolution, SQL validation,
  ``allowed_tables``, ``allowed_methods`` and result limits behave as they do in
  :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.
- Passes its result through Airflow's secret masker before the model sees it. A
  connection password that turns up in a query result reaches the model as ``***``.
- Writes a ``Tool call: <name>`` group to the task log. Calls to a toolset are also
  counted in the ``common_ai.tool_calls`` metric with ``framework=anthropic`` (see
  :doc:`../observability`).

When a tool fails
-----------------

A failure the model can correct, such as a query naming a table outside
``allowed_tables``, comes back to it as a ``tool_result`` with ``is_error`` set. The
model received this block when it was told to count the rows of a ``sales`` table that
the toolset did not allow:

.. code-block:: json

    {
      "type": "tool_result",
      "tool_use_id": "toolu_01PNeqBojrc5S7FnNAeP1brL",
      "content": "The query tool failed: Query references tables that are not in the allowed tables list: sales. Use list_tables to see the allowed tables.\nUse the list_tables and get_schema tools to inspect the database, then fix the query and try again.",
      "is_error": true
    }

It then listed the tables, found ``orders``, and answered from it. ``SQLToolset``,
``HookToolset`` and ``ObjectStorageToolset`` allow one correction: a tool that fails in
two model turns in a row ends the run with
``ToolCallError: query kept failing after 1 correction(s): ...``. Each turn counts once,
however many of its calls failed. ``SQLToolset`` treats every database error as
correctable, so a query rejected because the connection's credentials are wrong ends the
run on its second failure.

Any other failure, such as a hook raising, ends the run. ``tools.run`` raises
:class:`~airflow.providers.common.ai.tools.ToolCallError` before the next model
request, the task fails, and Airflow's own retry takes over. Here is the error from a
run where a tool's call to a billing API raised; the masker has already replaced the
token in the message:

.. code-block:: text

    airflow.providers.common.ai.tools.ToolCallError: fetch_invoice failed: RuntimeError: billing API returned 503 for token ***

Drive the runner with ``tools.run``
-----------------------------------

The tool runner catches every exception a tool raises and sends it to the model as an
error result. If you finish the loop with the runner's own ``until_done()``, or iterate
it yourself, a hook that raises becomes one more thing for the model to read, and the
task can succeed with an answer written around the failure:

.. code-block:: python

    message = runner.until_done()  # a failing hook becomes text the model reads
    message = tools.run(runner)  # a failing hook fails the task

Airflow's tools log a warning when a failure goes to the model this way.

``tools.run`` runs each turn's tool calls itself, which is also how the retry limit
knows where one model turn ends. When a call fails in a way that ends the run, the
turn's later calls are not run; the calls before it already have.

``tools.run`` takes the runner that ``client.beta.messages.tool_runner`` returns. It
does not support the streaming runner you get with ``stream=True``: a type checker
rejects the call, and at run time it fails with an ``AttributeError`` after the first
model request, before any tool runs.

Async clients
-------------

For an ``AsyncAnthropic`` client, use
:class:`~airflow.providers.common.ai.tools.anthropic.AsyncAirflowTools` and await
``run``, inside an ``async def`` task or any other coroutine:

.. code-block:: python

    from anthropic import AsyncAnthropic

    from airflow.providers.common.ai.tools.anthropic import AsyncAirflowTools
    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    client = AsyncAnthropic()
    tools = AsyncAirflowTools(SQLToolset(db_conn_id="warehouse"))
    runner = client.beta.messages.tool_runner(
        model="claude-opus-5-5",
        max_tokens=16000,
        tools=tools.tools,
        messages=[{"role": "user", "content": "Which tables exist?"}],
    )
    message = await tools.run(runner)

Differences from ``AgentOperator``
----------------------------------

The loop is the SDK's, so what ``AgentOperator`` builds on top of Pydantic AI does not
apply:

- A task retry runs the loop again from the start. Tools that change data must be safe
  to repeat, because there is no step-level replay as with
  ``AgentOperator(durable=True)``.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``.
- A toolset's connection ID is used as written. It is not rendered as a template, so
  pass the value rather than a Jinja expression.

Installation
------------

Install this provider with its ``anthropic`` extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[anthropic]"

The ``anthropic`` extra installs the SDK. Add ``apache-airflow-providers-anthropic`` if
you build the client with ``AnthropicHook``. ``tool_runner`` is a beta helper of the
SDK. The adapter needs ``anthropic`` 1.1.0 or later, which the extra requires,
and is tested with 1.8.0.

.. seealso::

    - :doc:`../toolsets/index`, for what each toolset exposes and its limits.
    - :doc:`index`, for the framework-neutral tool interface ``AirflowTools`` is built on.
    - `Tool Runner <https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-runner>`__,
      in Anthropic's documentation, for the runner itself.
