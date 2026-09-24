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

.. _howto/frameworks:strands:

Strands Agents
==============

You have an agent built with `Strands Agents <https://strandsagents.com/>`__ and
you want it to run as an Airflow task: after an upstream task, on a schedule, or
when an asset updates, reading data through connections your deployment already
manages. Keep the agent as it is. Build the model and the ``Agent`` yourself, and
give it Airflow's toolsets with the
:class:`~airflow.providers.common.ai.tools.strands.AirflowTools` plugin.

.. note::

    ``AirflowTools`` and the framework-neutral tool interface it builds on are
    experimental. They may change in a minor release of this provider.
    See :ref:`howto/stability`.

Run a Strands agent in a task
-----------------------------

The agent below answers a question about a database. Its model's API key comes
from an Airflow connection, and its tools come from a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`, so the agent can
list tables, read schemas and run read-only queries, with results bounded to
what fits in the model's context:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands_agent.py
    :language: python
    :start-after: [START example_strands_agent]
    :end-before: [END example_strands_agent]

Everything except ``AirflowTools`` is plain Strands. The agent's loop, model
features, hooks and session handling work as the Strands documentation describes,
and tools you wrote with Strands' own ``@tool`` decorator go in ``tools=`` as
usual, next to the plugin.

The model can come from any connection that holds its credentials. For Strands'
default Bedrock model, build ``BedrockModel(boto_session=...)`` from
``AwsBaseHook(aws_conn_id=...).get_session()`` in the Amazon provider, so the
model uses the same AWS connection as the rest of your Dags.

What the plugin gives the agent
-------------------------------

``AirflowTools`` accepts any toolset that implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, such as
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` and
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`, and adds one
Strands tool per toolset tool. ``MCPToolset`` is the exception: connect Strands' own
``MCPClient`` to the server instead. Each tool:

- Keeps the toolset tool's name, description and argument schema.
- Runs through the toolset itself, so connection resolution, SQL validation,
  ``allowed_tables``, ``allowed_methods`` and result bounds behave as they
  do in :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.
- Passes every result through Airflow's secret masker before it goes back to the
  model.
- Writes a ``Tool call: <name>`` group to the task log with the call's duration.

Failures follow the same rule as in ``AgentOperator``:

- A failure the model can correct, such as a rejected SQL statement or an invalid
  argument, reaches it as a Strands result with ``status="error"``, and the model
  can try again. The tool's retry limit bounds how many times in a row it can do so.
  ``SQLToolset`` treats every database error this way, so a query that fails because
  the database rejects the connection's credentials ends the run once that limit is
  reached.
- A refusal the tool reports as final, by raising pydantic-ai's ``ToolFailed``, also
  reaches the model as an error, and does not count against the retry limit. Bound the task with ``execution_timeout`` if a model could
  keep asking for it.
- Any other failure, such as a hook method raising, ends the agent run. Strands raises
  ``EventLoopException`` with the masked
  :class:`~airflow.providers.common.ai.tools.ToolCallError` as its
  ``original_exception``, the task fails, and Airflow's own retry takes over. Strands
  on its own would hand such an error to the model and let the run carry on.

Outside ``AgentOperator``, a toolset's connection ID is used as written: it is not
rendered as a template, so pass the value itself rather than a Jinja expression.

The plugin is built on the framework-neutral tool interface described in
:ref:`howto/frameworks`, which is also the starting point for an adapter to another
framework.

Why masking tool results matters
--------------------------------

Airflow masks connection passwords in task logs. Tool results are not logs:
they go to the model, to the model provider's API, into any traces the framework
records, and often into the agent's final answer, which your task may return as
an XCom. A tool whose client library puts a credentialed URL in its error message
would send the raw password to all of those places, even though the task log
shows ``***``.

Tools added by ``AirflowTools`` apply the masker to what they return, so a
registered secret is replaced with ``***`` before it leaves the tool. The masker
knows the secrets Airflow has registered, such as connection passwords and
sensitive connection extras. It does not recognize a credential that exists only
in the data a tool returns, so the permissions of the connection's database role
or cloud identity remain the real boundary on what the agent can reach.

Tools you write yourself with Strands' ``@tool`` decorator do not pass through
the masker. To give one of your own functions the same treatment, wrap it as an
:class:`~airflow.providers.common.ai.tools.AirflowTool` and pass it to
``AirflowTools`` alongside the toolsets:

.. code-block:: python

    import asyncio

    from airflow.providers.common.ai.tools import AirflowTool, ToolResult
    from airflow.providers.common.ai.tools.strands import AirflowTools


    async def lookup_customer(arguments: dict) -> ToolResult:
        # crm_client.get blocks, so run it off the agent's event loop.
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
    agent = Agent(model=model, plugins=[AirflowTools(warehouse, lookup)])

Differences from ``AgentOperator``
----------------------------------

The agent is Strands', so features that ``AgentOperator`` implements on top of
Pydantic AI do not apply:

- A task retry runs the agent again from the start. There is no step-level
  replay as with ``AgentOperator(durable=True)``, so tools that change data must
  be safe to repeat.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``.

Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-common-ai "strands-agents>=1.56.0"

Pin the ``strands-agents`` lower bound as above; 1.56 is the release this
integration is tested with. Without a bound, a resolver that cannot satisfy Strands'
dependencies may pick the ``0.0.1`` placeholder release instead of reporting a
conflict. For ``AnthropicModel``, install ``anthropic`` through this provider's
``anthropic`` extra rather than through Strands' own ``anthropic`` extra, which
requires an ``anthropic`` release older than 1.0. Strands also pins ``mcp`` below 2.2, so
installing it downgrades a newer ``mcp``, which ``MCPToolset`` uses.
