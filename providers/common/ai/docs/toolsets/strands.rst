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

.. _howto/toolsets:strands:

Strands Agents
==============

You have an agent built with `Strands Agents <https://strandsagents.com/>`__ and
you want it to run as an Airflow task: after an upstream task, on a schedule, or
when an asset updates, reading data through connections your deployment already
manages. Keep the agent as it is. Build the model and the ``Agent`` yourself, and
give it Airflow's toolsets as Strands tools with
:func:`~airflow.providers.common.ai.tools.strands.as_strands_tools`.

.. warning::

    ``as_strands_tools`` and the framework-neutral tool interface it builds on are
    experimental. They may change in a minor release of this provider.

Run a Strands agent in a task
-----------------------------

The agent below answers a question about a database. Its model's API key comes
from an Airflow connection, and its tools are a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`, so the agent can
list tables, read schemas and run read-only queries, with results bounded to
what fits in the model's context:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands_agent.py
    :language: python
    :start-after: [START example_strands_agent]
    :end-before: [END example_strands_agent]

Everything except ``as_strands_tools`` is plain Strands. The agent's loop, model
features, hooks and session handling work as the Strands documentation describes,
and ``as_strands_tools(...)`` can sit next to tools you wrote with Strands' own
``@tool`` decorator in the same ``tools`` list.

The model can come from any connection that holds its credentials. For Strands'
default Bedrock model, build ``BedrockModel(boto_session=...)`` from
``AwsBaseHook(aws_conn_id=...).get_session()`` in the Amazon provider, so the
model uses the same AWS connection as the rest of your Dags.

What ``as_strands_tools`` gives the agent
-----------------------------------------

``as_strands_tools`` accepts any toolset that implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, which today means
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` and
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`, and returns one
Strands ``PythonAgentTool`` per tool. Each Strands tool:

- Keeps the toolset tool's name, description and argument schema.
- Runs through the toolset itself, so connection resolution, SQL validation,
  ``allowed_tables``, ``allowed_methods`` and result bounds behave exactly as they
  do in :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.
- Reports a failure to the model as a Strands result with ``status="error"``,
  so the model can read what went wrong and try again. This covers a rejected SQL
  statement, invalid arguments and an exception raised by the hook.
- Passes every result and error through Airflow's secret masker before it goes
  back to the model.
- Writes a ``Tool call: <name>`` group to the task log with the call's duration.

Why masking tool results matters
--------------------------------

Airflow masks connection passwords in task logs. Tool results are not logs:
they go to the model, to the model provider's API, into any traces the framework
records, and often into the agent's final answer, which your task may return as
an XCom. A tool whose client library puts a credentialed URL in its error message
would send the raw password to all of those places, even though the task log
shows ``***``.

Tools built by ``as_strands_tools`` apply the masker to what they return, so a
registered secret is replaced with ``***`` before it leaves the tool. The masker
knows the secrets Airflow has registered, such as connection passwords and
sensitive connection extras. It does not recognize a credential that exists only
in the data a tool returns, so the permissions of the connection's database role
or cloud identity remain the real boundary on what the agent can reach.

Tools you write yourself with Strands' ``@tool`` decorator do not pass through
the masker. To give one of your own functions the same treatment, wrap it as an
:class:`~airflow.providers.common.ai.tools.AirflowTool` and pass it to
``as_strands_tools`` alongside the toolsets:

.. code-block:: python

    import asyncio

    from airflow.providers.common.ai.tools import AirflowTool, ToolResult


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

Differences from ``AgentOperator``
----------------------------------

The agent is Strands', so features that ``AgentOperator`` implements on top of
pydantic-ai do not apply:

- A task retry runs the agent again from the start. There is no step-level
  replay as with ``AgentOperator(durable=True)``, so tools that change data must
  be safe to repeat.
- A tool error does not end the run. The model sees it and may call the tool
  again, so if a tool can keep failing, bound the task with ``execution_timeout``.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``.

Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-common-ai "strands-agents>=1.56.0"

Pin the ``strands-agents`` lower bound as above. Without it, a resolver that
cannot satisfy Strands' dependencies may pick the ``0.0.1`` placeholder release
of ``strands-agents`` instead of reporting a conflict. For ``AnthropicModel``,
install ``anthropic`` through this provider's ``anthropic`` extra rather than
through Strands' own ``anthropic`` extra, which requires an ``anthropic`` release
older than 1.0.

The framework-neutral tool interface
------------------------------------

``as_strands_tools`` is a thin adapter over an interface that belongs to no agent
framework, in :mod:`airflow.providers.common.ai.tools`:

- :class:`~airflow.providers.common.ai.tools.AirflowTool` is one operation: a
  name, a description, a JSON Schema for its arguments and an async function.
  Call it through ``AirflowTool.call``, which turns exceptions into error results
  and applies the secret masker.
- :class:`~airflow.providers.common.ai.tools.ToolResult` is what a call returns:
  JSON-compatible content and an explicit ``is_error`` flag.
- :class:`~airflow.providers.common.ai.tools.ToolProvider` is anything with an
  ``airflow_tools()`` method returning ``AirflowTool`` objects.
  ``SQLToolset.airflow_tools()`` and ``HookToolset.airflow_tools()`` implement it.

An adapter for another framework maps these three onto that framework's tool type
and error status, as ``as_strands_tools`` does for Strands.
