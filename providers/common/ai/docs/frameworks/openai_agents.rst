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
task, reading data through connections your deployment already manages. Keep the agent
as it is, and add Airflow's toolsets to its tools with
:class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTools`.

.. note::

    ``AirflowTools`` is experimental: it may change in a minor release of this provider.
    The framework-neutral tool interface it builds on is stable.
    See :ref:`howto/stability`.

Run an OpenAI Agents SDK agent in a task
----------------------------------------

The agent below answers a question about a database with a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`. Its model's API key, and
optionally the base URL of another OpenAI-compatible endpoint, come from an Airflow
connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_openai_agents_agent.py
    :language: python
    :start-after: [START example_openai_agents_agent]
    :end-before: [END example_openai_agents_agent]

Everything except ``AirflowTools`` is plain OpenAI Agents SDK: the agent, its runner,
handoffs, guardrails and your own function tools work as the SDK documentation
describes. ``AirflowTools`` is a list of ``FunctionTool`` objects, so add your own tools
next to them with ``tools=[*AirflowTools(warehouse), my_tool]``.

What the tools give the agent
-----------------------------

``AirflowTools`` takes the same arguments as the Strands plugin (see :doc:`strands`):
toolsets that implement :class:`~airflow.providers.common.ai.tools.ToolProvider`, such
as ``SQLToolset`` and ``HookToolset``, and individual
:class:`~airflow.providers.common.ai.tools.AirflowTool` objects. ``MCPToolset`` is the
exception: attach an MCP server with the SDK's own ``mcp_servers=``. Each tool keeps the
toolset tool's name, description and argument schema, runs through the toolset, and
passes its result through Airflow's secret masker.

- The schema goes to the model as it is, with ``strict_json_schema=False``. The SDK's
  strict mode would mark every argument required, including the ones a toolset leaves
  optional.
- A failure the model can correct, such as a rejected SQL statement or an invalid
  argument, reaches it as ``{"error": ...}``. The tool's retry limit bounds how many
  times in a row the model can try again, counted afresh on every run of the agent.
- A refusal the tool reports as final, such as a file that does not exist, also comes
  back as ``{"error": ...}`` without counting against that limit, and so do arguments
  that are not a JSON object. The ``max_turns`` argument of ``Runner.run`` bounds how
  long a model can keep asking.
- Any other failure raises
  :class:`~airflow.providers.common.ai.tools.ToolCallError` out of ``Runner.run`` and
  ``Runner.run_sync``, with the message masked. The task fails and Airflow's own retry
  takes over.

Outside ``AgentOperator``, a toolset's connection ID is used as written: it is not
rendered as a template.

An agent used as a tool
-----------------------

``Agent.as_tool()`` turns any exception from the inner agent's run into text for the
outer model by default, so a tool failure that should fail the task would only reach the
orchestrating model as a message. When the inner agent holds ``AirflowTools``, pass
``failure_error_function=None``:

.. code-block:: python

    analyst = Agent(name="analyst", model=model, tools=AirflowTools(warehouse))
    lead = Agent(
        name="lead",
        model=model,
        tools=[analyst.as_tool("ask_analyst", "Ask the SQL analyst.", failure_error_function=None)],
    )

The ``ToolCallError`` then fails the outer run, and with it the task. The same applies to
a function tool of your own that runs another agent: let the exception propagate. A
nested run shares the outer run's retry count for each tool.

Tracing
-------

The SDK has its own tracing, which is on by default and sends every run's traces to
OpenAI's Traces backend using ``OPENAI_API_KEY`` from the environment. It does this
whatever model the agent runs, including a self-hosted one, and the traces hold the
prompts and model output. Tool results are masked before the model sees them; nothing
else in a trace is.

The example turns tracing off for its run with ``RunConfig(tracing_disabled=True)``. To
turn it off for the whole process, call ``agents.set_tracing_disabled(True)`` or set
``OPENAI_AGENTS_DISABLE_TRACING=1`` in the worker's environment. The SDK does not emit
OpenTelemetry spans, so
:func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing` does not apply
to it.

Differences from ``AgentOperator``
----------------------------------

- A task retry runs the agent again from the start, so tools that change data must be
  safe to repeat.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``, and the
  SDK's own ``needs_approval`` interruptions are not connected to Airflow's approval UI.

Installation
------------

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[sql]" "openai-agents>=0.20.0"

The SDK requires ``openai>=3`` from 0.21.0 on. Airflow's constraints file pins ``openai``
below 3, so with that file the command above installs ``openai-agents`` 0.20, the last
release that accepts ``openai`` 2.x. ``AirflowTools`` works with both.
