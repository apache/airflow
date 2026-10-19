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

The SDK has a tracing system of its own rather than OpenTelemetry. By default it sends
every run's traces to OpenAI's Traces backend using ``OPENAI_API_KEY`` from the
environment, whatever model the agent runs, including a self-hosted one, and the traces
hold the prompts and model output. Tool results are masked before the model sees them;
nothing else in those traces is.

:class:`~airflow.providers.common.ai.tools.openai_agents.AirflowTraceProcessor` is a trace
processor for the SDK that records each trace as OpenTelemetry spans under the task's
span instead. The example replaces the SDK's processors with it, so nothing goes to
OpenAI, and runs the agent inside
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
``agents.add_trace_processor`` instead. The spans use the names of the OpenTelemetry GenAI
conventions (``invoke_agent``, ``chat`` with the model and token counts,
``execute_tool``), and the SDK's other spans are named ``openai_agents.<kind>``. Prompts, completions, and tool arguments and results are left
out unless ``[common.ai] otel_export_enabled`` and ``capture_content`` are both on, and pass
through Airflow's secret masker when they are recorded; see :doc:`../observability`.

Use ``AirflowTraceProcessor`` when span content should follow ``[common.ai] capture_content``
and Airflow's secret masker.

To turn the SDK's tracing off altogether, call ``agents.set_tracing_disabled(True)`` or set
``OPENAI_AGENTS_DISABLE_TRACING=1`` in the worker's environment. That stops
``AirflowTraceProcessor`` from receiving anything too.

.. _howto/frameworks:openai-agents-durable:

Durable execution
-----------------

When a task that calls ``Runner.run`` fails and Airflow retries it, the agent starts
again from the prompt: every model call is paid for again, and every tool call happens
again. On a task with ``retries``, run the agent with
:func:`~airflow.providers.common.ai.durable.openai_agents.run_durably_sync`, or
:func:`~airflow.providers.common.ai.durable.openai_agents.run_durably` from async code,
instead. They take the same arguments as ``Runner.run_sync`` and ``Runner.run``, except
``previous_response_id``, ``conversation_id`` and ``auto_previous_response_id``, which they
refuse: with them OpenAI keeps the conversation, and a replayed call never reaches it.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_openai_agents_durable.py
    :language: python
    :start-after: [START example_openai_agents_durable]
    :end-before: [END example_openai_agents_durable]

Each model call and function tool call is recorded in the task instance's durable
journal as it completes, the journal ``AgentOperator(durable=True)`` uses (see
:doc:`../durable_execution`). On Airflow 3.3 and later the journal is kept in the task
state store; on earlier versions, set ``[common.ai] durable_cache_path`` to where it is
kept. Once the run succeeds, what was recorded is deleted, so clearing the task later
starts over. Outside a task there is nothing to replay into, and the agent runs as
``Runner.run`` runs it.

What a retry replays
^^^^^^^^^^^^^^^^^^^^

On a retry the agent runs again from the start, and each call that asks what the failed
try asked, in the same order, returns what it returned then: the calls of every model
given by name or in ``run_config``, the model and function tools of the starting agent
and of the agents in its ``handoffs`` list, and an agent used through ``as_tool``, as one
call. Recorded tool results pass through Airflow's secret masker before they are saved,
and a replayed model call reports no token usage.

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
- A tool call that failed, including one whose exception ``failure_error_function`` turned
  into text for the model. A ``ModelBehaviorError`` that a tool raises, such as arguments
  that fail validation with ``failure_error_function=None``, counts as the tool's failure:
  the tool call runs again, but the answer that asked for it replays.
- Tools other than function tools: hosted tools such as web search, the tools of
  ``mcp_servers``, and shell and computer tools.
- A function tool whose ``needs_approval`` is a function, since the SDK checks the
  arguments it approved only through its own invoker.
- The function tools of an agent reached through ``handoff()``, and its model too unless
  the agent's ``model`` is a model name or not set.
- A streamed run.
- Every call, when the run has a ``session``: the SDK adds the failed try's prompt to it
  before the first model call, so that call no longer asks what it asked then.

A ``SandboxToolset`` without ``attach_to`` is destroyed when the try ends, so the retry
starts with a new, empty sandbox that the replayed conversation does not describe.

Differences from ``AgentOperator``
----------------------------------

- A task retry runs the agent again from the start unless it runs through
  ``run_durably``; see :ref:`howto/frameworks:openai-agents-durable`.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``, and the
  SDK's own ``needs_approval`` interruptions are not connected to Airflow's approval UI.

Installation
------------

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[sql]" "openai-agents>=0.20.0"

The SDK requires ``openai>=3`` from 0.21.0 on. Airflow's constraints file pins ``openai``
below 3, so with that file the command above installs ``openai-agents`` 0.20, the last
release that accepts ``openai`` 2.x. ``AirflowTools`` works with both.
