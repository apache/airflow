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

.. _howto/frameworks:adk:

Google ADK
==========

You have an agent built with the `Agent Development Kit
<https://google.github.io/adk-docs/>`__ and you want it to run as an Airflow task,
reading data through connections your deployment already manages. Keep the agent as
it is, and add Airflow's toolsets to its tools with the
:class:`~airflow.providers.common.ai.tools.adk.AirflowTools` toolset.

.. note::

    ``AirflowTools`` and the framework-neutral tool interface it builds on are
    experimental. They may change in a minor release of this provider.
    See :ref:`howto/stability`.

Run an ADK agent in a task
--------------------------

The agent below answers a question about a database with a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`. Its model is Claude
through ADK's ``AnthropicLlm``, with the API key from an Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_adk_agent.py
    :language: python
    :start-after: [START example_adk_agent]
    :end-before: [END example_adk_agent]

Everything except ``AirflowTools`` is plain ADK: the agent, its runner and session
service, callbacks and your own function tools work as the ADK documentation
describes. ``AirflowTools`` is an ADK ``BaseToolset``, so it goes in ``tools=`` next to
any other tool or toolset.

What the toolset gives the agent
--------------------------------

``AirflowTools`` takes the same arguments as the Strands plugin (see
:doc:`strands`): toolsets that implement
:class:`~airflow.providers.common.ai.tools.ToolProvider`, such as ``SQLToolset`` and
``HookToolset``, and individual
:class:`~airflow.providers.common.ai.tools.AirflowTool` objects. ``MCPToolset`` is the exception:
use ADK's own ``McpToolset`` for an MCP server. Each tool keeps the
toolset tool's name, description and argument schema, runs through the toolset, and
passes its result through Airflow's secret masker.

- A result reaches the model as ``{"result": ...}``.
- A failure the model can correct, such as a rejected SQL statement or an invalid
  argument, reaches it as ``{"error": ...}``, ADK's own convention for a failed tool,
  and is marked as an error in ADK's telemetry. The tool's retry limit bounds how many
  times in a row the model can try again.
- A refusal the tool reports as final, such as a file that does not exist, also comes
  back as ``{"error": ...}`` without counting against that limit. ADK's own
  ``RunConfig(max_llm_calls=...)`` bounds how long a model can keep asking.
- Any other failure raises
  :class:`~airflow.providers.common.ai.tools.ToolCallError` out of
  ``runner.run_async``, with the message masked. The task fails and Airflow's own retry
  takes over, which is ADK's normal behaviour for a tool that raises.

Outside ``AgentOperator``, a toolset's connection ID is used as written: it is not
rendered as a template.

Differences from ``AgentOperator``
----------------------------------

- A task retry runs the agent again from the start. ADK's session services can persist
  a session across runs, but resuming one after a worker is lost is not something this
  provider has tested, so tools that change data must be safe to repeat.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``.

Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-common-ai "google-adk>=2.9.1"

ADK caps ``opentelemetry-api`` and ``opentelemetry-sdk`` at 1.42.1, which is lower than
the version pinned in Airflow's constraints file, so the command above fails when
it is run with that constraints file. Airflow itself accepts OpenTelemetry 1.42.1: build
the worker image without the constraints file for this step, or with a copy of it that
pins OpenTelemetry to 1.42.1. For ``AnthropicLlm``, install ``anthropic`` through this
provider's ``anthropic`` extra.
