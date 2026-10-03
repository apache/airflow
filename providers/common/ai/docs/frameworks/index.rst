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

.. _howto/frameworks:

Agent frameworks
================

This provider is built on `Pydantic AI <https://ai.pydantic.dev/>`__: its operators
and decorators create and run Pydantic AI agents. If you already build agents with
another framework, you do not have to move them to Pydantic AI to run them in Airflow.
Run the agent in a ``@task`` as you do today, and use this provider for the parts that
need Airflow: models and tools backed by Airflow connections.

Supported frameworks
--------------------

A framework is supported when this provider ships code for it and documents it. What
that code takes on differs from one framework to the next, so the table says which part
is Airflow's and which part stays yours.

.. list-table::
   :widths: 16 54 14 16
   :header-rows: 1

   * - Framework
     - What this provider gives you
     - Who runs the agent
     - Install
   * - Pydantic AI, through ``AgentOperator``
     - :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`,
       ``@task.agent``, the ``@task.llm`` family and every toolset, plus durable
       replay, human review, approval gates, retry policies and tracing.
     - The operator
     - Included
   * - Pydantic AI, your own agent
     - A model from a connection through ``PydanticAIHook``, and every toolset, since they
       are Pydantic AI toolsets. Guide: :doc:`pydantic_ai`.
     - You
     - Included
   * - Strands Agents
     - The :class:`~airflow.providers.common.ai.tools.strands.AirflowTools` plugin gives
       a Strands agent the tools of ``SQLToolset``, ``HookToolset``,
       ``ObjectStorageToolset`` and the other toolsets, with Airflow's secret masker
       applied to every result. Guide: :doc:`strands`.
     - You
     - ``strands-agents``
   * - Google ADK
     - The :class:`~airflow.providers.common.ai.tools.adk.AirflowTools` toolset gives an
       ADK agent the same tools, with the same masking. Guide: :doc:`adk`.
     - You
     - ``google-adk``
   * - LangChain
     - :class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook` returns chat
       and embedding models configured from a connection, and
       :func:`~airflow.providers.common.ai.toolsets.langchain_bridge.airflow_toolset_to_langchain_tools`
       turns any toolset into LangChain tools. Guides: :doc:`../hooks/langchain`,
       :doc:`../toolsets/langchain`.
     - You
     - ``[langchain]`` extra
   * - LlamaIndex
     - :class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook` returns
       language and embedding models configured from a connection, and embedding and
       retrieval operators build a RAG pipeline from them. Guide:
       :doc:`../rag_pipelines`.
     - No agent
     - ``[llamaindex]`` extra
   * - Claude Agent SDK, through ``HarnessOperator``
     - :class:`~airflow.providers.common.ai.operators.harness.HarnessOperator` runs the
       vendor's own agent loop in the task, with Airflow giving it tools, credentials and
       collecting the result. Guide: :doc:`../operators/harness`.
     - The vendor's loop, inside the operator
     - ``[claude-agent-sdk]`` extra

The Strands and ADK integrations, the framework-neutral tool interface under them, the
tracing helper, and the native harness interface under ``HarnessOperator``, are
experimental: they can change or be removed in a minor release of this provider. See
:ref:`howto/stability`.

Tested versions
---------------

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Framework
     - Version
     - Notes
   * - Strands Agents
     - 1.56.0
     - Pins ``mcp`` below 2.2, so installing it downgrades ``mcp`` if a newer one is
       installed.
   * - Google ADK
     - 2.9.1
     - Pins OpenTelemetry at 1.42.1 or lower, below the version in Airflow's constraints
       file. Airflow itself accepts 1.42.1.

These are the versions the adapter tests have been run against. CI does not run those
tests: its environment has ``mcp`` 2.2 and OpenTelemetry 1.44, which Strands and ADK
exclude. They run in an environment with the framework installed.

Choosing a route
----------------

Start from the agent you already have:

- **An existing Pydantic AI agent**: keep it, give it a model with ``PydanticAIHook`` and
  pass Airflow's toolsets to it. See :doc:`pydantic_ai`.
- **An existing Strands agent**: keep it, and add Airflow's toolsets with the
  ``AirflowTools`` plugin. See :doc:`strands`.
- **An existing ADK agent**: keep it, and add Airflow's toolsets with the ADK
  ``AirflowTools`` toolset. See :doc:`adk`.
- **An existing LangChain agent**: build its model with ``LangChainHook`` and add
  Airflow's toolsets with ``airflow_toolset_to_langchain_tools``.
- **No agent yet, or no preference**: use ``AgentOperator`` or ``@task.agent``. That route
  is the only one with step-level durable replay across task retries and a built-in
  human review step, because both are implemented on top of Pydantic AI.

Any other framework
-------------------

Every framework can run inside a ``@task``, and every framework can call an Airflow hook
from one of its own tools, so a framework that is not in the table above still has a
route. Before you depend on it, check that its dependencies resolve alongside the
provider versions your deployment uses: some frameworks pin versions of packages
Airflow also depends on, such as OpenTelemetry or a vendor SDK.

To give such a framework Airflow's toolsets rather than hand-written tools, build on
the framework-neutral interface in :mod:`airflow.providers.common.ai.tools`, which is
what the Strands and ADK integrations are written against:

- :class:`~airflow.providers.common.ai.tools.AirflowTool` is one operation: a name, a
  description, a JSON Schema for its arguments and an async function. Call it through
  ``AirflowTool.call``, which applies the secret masker to what the tool returns. A
  failure the model can correct comes back as an error result; any other failure raises
  :class:`~airflow.providers.common.ai.tools.ToolCallError`, which should end the run.
- :class:`~airflow.providers.common.ai.tools.ToolResult` is what a call returns:
  JSON-compatible content and an explicit ``is_error`` flag for failures the model can
  correct.
- :class:`~airflow.providers.common.ai.tools.ToolProvider` is anything with an
  ``airflow_tools()`` method returning ``AirflowTool`` objects.
  Every toolset this provider ships that reads from a connection implements it, except
  ``MCPToolset`` and the Agent Skills toolset. ``MCPToolset`` works in Pydantic AI agents
  and through the LangChain bridge; for Strands, ADK and other frameworks, connect the
  framework's own MCP client to the server.

An adapter maps these onto the framework's own tool type and error status, and makes
sure a ``ToolCallError`` ends the run rather than reaching the model, as the Strands
plugin does. :func:`~airflow.providers.common.ai.tools.collect_tools` turns the toolsets and
tools an adapter is given into one list, as the Strands and ADK adapters do.

Blocking hook calls, such as a SQL query, run one at a time in the task's process, so an
agent that calls two database tools at once gets its answers one after the other.

Run a Strands or ADK agent inside
:func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing` to keep its
spans tied to the task, and free of prompt text unless ``[common.ai] capture_content`` is
on; see :doc:`../observability`.

.. toctree::
    :hidden:
    :titlesonly:

    Pydantic AI <pydantic_ai>
    Strands Agents <strands>
    Google ADK <adk>
