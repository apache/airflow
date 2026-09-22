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

LangChain bridge
================

Tools bridge in both directions between common.ai's toolsets and LangChain.

**LangChain tools → ``AgentOperator``.** No Airflow code is needed. pydantic-ai
ships `pydantic_ai.ext.langchain.LangChainToolset
<https://ai.pydantic.dev/toolsets/>`__ upstream, which wraps existing LangChain
tools as an ``AbstractToolset``. Drop it straight into ``AgentOperator``:

.. code-block:: python

    from pydantic_ai.ext.langchain import LangChainToolset

    AgentOperator(
        task_id="agent_with_langchain_tools",
        prompt="Research the question and summarise.",
        llm_conn_id="pydanticai_default",
        toolsets=[LangChainToolset([my_langchain_tool])],
    )

**common.ai toolsets → LangChain.** The reverse direction is what
:func:`~airflow.providers.common.ai.toolsets.langchain_bridge.airflow_toolset_to_langchain_tools`
provides. It converts any pydantic-ai toolset -- including ``SQLToolset``,
``HookToolset``, and ``MCPToolset`` -- into a list of LangChain
``StructuredTool`` objects, so a LangChain agent or chain can call Airflow's
curated, connection-managed tools:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_langchain_toolset_bridge.py
    :language: python
    :start-after: [START example_langchain_toolset_bridge]
    :end-before: [END example_langchain_toolset_bridge]

Each generated tool keeps the source tool's name, description, and argument
schema, and routes calls back through the original toolset, so the toolset's own
behavior (connection resolution, ``SQLToolset``'s SQL validation, and
``allowed_tables`` filtering) still applies. ``get_tools`` runs eagerly at
conversion time to enumerate the tools.

When a toolset raises pydantic-ai's ``ModelRetry`` to ask the model to correct
its input (``SQLToolset`` does this on, for example, an unknown column), the
bridge returns that message as the tool's output so the model sees it and tries
again. ``ModelRetry`` is a feed-the-model-and-retry signal rather than a
failure, so returning it preserves the self-correction the toolset was written
for and works no matter how the agent is configured to handle tool errors
(raising would abort the run under ``create_agent``'s default handling).

The bridge does not hold a toolset session open across calls: ``get_tools`` and
every tool call each run under their own event loop, so for ``MCPToolset`` the
connection is opened and torn down around each call. It reconnects per call,
which is fine for stateless tools but unsuitable for ``stdio`` MCP servers (or
any server that keeps state between calls), since each call starts a fresh
session.

.. note::

    Outside an agent run there is no live ``RunContext``, so the bridge builds a
    minimal one with an inert placeholder model. The bundled toolsets ignore the
    context, so this is transparent for them. A custom toolset that reads live
    run state (``ctx.model``, ``ctx.messages``, ``ctx.usage``) will not behave
    correctly when bridged standalone.

Requires the ``langchain`` extra:
``pip install "apache-airflow-providers-common-ai[langchain]"``
