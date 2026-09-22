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

.. _howto/concepts:

Core concepts
=============

The provider connects Airflow to a model through a connection, runs the call or the agent
loop inside a task, gives agents tools through toolsets, and returns results through XCom.

Connections choose the model
----------------------------

``common.ai`` is built on `pydantic-ai <https://ai.pydantic.dev/>`__, so the model vendor
(OpenAI, Anthropic, Google, Bedrock, …) is picked by the connection ``llm_conn_id`` points
at — switching providers later is a connection change, not a Dag rewrite. Most connections
use the generic ``pydanticai`` type, but Azure OpenAI, Bedrock, and Vertex AI also have their
own connection types (``pydanticai_azure``, ``pydanticai_bedrock``, ``pydanticai_vertex``) for
provider-specific authentication.

The model name lives on the connection in ``provider:model`` form, and ``model_id`` on an
operator overrides it. :doc:`connections/pydantic_ai` has the full resolution order, and
:doc:`provider_fallback` explains how one connection can name others to fail over to.

Operators and decorators do the work
------------------------------------

Every operator ships with a matching ``@task`` decorator, so a Dag can use whichever style
it already uses. ``LLMOperator`` sends one prompt and returns one answer. ``AgentOperator``
runs a multi-turn loop in which the model calls tools until it is done. The other
operators are specializations of the first: branching on the answer, analyzing a file,
generating SQL, comparing schemas, or submitting many prompts as one batch.
:doc:`operators/index` has the selection table.

The AI step is orchestrated by Airflow: the model calls, the agent loop, and any tools
run in the Airflow worker by default, where they get retries, logging, and observability like
any other task. The exception is :ref:`SandboxToolset <sandbox-limitations>`, which exists so
that code the *model* writes runs somewhere else.

Toolsets give agents reach
--------------------------

A toolset is what an agent is allowed to call. The provider ships toolsets that wrap
Airflow hooks, SQL databases, files through DataFusion, MCP servers, Agent Skills, a
sandboxed shell, and vendor-managed agents. An agent's reach is exactly the toolsets you
register on it. :doc:`toolsets/index` compares them and :doc:`agent_security` explains
the defense layers.

Existing LangChain tools are not locked out either: pydantic-ai ships
``pydantic_ai.ext.langchain.LangChainToolset`` upstream, which wraps LangChain tools for a
common.ai agent, and the provider's own
:func:`~airflow.providers.common.ai.toolsets.langchain_bridge.airflow_toolset_to_langchain_tools`
converts the other way — Airflow-managed toolsets into LangChain tools (see
:doc:`toolsets/langchain`).

Hooks are the plumbing underneath
---------------------------------

The provider's hooks bridge an Airflow connection to a specific framework's model objects.
Each hook is a thin adapter: it reads credentials and config from the connection, then
returns native framework objects (a ``pydantic_ai`` ``Agent`` / ``Model``, a LangChain
``BaseChatModel`` or ``Embeddings``, an MCP client, ...). Operators and ``@task`` decorators
use these hooks internally, and you reach for one directly only when you want the
framework object in a plain ``@task``.

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Hook
     - When to use
   * - :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
     - Default for ``common.ai`` operators (``LLMOperator``, ``AgentOperator``,
       ``LLMBranchOperator``, ...). Returns a pydantic-ai ``Agent`` / ``Model``.
       See :doc:`hooks/pydantic_ai`.
   * - :class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook`
     - Direct LangChain access for tasks that compose ``Runnable``\\s, use the
       LangChain agent surface, or need LangChain-native chat / embedding model
       objects. Independent of the pydantic-ai-backed operators.
       See :doc:`hooks/langchain`.
   * - :class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook`
     - Backs the LlamaIndex ``LlamaIndexEmbeddingOperator`` and
       ``LlamaIndexRetrievalOperator``.
       Returns LlamaIndex-native ``BaseEmbedding`` / ``LLM`` objects (OpenAI
       by default). For non-OpenAI vendors, pass a pre-built
       ``BaseEmbedding`` / ``LLM`` instance straight to the operator and
       bypass the hook. See :doc:`hooks/llamaindex`.
   * - :class:`~airflow.providers.common.ai.hooks.mcp.MCPHook`
     - Backs ``MCPToolset`` (see :doc:`toolsets/mcp`) for agent tasks that call
       tools on a remote MCP server. Configure the connection via
       :doc:`connections/mcp`. See :doc:`hooks/mcp`.

Results flow through XCom
-------------------------

Every operator pushes its result to XCom like any other task. A plain string arrives as a
string. A Pydantic ``output_type`` arrives as the model instance, typed, so a downstream
task can use attribute access. :doc:`structured_output` explains how the class is
registered for deserialization and where that stops working.

Related providers
-----------------

Use a vendor's own provider instead when the Dag needs that vendor's **native API surface**, a
service the vendor runs for you, which no vendor-neutral operator wraps:

* :doc:`apache-airflow-providers-openai:index` — the Embeddings and Responses APIs, and Batch
  jobs built from a pre-uploaded JSONL file of raw request bodies.
* :doc:`apache-airflow-providers-anthropic:index` — Message Batches built from raw Messages
  API request bodies (multi-turn, images, tools), and Managed Agents sessions where the agent
  loop runs on Anthropic's infrastructure rather than in the Airflow worker.
* :doc:`apache-airflow-providers-cohere:index` — Cohere's own Embed API.
* :doc:`apache-airflow-providers-google:index` — Vertex AI's Batch Prediction jobs
  (``CreateBatchPredictionJobOperator``), a managed batch service like OpenAI's Batch API.
* :doc:`apache-airflow-providers-amazon:index` — Bedrock's Batch Inference
  (``BedrockBatchInferenceOperator``), and Bedrock AgentCore's managed agent runtime
  (``BedrockCreateAgentRuntimeOperator`` / ``BedrockInvokeAgentRuntimeOperator``), where the
  agent loop runs on AWS's infrastructure rather than in the Airflow worker.

The :doc:`landing page <index>` has the decision table and the rule of thumb that separates
the two.
