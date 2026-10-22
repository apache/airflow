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

.. _howto/features:

Feature map
===========

Most of the provider's operators and decorators run
`Pydantic AI <https://ai.pydantic.dev/>`__ agents, with the model taken from an Airflow
connection. A feature that calls a model through those connections also needs the extra
for your model vendor (:doc:`model_providers`); the **Extra** column lists what it needs
on top of that, and a bare name there is an extra of this provider. :doc:`installation`
lists the features that need a newer Airflow than the provider's 2.11, and
:doc:`stability` says which features are stable.

Run a model or an agent
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`@task.llm <operators/llm>`
     - Sends one prompt and returns the answer as text or a typed object.
     - None
   * - :doc:`@task.agent <operators/agent>`
     - Runs an agent that calls tools until it has an answer.
     - None
   * - :doc:`@task.llm_branch <operators/llm_branch>`
     - Lets the model pick which downstream tasks run.
     - None
   * - :doc:`@task.llm_sql <operators/llm_sql>`
     - Writes a SQL query from a question and checks it, without running it.
     - ``sql``; for files, ``apache-airflow-providers-common-sql[datafusion]``
   * - :doc:`@task.llm_file_analysis <operators/llm_file_analysis>`
     - Sends files from object storage, including images and PDFs, to the model with a
       prompt.
     - ``parquet`` or ``avro`` for those formats
   * - :doc:`@task.llm_schema_compare <operators/llm_schema_compare>`
     - Compares table schemas across databases and files and grades each mismatch.
     - ``common.sql``; for files, ``apache-airflow-providers-common-sql[datafusion]``
   * - :doc:`@task.llm_batch <operators/llm_batch>`
     - Sends many prompts through the OpenAI or Anthropic batch API at about half the
       per-token price.
     - None
   * - :doc:`Document and RAG pipelines <rag_pipelines>`
     - Loads documents, embeds them and retrieves the passages closest to a question.
     - ``llamaindex``, and install the vendor's package for vendors other than OpenAI;
       ``pdf`` or ``docx`` for those formats

Choose a model
--------------

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`Models and providers <model_providers>`
     - Takes the vendor, model and key from a connection, so changing vendor is a
       connection edit.
     - The vendor's, such as ``openai`` or ``anthropic``; the linked page lists each
   * - :doc:`Self-hosted models <self_hosted_models>`
     - Runs tasks against a model server you host, such as Ollama or vLLM.
     - ``openai``
   * - :doc:`Decision models <decision_models>`
     - Uses a model that cannot write text, such as TypeSafe's hosted Jev or Ollama's
       decision models, to answer a typed question with one value from a set you
       define, plus a confidence.
     - Jev: ``typesafe``, and install ``pydantic-ai-slim`` 2.45+; others: install
       ``pydantic-ai-slim`` 2.53+
   * - :doc:`LlamaIndex models <hooks/llamaindex>`
     - Builds LlamaIndex language and embedding models from a connection.
     - ``llamaindex``, and install the vendor's package for vendors other than OpenAI

Give an agent tools
-------------------

:doc:`toolsets/index` compares what each toolset reaches and how you limit it;
:doc:`agent_security` covers what an agent holding them can reach, with a production
checklist.

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`HookToolset <toolsets/hook>`
     - Turns the methods you list on any Airflow hook into tools.
     - None
   * - :doc:`SQLToolset <toolsets/sql>`
     - Queries a database, read-only by default, and only the tables in
       ``allowed_tables`` when you set it.
     - ``sql``
   * - :doc:`ObjectStorageToolset <toolsets/object_storage>`
     - Lists and reads files under one object storage path.
     - None
   * - :doc:`MCPToolset <toolsets/mcp>`
     - Gives the agent the tools of an MCP server.
     - ``mcp``
   * - :doc:`DataFusionToolset <toolsets/datafusion>`
     - Runs SQL over Parquet, CSV and Avro files and Iceberg tables, in the worker.
     - ``sql``, plus ``apache-airflow-providers-common-sql[datafusion]``, and its
       ``apache.iceberg`` extra for Iceberg
   * - :doc:`AgentSkillsToolset <toolsets/skills>`
     - Loads ``SKILL.md`` instruction bundles when the model asks for one.
     - ``skills``; on Airflow 2, install ``pydantic-ai-skills>=1.2.0,<2``
   * - :doc:`SandboxToolset <sandbox/index>`
     - Gives the agent a shell and a filesystem in an isolated sandbox: a hosted
       service, a server you run, or a microVM on your machine for local development.
     - ``modal``, ``islo``, ``opensandbox``, ``openshell`` or ``boat``, by backend;
       none for ``sbx``
   * - :doc:`ManagedAgentToolset <toolsets/managed_agent>`
     - Sends a prompt to an agent on Bedrock AgentCore or Vertex AI Agent Engine.
     - The Amazon or Google provider's ``common.ai`` extra
   * - :doc:`Progressive tool disclosure <toolsets/progressive_disclosure>`
     - Hides a toolset's tools from the model until it searches for them, with
       ``.defer_loading()`` on any toolset.
     - None

Control output and spend
------------------------

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`Structured output <structured_output>`
     - Returns a Pydantic object instead of text, on ``@task.llm``,
       ``@task.llm_file_analysis`` and ``@task.agent``.
     - None
   * - :ref:`Usage limits <agent-usage-budget>`
     - Fails the task past a cap on requests, tokens, tool calls or estimated cost (for
       models pydantic-ai can price), on ``@task.agent`` and every ``@task.llm*`` task
       except ``@task.llm_batch``.
     - None

Shape an agent run
------------------

These configure ``@task.agent`` and ``AgentOperator``.

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`Message history <message_history>`
     - Continues a conversation from an earlier run.
     - None
   * - :doc:`Capabilities and guardrails <capabilities>`
     - Turns on model reasoning (``Thinking``), web search (``WebSearch``) and other
       Pydantic AI capabilities, and input and output guards.
     - ``shields`` for guards
   * - :doc:`Code mode <code_mode>`
     - Lets the model call several tools from one Python snippet.
     - ``code-mode``
   * - :doc:`Agent memory <memory>`
     - Pydantic AI's ``Memory`` capability keeps notes the agent writes in one Dag run
       and reads in the next.
     - None; install ``pydantic-ai-harness``
   * - :ref:`Prompt caching <agent-prompt-caching>`
     - Marks the repeated start of each request for caching on Anthropic, and on the
       Bedrock and OpenRouter models that support it, so later requests pay less for it;
       OpenAI and Gemini cache on their own. On by default.
     - None

Put a Human in the loop
------------------------

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`Approve outputs <approval_gates>`
     - Pauses any ``@task.llm*`` task except ``@task.llm_batch`` until a person approves
       or rejects the output, optionally editing it.
     - None
   * - :ref:`Confidence gate <approval-gates:uncertain>`
     - Sends the answer to a person, or fails the task, when the model's confidence is
       under a bar you set. Only decision models report a confidence; with a text model
       every answer counts as uncertain. On ``@task.llm`` and ``@task.llm_branch``.
     - As for decision models
   * - :doc:`Review agent sessions <hitl_review>`
     - Lets a reviewer send a ``@task.agent`` answer back with feedback, round after
       round.
     - None
   * - :doc:`Approve tool calls <tool_approval>`
     - Pauses ``@task.agent`` before a marked tool runs, until a person approves or
       rejects the call.
     - None

Recover from failures
---------------------

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`Durable execution <durable_execution>`
     - With ``durable=True`` on ``@task.agent``, or the ``AirflowDurability`` capability
       on your own agent, a retry replays the model and tool calls that finished instead
       of paying for them again.
     - None
   * - :doc:`Retry policies <retry_policies>`
     - Has a model read a failure and decide whether the task retries, and when. Works
       on any task.
     - None
   * - :doc:`Provider fallback <provider_fallback>`
     - Moves to the next connection in ``fallback_conn_ids`` when a vendor's API
       returns an error.
     - None

Bring your own agent
--------------------

The Strands, ADK, Anthropic SDK, OpenAI Agents SDK, Claude Agent SDK and other-framework
routes get every toolset except
``MCPToolset`` and ``AgentSkillsToolset``.

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`Pydantic AI agent <frameworks/pydantic_ai>`
     - Gives an agent you run in a ``@task`` a model from a connection and the toolsets.
     - None
   * - :doc:`Strands Agents <frameworks/strands>`
     - Gives a Strands agent the toolsets. Run through ``invoke_durably``, it resumes on
       retry.
     - None; install ``strands-agents``
   * - :doc:`Google ADK <frameworks/adk>`
     - Gives an ADK agent the toolsets.
     - None; install ``google-adk``
   * - :doc:`Anthropic SDK tool runner <frameworks/anthropic>`
     - Gives a loop on the Anthropic SDK's ``tool_runner`` the toolsets.
     - ``anthropic``
   * - :doc:`OpenAI Agents SDK <frameworks/openai_agents>`
     - Gives an OpenAI Agents SDK agent the toolsets. Run through ``run_durably``, a retry
       replays its completed calls.
     - None; install ``openai-agents``
   * - :doc:`Claude Agent SDK <frameworks/claude_agent_sdk>`
     - Gives the Claude Agent SDK's agent loop the toolsets through an in-process MCP
       server.
     - ``claude-agent-sdk``
   * - :doc:`LangChain <hooks/langchain>`
     - Builds LangChain chat and embedding models from a connection, and
       :doc:`turns a toolset into LangChain tools <toolsets/langchain>`.
     - ``langchain``, and install ``langchain-<vendor>``
   * - :ref:`Any other framework <howto/frameworks:other>`
     - Gives an agent in any framework the toolsets, through a framework-neutral tool
       interface.
     - None

See what ran
------------

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Feature
     - What it does
     - Extra
   * - :doc:`Tracing <observability>`
     - Exports model and tool calls as OpenTelemetry spans.
     - None
   * - :ref:`AI Model tab <howto/observability:model-tab>`
     - Shows the model on the task instance page of ``@task.llm`` and ``@task.agent``
       tasks, and their token counts and an estimated cost.
     - None

.. toctree::
    :hidden:
    :titlesonly:

    Structured output <structured_output>
    Message history <message_history>
    Agent memory <memory>
    Capabilities and guardrails <capabilities>
    Code mode <code_mode>
    Approve outputs <approval_gates>
    Review agent sessions <hitl_review>
    Approve tool calls <tool_approval>
