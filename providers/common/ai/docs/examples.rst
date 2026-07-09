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

.. _howto/examples:

Examples
========

Every operator, decorator, and integration in this provider has a runnable Dag under
`example_dags <https://github.com/apache/airflow/tree/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags>`__.
This page groups them by scenario. Guides embed the same Dags inline where a step-by-step
walkthrough exists; the rest are linked directly to source below.

New to the provider? Start with :doc:`operators/index` to pick an operator, then come back here
for a worked example of the pattern you need.

Single-prompt tasks
--------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Guide
     - What it shows
   * - :doc:`operators/llm`
     - ``@task.llm`` for text, structured output, classification, and dynamic task mapping over
       LLM results
       (`example_llm.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm.py>`__,
       `example_llm_classification.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_classification.py>`__,
       `example_llm_analysis_pipeline.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_analysis_pipeline.py>`__).
   * - :doc:`operators/llm_branch`
     - ``@task.llm_branch`` picking which downstream task runs.
   * - :doc:`operators/llm_file_analysis`
     - ``@task.llm_file_analysis`` reasoning over files, images, and PDFs.
   * - :doc:`operators/llm_schema_compare`
     - ``@task.llm_schema_compare`` comparing two schemas with an LLM.
   * - :doc:`operators/llm_sql`
     - ``@task.llm_sql`` generating SQL from a natural-language question.

Agents & tools
--------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Guide
     - What it shows
   * - :doc:`operators/agent`
     - ``AgentOperator`` / ``@task.agent`` multi-turn tool use, durable execution, and pydantic-ai
       capabilities
       (`example_agent.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_agent.py>`__,
       `example_agent_durable.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py>`__,
       `example_agent_capabilities.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py>`__).
   * - :ref:`Toolsets <howto/toolsets>`
     - Loading ``SKILL.md`` Agent Skills
       (`example_agent_skills.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py>`__)
       and exposing an Airflow toolset to a LangChain agent, the reverse bridge
       (`example_langchain_toolset_bridge.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_langchain_toolset_bridge.py>`__).
   * - :doc:`connections/mcp`
     - Connecting an agent to an MCP server through an Airflow connection.
   * - :doc:`hitl_review`
     - Adding a human-in-the-loop review gate to agent output.
   * - `example_langchain_tool_agent.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_langchain_tool_agent.py>`__
     - A LangChain ReAct agent that decides its own tool calls, composed with ``LLMOperator`` for
       report formatting and AIP-90 HITL review.

Retrieval & document processing
--------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Guide
     - What it shows
   * - :doc:`operators/document_loader`
     - Parsing PDF, DOCX, CSV, and JSON into ``list[dict]`` for embedding.
   * - :doc:`hooks/pydantic_ai`
     - Calling ``PydanticAIHook`` and a pydantic-ai ``Agent`` directly.
   * - :doc:`hooks/langchain`
     - ``LangChainHook`` chat-only, embedding-only, and combined patterns.
   * - :doc:`hooks/llamaindex`
     - ``LlamaIndexHook`` plus the embedding and retrieval operators.

End-to-end scenarios
---------------------

Production-shaped Dags that combine several of the patterns above into one pipeline. See
:doc:`end_to_end_pipelines` for the architecture behind each one.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Example
     - What it shows
   * - `example_llamaindex_rag.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_rag.py>`__
     - Three RAG shapes with LlamaIndex: a single load-embed-retrieve-answer Dag, a production-shaped
       split into scheduled indexing plus on-demand query Dags, and multi-source RAG.
   * - `example_llamaindex_10k.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py>`__,
       `example_langchain_10k.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_langchain_10k.py>`__
     - SEC 10-K financial analysis against live EDGAR filings: one Dag indexes filings on a
       schedule, the other decomposes a comparison question at runtime and fans out retrieval with
       Dynamic Task Mapping. One variant per RAG library.
   * - `example_llm_survey_analysis.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_analysis.py>`__
     - Natural-language querying of a CSV via ``LLMSQLQueryOperator`` and ``AnalyticsOperator``,
       interactive and scheduled variants.
   * - `example_llm_survey_agentic.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_agentic.py>`__
     - The same survey data, analyzed by fanning a multi-dimensional research question into an
       agentic multi-query synthesis pipeline.
   * - `example_aip_progress_tracker.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py>`__
     - The same use case -- tracking Airflow Improvement Proposal progress -- solved two ways, a
       deterministic pipeline and an autonomous agent, to compare the tradeoff.

Reliability
-----------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Guide
     - What it shows
   * - :doc:`retry_policies`
     - Classifying task failures with an LLM to decide retry, fail, or delay. Source:
       `example_llm_retry_policy.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_retry_policy.py>`__.

.. toctree::
    :hidden:
    :maxdepth: 1

    end_to_end_pipelines
