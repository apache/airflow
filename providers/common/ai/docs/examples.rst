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

Example Dags
============

Every operator, decorator and integration has a runnable Dag under
`example_dags <https://github.com/apache/airflow/tree/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags>`__,
listed here by operator. Guides embed the Dags they walk through; the rest link to source.

For what to build rather than how, start at :doc:`use_cases/index`.

Single-prompt tasks
--------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Guide
     - What it shows
   * - :doc:`operators/llm`
     - Summarize and extract entities, grade incident severity, and
       :doc:`triage a queue of support tickets <use_cases/triage_support_tickets>` one mapped
       task at a time
       (`example_llm.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm.py>`__,
       `example_llm_classification.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_classification.py>`__,
       `example_llm_analysis_pipeline.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_analysis_pipeline.py>`__).
   * - :doc:`operators/llm_branch`
     - Let the model pick which downstream task runs, and
       :doc:`route a failed task to rerun, page or ignore <use_cases/route_pipeline_failures>`
       with a confidence bar.
   * - :doc:`operators/llm_file_analysis`
     - ``@task.llm_file_analysis`` reasoning over files, images, and PDFs.
   * - :doc:`operators/llm_schema_compare`
     - Compare two schemas and
       :doc:`block a load when they drifted <use_cases/gate_loads_on_schema_drift>`.
   * - :doc:`operators/llm_sql`
     - ``@task.llm_sql`` generating SQL from a natural-language question.

Batch processing
-----------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Guide
     - What it shows
   * - :doc:`operators/llm_batch`
     - :doc:`Classify thousands of reviews at half the price <use_cases/classify_reviews_in_bulk>`
       through one OpenAI or Anthropic batch job, with structured output and results landed on
       object storage
       (`example_llm_batch.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py>`__).

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
   * - :doc:`use_cases/research_agent_with_review`
     - A LangChain ReAct agent that decides its own tool calls, composed with ``LLMOperator`` for
       report formatting and AIP-90 HITL review
       (`example_langchain_tool_agent.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_langchain_tool_agent.py>`__).

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

By use case
-----------

Dags written around a job. Each has a page under :doc:`use_cases/index` with the Dag
embedded and steps to run it.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Use case
     - Source
   * - :doc:`use_cases/ask_questions_over_pdfs`
     - `example_llamaindex_rag.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_rag.py>`__: a weekly indexing Dag
       plus an on-demand query Dag, with single-Dag and multi-source variants.
   * - :doc:`use_cases/compare_10k_filings`
     - `example_llamaindex_10k.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py>`__ and
       `example_langchain_10k.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_langchain_10k.py>`__: live SEC EDGAR filings,
       per-company retrieval fan-out, human review at both ends. One variant per RAG library.
   * - :doc:`use_cases/monthly_report_from_a_csv`
     - `example_llm_survey_analysis.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_analysis.py>`__: download,
       schema check, generated SQL, email; plus an interactive variant with HITL.
       `example_llm_survey_agentic.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_agentic.py>`__ fans a
       multi-dimensional question out one SQL query per dimension.
   * - :doc:`use_cases/explain_revenue_anomaly`
     - `example_sandbox_toolset.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py>`__: an agent with a
       read-only warehouse toolset and a sandbox for the arithmetic.
   * - :doc:`use_cases/weekly_status_report`
     - `example_aip_progress_tracker.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py>`__: the same
       report built as a deterministic pipeline with a hallucination check and as one
       autonomous agent.
   * - :doc:`use_cases/research_agent_with_review`
     - `example_langchain_tool_agent.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_langchain_tool_agent.py>`__: a LangChain
       ReAct agent between a question-review gate and a report-approval gate.

Reliability
-----------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Guide
     - What it shows
   * - :doc:`retry_policies`
     - Classifying task failures with an LLM into categories you define, then deriving
       retry, fail, or delay from the category; and the same on a classifier model with a
       confidence bar. Source:
       `example_llm_retry_policy.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_retry_policy.py>`__.
   * - :doc:`provider_fallback`
     - Failing over to another vendor inside one task attempt, and drilling the chain
       without waiting for an outage. Source:
       `example_llm_fallback.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_fallback.py>`__.
   * - :doc:`classifier_models`
     - Routing a failure with a model that answers typed questions instead of writing
       text, and escalating when its confidence is low. Source:
       `example_classifier_model.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_classifier_model.py>`__.
