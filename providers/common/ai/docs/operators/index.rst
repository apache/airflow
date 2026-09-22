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

Choosing an operator
====================

By use case
-----------

The common-ai provider ships several operators (and matching ``@task`` decorators). Use this table
to pick the one that fits your use case:

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Need
     - Operator
     - Decorator
   * - Single prompt → text or structured output
     - :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`
     - ``@task.llm``
   * - Many prompts, ~half the cost, up to 24h turnaround (OpenAI/Anthropic batch APIs)
     - :class:`~airflow.providers.common.ai.operators.llm_batch.LLMBatchOperator`
     - ``@task.llm_batch``
   * - Analyze files, prefixes, images, or PDFs with one prompt
     - :class:`~airflow.providers.common.ai.operators.llm_file_analysis.LLMFileAnalysisOperator`
     - ``@task.llm_file_analysis``
   * - LLM picks which downstream task runs
     - :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
     - ``@task.llm_branch``
   * - Natural-language → SQL generation (no execution)
     - :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator`
     - ``@task.llm_sql``
   * - Compare schemas across data sources and detect drift
     - :class:`~airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator`
     - ``@task.llm_schema_compare``
   * - Multi-turn reasoning with tools (DB queries, API calls, etc.)
     - :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`
     - ``@task.agent``
   * - Parse files (PDF, DOCX, CSV, etc.) into document dicts for embedding
     - :class:`~airflow.providers.common.ai.operators.document_loader.DocumentLoaderOperator`
     - *(no decorator)*
   * - Chunk documents and produce embedding vectors
     - :class:`~airflow.providers.common.ai.operators.llamaindex_embedding.LlamaIndexEmbeddingOperator`
     - *(no decorator)*
   * - Retrieve relevant chunks from a vector index
     - :class:`~airflow.providers.common.ai.operators.llamaindex_retrieval.LlamaIndexRetrievalOperator`
     - *(no decorator)*

**LLMOperator / @task.llm** — stateless, single-turn calls. Use this for classification,
summarization, extraction, or any prompt that produces one response. Supports structured output
via an ``output_type`` Pydantic model.

**LLMBatchOperator / @task.llm_batch** — many prompts submitted as one provider batch job,
at roughly half the per-token cost with up to a 24-hour turnaround. Unlike every other operator
on this page, results are written to object storage, not XCom (see :doc:`llm_batch`) -- reach
for this when you have too many prompts to run one-at-a-time economically and can tolerate an
asynchronous turnaround.

**LLMFileAnalysisOperator / @task.llm_file_analysis** — stateless, single-turn file analysis.
Use this when the prompt should reason over file contents or multimodal attachments already chosen
by the Dag author. The operator resolves files via ``ObjectStoragePath`` and keeps the interaction
read-only.

**AgentOperator / @task.agent** — multi-turn tool-calling loop. The model decides which tools to
invoke and when to stop. Use this when the LLM needs to take actions (query databases, call APIs,
read files) to produce its answer. You configure available tools through ``toolsets``.

AgentOperator *works* without toolsets — pydantic-ai supports tool-less agents for multi-turn
reasoning — but if you don't need tools, ``LLMOperator`` is simpler and more explicit.

**DocumentLoaderOperator** — framework-agnostic file parsing. Use this to convert files
(text, CSV, JSON, PDF, DOCX) into ``list[dict(text, metadata)]`` for downstream embedding.
No AI framework dependency.

What the operators cover
------------------------

Use this provider when a Dag needs:

* **Generation, classification, summarization, or structured extraction** —
  :doc:`LLMOperator and @task.llm <llm>`, with Pydantic-typed output pushed to XCom.
* **Many prompts at half the price** — :doc:`LLMBatchOperator and @task.llm_batch <llm_batch>`
  submit prompts as one provider batch job (OpenAI or Anthropic), poll in deferrable mode for up to 24
  hours, re-attach on retry instead of paying twice, and land results as JSONL on object storage.
* **Branching on a model's decision** — :doc:`LLMBranchOperator <llm_branch>`.
* **Agents with tools** — :doc:`AgentOperator <agent>` runs a multi-turn agent loop
  in the worker, calling Airflow-defined :doc:`toolsets <../toolsets/index>` (SQL, hooks, MCP servers,
  a sandboxed shell and filesystem,
  :ref:`Agent Skills <agent-skills>`), optionally collapsed into a single sandboxed
  :ref:`code mode <code-mode>` call, with optional human-in-the-loop review and durable step
  replay — if the task retries after a failure, completed steps are replayed from cache
  instead of re-executing. Guardrails from the upstream ``pydantic-ai-shields`` package
  (``InputGuard``, ``OutputGuard``, ``ToolGuard``, ``CostTracking``) plug into the same agent
  loop (see :doc:`agent`).
* **Analyzing files or comparing schemas with an LLM** —
  :doc:`LLMFileAnalysisOperator <llm_file_analysis>` reads a file (object storage or
  local) into a prompt; :doc:`LLMSchemaCompareOperator <llm_schema_compare>` diffs
  schemas across systems and flags drift a plain equality check would miss.
* **Generating SQL from natural language** —
  :doc:`LLMSQLQueryOperator <llm_sql>` returns the generated query via XCom for
  ``SQLExecuteQueryOperator`` or a downstream task to run; it does not execute the query itself.
* **Document pipelines for RAG** —
  :doc:`DocumentLoaderOperator <document_loader>` parses files into structured text
  and metadata, :doc:`LlamaIndexEmbeddingOperator <llamaindex_embedding>` embeds it,
  and :doc:`LlamaIndexRetrievalOperator <llamaindex_retrieval>` retrieves the closest
  chunks for an :doc:`LLMOperator <llm>` prompt (see the table above for the
  full set).
