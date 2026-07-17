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

End-to-End Pipelines
=====================

The Dags in this guide combine several patterns from the operator and hook guides into one
production-shaped pipeline. Each section explains the architecture -- how the Dags are split,
why, and how the pieces are wired together -- rather than the mechanics of a single operator,
which the linked guides already cover. Read the full source for the runnable Dag.

LlamaIndex RAG shapes
-----------------------

`example_llamaindex_rag.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_rag.py>`__
walks the same load -> embed -> retrieve -> answer pattern through three shapes, from simplest to
production-shaped:

- ``example_llamaindex_rag_pipeline`` (``schedule=None``) -- everything in one Dag:
  ``DocumentLoaderOperator`` parses local files, ``LlamaIndexEmbeddingOperator`` chunks and
  persists the index, ``LlamaIndexRetrievalOperator`` retrieves for a fixed question, and an
  ``LLMOperator`` synthesizes the answer.
- ``example_llamaindex_index_pdf`` / ``example_llamaindex_query`` -- the load/embed step moved
  into its own weekly Dag (``schedule="@weekly"``) so the index stays fresh as PDFs arrive, while
  a second Dag (``schedule=None``, triggered with a ``question`` param) retrieves and answers on
  demand against the persisted index -- the same index/query split the SEC 10-K pipelines below
  use, without their multi-company retrieval fan-out.
- ``example_llamaindex_multi_source`` -- two ``DocumentLoaderOperator`` calls tag documents from
  different sources via ``metadata_fields`` before merging and embedding them into one index, for
  filtered retrieval downstream.

See :doc:`operators/document_loader`, :doc:`operators/llamaindex_embedding`, and
:doc:`operators/llamaindex_retrieval` for how the individual operators work.

SEC 10-K financial analysis
----------------------------

`example_llamaindex_10k.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py>`__ and
`example_langchain_10k.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_langchain_10k.py>`__
compare companies' SEC 10-K filings, fetched live from EDGAR. Each file splits the work into two
Dags:

- An **indexing Dag** (``schedule="@weekly"``) that fetches each company's latest 10-K and
  persists an index per ticker to disk.
- An **analysis Dag** (``schedule=None``, triggered manually or on demand) that decomposes a
  comparison question, retrieves against the indexes, and produces a reviewed report.

The two Dags are not linked by an Asset or XCom -- they agree only on a shared on-disk index
path (``INDEX_BASE_DIR/{ticker}``). Run the indexing Dag at least once before triggering
analysis for a given company.

Both files build the same analysis shape; the only structural difference is that LlamaIndex has
dedicated ``LlamaIndexEmbeddingOperator``/``LlamaIndexRetrievalOperator`` classes, while
LangChain does not yet, so the LangChain file's indexing and retrieval steps are plain ``@task``
functions calling :class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook` and FAISS
directly.

Analysis Dag flow:

1. A human confirms or edits the comparison question and the tickers to compare
   (``HITLEntryOperator``).
2. ``@task.llm`` decomposes the question into one sub-question per company -- the LLM decides how
   many sub-questions are needed, not the Dag author:

   .. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py
       :language: python
       :start-after: [START 10k_decompose]
       :end-before: [END 10k_decompose]

3. Dynamic Task Mapping fans retrieval out, one mapped task per sub-question, each querying that
   company's index:

   .. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py
       :language: python
       :start-after: [START 10k_dtm_retrieval]
       :end-before: [END 10k_dtm_retrieval]

4. The mapped results are zipped back to their sub-questions (mapped task outputs preserve input
   order) and synthesized into a structured ``AnalysisReport`` by an ``LLMOperator`` bounded by
   ``UsageLimits``.
5. ``ApprovalOperator`` gates the report before hand-off.

See :doc:`operators/llamaindex_embedding`, :doc:`operators/llamaindex_retrieval`, and
:doc:`hooks/langchain` for how the individual operators/hooks behave.

Natural-language survey analysis
----------------------------------

`example_llm_survey_analysis.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_analysis.py>`__
answers natural-language questions over a CSV with the same core three tasks --
``LLMSQLQueryOperator`` generates SQL, ``AnalyticsOperator`` runs it, a ``@task`` extracts the
rows (see :doc:`operators/llm_sql` for how those two operators work together). The file defines
two Dags around that core, shaped for different operating modes:

- ``example_llm_survey_interactive`` (``schedule=None``) -- a human confirms or edits the
  question before SQL generation (``HITLEntryOperator``) and approves the extracted result
  afterward (``ApprovalOperator``). It assumes the CSV is already in place.
- ``example_llm_survey_scheduled`` (``schedule="@monthly"``) -- downloads the CSV itself
  (``HttpOperator``), validates its schema against a reference CSV
  (``LLMSchemaCompareOperator``) before generating SQL, and ends by emailing or logging the
  result. No human gate -- suited to recurring reporting.

Agentic multi-dimensional survey synthesis
---------------------------------------------

`example_llm_survey_agentic.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_agentic.py>`__
answers a research question that a single SQL query cannot -- one that spans several
dimensions (executor, deployment, cloud, Airflow version) -- by fanning both SQL generation and
execution out with Dynamic Task Mapping, one mapped pair per dimension, then synthesizing:

1. ``decompose_question`` returns one sub-question per dimension.
2. ``LLMSQLQueryOperator.partial(...).expand(prompt=sub_questions)`` generates SQL for each
   dimension as an independent mapped task instance.
3. ``AnalyticsOperator.partial(...).expand(...)`` runs each query. If one dimension's query
   fails, only that mapped instance retries -- the other three keep their results.
4. ``collect_results`` zips the dimension labels back onto the (order-preserved) results.
5. An ``LLMOperator`` synthesizes the four labeled result sets into one narrative, then
   ``ApprovalOperator`` gates it.

The Dag's own docstring explains the reasoning for fanning out rather than looping inside a
single LLM call: each sub-query becomes a named, logged task instance instead of a hidden tool
call, so a failure is isolated and retryable, and every intermediate result is visible in XCom
instead of being an opaque step inside an LLM reasoning loop.

AIP progress tracking: pipeline vs. agent
--------------------------------------------

`example_aip_progress_tracker.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py>`__
tracks the same thing -- progress on a set of Airflow Improvement Proposals, checked against
Confluence specs and GitHub activity -- with two different architectures, to compare the tradeoff directly:

- ``example_aip_progress_tracker`` -- a **deterministic pipeline**. Evidence is gathered by
  fixed tasks; dynamically-mapped ``LLMOperator`` calls analyze each AIP with structured output;
  the per-AIP analyses are synthesized into one report; a second ``LLMOperator`` validates that
  synthesis against the raw evidence and flags unsupported claims; a plain-Python task applies
  only the flagged corrections mechanically (string/regex replacement, no LLM involved); a human
  reviews the corrected report.
- ``example_aip_progress_tracker_skills`` -- an **autonomous agent**. A single ``AgentOperator``
  loaded with an Agent Skill (``AgentSkillsToolset``) and a handful of custom tool functions
  decides its own evidence-gathering strategy and tool-call order, then the same human review
  gate.

The pipeline's three-layer defense against hallucination is the pattern worth taking away:
structured LLM output, then a second LLM call whose only job is to judge the first against the
original evidence, then a deterministic (non-LLM) step that applies exactly what was flagged:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py
    :language: python
    :start-after: [START aip_tracker_validation]
    :end-before: [END aip_tracker_validation]

The agent variant collapses that whole graph into one operator call, trading auditability of
each step for simplicity and letting the model decide how to use its tools:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py
    :language: python
    :start-after: [START aip_tracker_skills_operator]
    :end-before: [END aip_tracker_skills_operator]

Reach for the pipeline when accuracy matters and every step needs to be auditable; reach for the
agent when the problem is open-ended enough that a fixed task graph would just be re-encoding
what the model can already work out from the skill instructions.
