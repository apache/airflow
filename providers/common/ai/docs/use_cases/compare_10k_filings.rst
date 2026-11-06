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

Compare companies' 10-K filings
===============================

An analyst wants to know which of several companies carries the most concentrated risk and
which is growing fastest, grounded in the filings rather than the model's memory. A weekly
Dag fetches each latest 10-K from SEC EDGAR and indexes it. An on-demand Dag has the model
split the question per company, retrieves against each index in its own task so a missing
index retries alone, and writes a structured report. A human edits the question on the way
in and approves the report on the way out.

Two variants build the same graph: LlamaIndex operators, or LangChain with FAISS.

What this demonstrates
----------------------

* :doc:`../operators/llamaindex_embedding` and
  :doc:`../operators/llamaindex_retrieval` -- index per ticker on a schedule, retrieve
  per sub-question on demand.
* :doc:`../operators/llm` -- ``@task.llm`` with structured output for decomposition, and
  ``LLMOperator`` bounded by ``UsageLimits`` for synthesis.
* :doc:`apache-airflow:authoring-and-scheduling/dynamic-task-mapping` -- the model decides
  how many sub-questions there are; the Dag maps over them at runtime.
* :doc:`apache-airflow-providers-standard:operators/hitl` -- ``HITLEntryOperator`` on the
  way in, ``ApprovalOperator`` on the way out.
* :doc:`../hooks/langchain` -- the LangChain variant does indexing and retrieval in plain
  ``@task`` functions over ``LangChainHook`` and FAISS.

Run it
------

1. Install the provider with the LlamaIndex extra (or ``langchain`` for the other
   variant):

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai,llamaindex]"

2. Create a ``llamaindex`` connection named ``llamaindex_default`` for embedding and
   retrieval (see :doc:`../connections/llamaindex`); ``pydanticai_default`` does
   decomposition and synthesis.

3. Set ``EDGAR_USER_AGENT`` in the file to your name and email. SEC requires a contact
   address on every EDGAR request. No API key is needed.

4. Run the indexing Dag once, then trigger the analysis:

   .. code-block:: bash

       airflow dags test example_llamaindex_10k_index
       airflow dags test example_llamaindex_10k_analysis

The run pauses at ``analyst_input`` to confirm the question and tickers, and at
``review_report``. Answer both from Required Actions in the UI (see the note on
:doc:`index`). The ``synthesize_report`` XCom holds the ``AnalysisReport``.

The Dags share only the index path ``INDEX_BASE_DIR/<lowercased ticker>``, so index a
ticker before analyzing it.

The indexing Dag
----------------

One mapped ``LlamaIndexEmbeddingOperator`` per ticker, weekly:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py
    :language: python
    :start-after: [START example_llamaindex_10k_index]
    :end-before: [END example_llamaindex_10k_index]

The analysis Dag
----------------

The output types come first. ``DecomposedQuestion`` is what the model returns from the
decomposition step, and ``AnalysisReport`` is what the reviewer approves:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py
    :language: python
    :start-after: [START 10k_structured_output]
    :end-before: [END 10k_structured_output]

The Dag itself:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_10k.py
    :language: python
    :start-after: [START example_llamaindex_10k_analysis]
    :end-before: [END example_llamaindex_10k_analysis]

Decomposition is the step to notice: the model returns a list and the Dag maps retrieval
over it, so the model decides at runtime how many tasks run, each with its own log and
retries.

Adapting it
-----------

* Change ``DEFAULT_TICKERS`` or pass ``tickers`` as a Dag param. Any US-listed company
  works; EDGAR resolves the ticker.
* Replace ``fetch_filings`` with your own document source; nothing downstream cares where
  the text came from.
* Tighten ``UsageLimits`` on ``synthesize_report`` to cap spend per run.
* The LangChain build is ``example_langchain_10k.py`` (Dag ids ``example_langchain_10k_index``
  and ``example_langchain_10k_analysis``); it adds a ``langchain_default`` connection for
  embeddings.
