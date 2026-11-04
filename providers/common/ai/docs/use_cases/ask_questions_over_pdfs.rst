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

Ask questions over a growing PDF corpus
=======================================

A folder of quarterly reports keeps growing and people keep asking questions answered
somewhere inside it. One Dag keeps a vector index fresh weekly; the other answers a question
on demand from retrieved excerpts, citing them by number. Indexing runs on a schedule so no
query pays for embedding, and ``question`` is a param, so anyone can trigger it from the UI,
CLI or REST API.

What this demonstrates
----------------------

* :doc:`../operators/document_loader` -- ``DocumentLoaderOperator`` parses a glob of PDFs
  into documents.
* :doc:`../operators/llamaindex_embedding` -- ``LlamaIndexEmbeddingOperator`` chunks,
  embeds and persists the index to disk.
* :doc:`../operators/llamaindex_retrieval` -- ``LlamaIndexRetrievalOperator`` pulls the
  top matching chunks for a question.
* :doc:`../operators/llm` -- ``LLMOperator`` answers from the excerpts only, with the
  question and the context templated in from params and XCom.

Run it
------

1. Install the provider with the LlamaIndex and PDF extras:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai,llamaindex,pdf]"

2. Create a ``llamaindex`` connection named ``llamaindex_default`` for embedding and
   retrieval (see :doc:`../connections/llamaindex`); ``pydanticai_default`` writes the answer.

3. Put some PDFs under ``/opt/airflow/data/reports/``, run the indexing Dag once, then
   ask a question:

   .. code-block:: bash

       airflow dags test example_llamaindex_index_pdf
       airflow dags test example_llamaindex_query \
           --conf '{"question": "What drove the change in operating margin?"}'

The ``synthesize`` XCom holds the answer with ``[n]`` references into the excerpts that
``format_context`` numbered.

The indexing Dag
----------------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_rag.py
    :language: python
    :start-after: [START howto_llamaindex_index_dag]
    :end-before: [END howto_llamaindex_index_dag]

The query Dag
-------------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_rag.py
    :language: python
    :start-after: [START howto_llamaindex_query_dag]
    :end-before: [END howto_llamaindex_query_dag]

Two more shapes
---------------

``example_llamaindex_rag_pipeline`` runs the same four operators in one Dag with a fixed
question. Run it once to see the chain end to end.

``example_llamaindex_multi_source`` loads from two places, tags each document with where
it came from through ``metadata_fields``, and embeds them into one index so retrieval can
filter by source later:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_rag.py
    :language: python
    :start-after: [START howto_llamaindex_multi_source]
    :end-before: [END howto_llamaindex_multi_source]

Adapting it
-----------

* Change ``source_path`` to your folder or an object storage URI; the loader also reads
  DOCX, CSV and JSON.
* Change the indexing schedule to match how often documents land, or trigger it from an
  Asset when the upstream Dag that drops the PDFs emits one.
* Raise ``top_k`` for broad questions, lower it for precise ones. Keep the excerpts-only
  instruction in the prompt; it stops the model filling gaps from memory.
* :doc:`compare_10k_filings` grows this into a fan-out over several indexes with a human at
  both ends.
