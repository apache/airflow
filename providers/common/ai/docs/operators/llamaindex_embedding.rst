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

.. _howto/operator:llamaindex_embedding:

``EmbeddingOperator``
=====================

Use :class:`~airflow.providers.common.ai.operators.llamaindex_embedding.EmbeddingOperator`
to chunk documents and produce embedding vectors using LlamaIndex. This operator
bridges document loading (Airflow provider hooks returning text) and vector
storage (pgvector, Pinecone, Weaviate ingest operators).

Basic Usage
-----------

Provide a list of documents with ``text`` and ``metadata`` keys. The operator
chunks the documents, embeds them, and returns the results:

.. code-block:: python

    from airflow.providers.common.ai.operators.llamaindex_embedding import EmbeddingOperator

    embed = EmbeddingOperator(
        task_id="embed_docs",
        documents=[
            {"text": "Airflow is a workflow orchestration platform.", "metadata": {"source": "docs"}},
            {"text": "LlamaIndex is a data framework for LLM applications.", "metadata": {"source": "docs"}},
        ],
        llm_conn_id="openai_default",
    )

Connection Configuration
------------------------

The operator uses :class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook`
internally. Configure your embedding API credentials via the ``pydanticai``
connection type.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Chunking Parameters
-------------------

Control how documents are split into chunks before embedding:

.. code-block:: python

    embed = EmbeddingOperator(
        task_id="embed_docs",
        documents=documents,
        llm_conn_id="openai_default",
        chunk_size=256,
        chunk_overlap=25,
    )

Index Persistence
-----------------

Set ``persist_dir`` to save the LlamaIndex index for later retrieval via
:class:`~airflow.providers.common.ai.operators.llamaindex_retrieval.RetrievalOperator`:

.. code-block:: python

    embed = EmbeddingOperator(
        task_id="embed_docs",
        documents=documents,
        llm_conn_id="openai_default",
        persist_dir="/opt/airflow/data/my_index",
    )

Output Shape
------------

The operator returns a dict:

.. code-block:: python

    {
        "document_count": 2,
        "chunk_count": 5,
        "persist_dir": "/opt/airflow/data/my_index",
        "chunks": [
            {"text": "chunk text", "metadata": {"source": "docs"}, "vector": [0.1, ...]},
            ...
        ],
    }

Each chunk includes ``text``, ``metadata``, and optionally ``vector`` (the
embedding array). The ``chunks`` list is ready for downstream consumption by
vector store ingest operators.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Default
     - Description
   * - ``documents``
     - (required)
     - List of dicts with ``text`` and ``metadata`` keys.
   * - ``llm_conn_id``
     - ``pydanticai_default``
     - Airflow connection ID for the embedding API.
   * - ``embed_model``
     - ``text-embedding-3-small``
     - Embedding model name.
   * - ``chunk_size``
     - ``512``
     - Chunk size for the sentence splitter.
   * - ``chunk_overlap``
     - ``50``
     - Overlap between chunks.
   * - ``persist_dir``
     - ``None``
     - Directory path to persist the index.
