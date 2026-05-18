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

.. _howto/operator:llamaindex_retrieval:

``RetrievalOperator``
=====================

Use :class:`~airflow.providers.common.ai.operators.llamaindex_retrieval.RetrievalOperator`
to retrieve relevant document chunks from a persisted LlamaIndex index. The
operator performs similarity search against the provided query and returns
results ready for downstream synthesis via ``LLMOperator``.

Basic Usage
-----------

Provide a query string and the path to a previously persisted index:

.. code-block:: python

    from airflow.providers.common.ai.operators.llamaindex_retrieval import RetrievalOperator

    retrieve = RetrievalOperator(
        task_id="retrieve_context",
        query="What are Airflow's key features?",
        index_persist_dir="/opt/airflow/data/my_index",
        llm_conn_id="openai_default",
    )

Query Templating
----------------

The ``query`` field supports Jinja templating, so it can be set dynamically
from upstream task output or Dag run configuration:

.. code-block:: python

    retrieve = RetrievalOperator(
        task_id="retrieve_context",
        query="{{ dag_run.conf['question'] }}",
        index_persist_dir="/opt/airflow/data/my_index",
        llm_conn_id="openai_default",
        top_k=10,
    )

Output Shape
------------

The operator returns a dict:

.. code-block:: python

    {
        "question": "What are Airflow's key features?",
        "chunks": [
            {
                "text": "Airflow provides ...",
                "score": 0.95,
                "metadata": {"source": "overview.txt"},
                "source": "node-abc123",
            },
            ...
        ],
    }

Each chunk includes ``text``, ``score`` (similarity), ``metadata``, and
``source`` (the LlamaIndex node ID). This output pairs naturally with
``LLMOperator`` for RAG synthesis using Jinja templates.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Default
     - Description
   * - ``query``
     - (required)
     - The query string to search for. Supports Jinja templating.
   * - ``index_persist_dir``
     - (required)
     - Path to the persisted LlamaIndex index directory.
   * - ``llm_conn_id``
     - ``pydanticai_default``
     - Airflow connection ID for the embedding API.
   * - ``embed_model``
     - ``text-embedding-3-small``
     - Embedding model name.
   * - ``top_k``
     - ``5``
     - Number of top results to retrieve.
