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

LlamaIndex ``LlamaIndexRetrievalOperator``
==========================================

Load a persisted LlamaIndex index and run similarity search. Designed to
sit between
:class:`~airflow.providers.common.ai.operators.llamaindex_embedding.LlamaIndexEmbeddingOperator`
(which builds the index) and
:class:`~airflow.providers.common.ai.operators.llm.LLMOperator` (which
synthesises an answer from the retrieved chunks).

Passes the embedding model **directly** to
``load_index_from_storage(..., embed_model=...)`` -- no LlamaIndex
``Settings`` mutation. The embedding model must match the one used when
the index was originally built.

Basic usage
-----------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_hook.py
    :language: python
    :start-after: [START howto_hook_llamaindex_retrieve]
    :end-before: [END howto_hook_llamaindex_retrieve]

``query`` is templated, so DAG-run params, XCom, and Variables all flow
through cleanly.

Cloud-persisted indexes
-----------------------

``index_persist_dir`` accepts the same local-path-or-URI shape as
``LlamaIndexEmbeddingOperator.persist_dir``. Pass ``persist_conn_id`` to point at
the Airflow connection that holds cloud credentials. The operator raises
``FileNotFoundError`` with a clear "did you run LlamaIndexEmbeddingOperator first?"
message when the path is missing.

Bring-your-own embedding model
------------------------------

Same shape as ``LlamaIndexEmbeddingOperator``: ``embed_model`` accepts either a
string model name (OpenAI via the hook) or a pre-built ``BaseEmbedding``
instance for non-OpenAI vendors. See the BYO example in
:doc:`llamaindex_embedding`.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``query``
     - The query string. Templated.
   * - ``index_persist_dir``
     - Local path or storage URI pointing at the persisted index.
       Templated.
   * - ``persist_conn_id``
     - Cloud credentials connection ID for ``index_persist_dir`` URIs.
       Templated.
   * - ``embed_model``
     - String model name OR pre-built ``BaseEmbedding`` instance. Must
       match the model used when the index was built. Templated.
   * - ``llm_conn_id``
     - Airflow connection ID used when ``embed_model`` is a string. Falls
       back to ``LlamaIndexHook.default_conn_name`` (``llamaindex_default``)
       when ``None``.
   * - ``embed_conn_id``
     - Optional separate connection ID for the embedding provider. Falls
       back to ``llm_conn_id`` when ``None``.
   * - ``top_k``
     - Number of top similarity results to return (default 5).

Output
------

Returns a dict with::

    {
        "query": str,
        "chunks": [
            {
                "text": str,
                "score": float,
                "metadata": dict,
                "node_id": str,
            },
            ...
        ],
    }
