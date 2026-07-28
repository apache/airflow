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

LlamaIndex ``LlamaIndexEmbeddingOperator``
==========================================

Chunk a ``list[dict]`` of documents and produce embedding vectors using
LlamaIndex. Designed to feed the output of
:class:`~airflow.providers.common.ai.operators.document_loader.DocumentLoaderOperator`
into vector storage (pgvector, Pinecone, Weaviate, ...).

The operator calls the embedding model **directly** (and passes it to
``VectorStoreIndex(..., embed_model=...)`` when persisting) -- it does not
mutate LlamaIndex's global ``Settings`` singleton, so concurrent tasks in the
same worker process don't race on shared model state.

Basic usage
-----------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_hook.py
    :language: python
    :start-after: [START howto_hook_llamaindex_embed]
    :end-before: [END howto_hook_llamaindex_embed]

``documents`` is templated, so ``loader.output`` (XCom direct) is resolved
to a native ``list[dict]`` before ``execute`` runs.

Bring-your-own embedding model
------------------------------

LlamaIndex doesn't ship a universal embedding-model initializer, so the
operator's ``embed_model`` parameter accepts either:

* a string model name (e.g. ``"text-embedding-3-small"``) -- the operator
  constructs an ``OpenAIEmbedding`` via
  :class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook`
  using ``llm_conn_id`` / ``embed_conn_id``, or
* a pre-built ``BaseEmbedding`` instance -- bypass the hook entirely. Use
  this for Cohere, Bedrock, Vertex, HuggingFace, etc.:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_hook.py
    :language: python
    :start-after: [START howto_hook_llamaindex_byo_embed_model]
    :end-before: [END howto_hook_llamaindex_byo_embed_model]

Persisting to cloud storage
---------------------------

``persist_dir`` accepts local paths and storage URIs (``s3://``, ``gs://``,
``azure://``, ``file://``) resolved via
:class:`~airflow.sdk.ObjectStoragePath`. Pass ``persist_conn_id`` to
point at the Airflow connection that holds the cloud credentials:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_hook.py
    :language: python
    :start-after: [START howto_hook_llamaindex_cloud_persist]
    :end-before: [END howto_hook_llamaindex_cloud_persist]

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``documents``
     - ``list[dict]`` with ``text`` / ``metadata`` keys. Templated, so
       binding ``loader.output`` resolves to the native list before
       execute.
   * - ``embed_model``
     - String model name OR pre-built ``BaseEmbedding`` instance.
   * - ``llm_conn_id``
     - Airflow connection ID used when ``embed_model`` is a string. Falls
       back to ``LlamaIndexHook.default_conn_name`` (``llamaindex_default``)
       when ``None``.
   * - ``embed_conn_id``
     - Optional separate connection ID for the embedding provider. Falls
       back to ``llm_conn_id`` when ``None``.
   * - ``chunk_size``
     - Sentence-splitter chunk size (default 512).
   * - ``chunk_overlap``
     - Overlap between chunks (default 50).
   * - ``persist_dir``
     - Local path or storage URI to persist the LlamaIndex index.
   * - ``persist_conn_id``
     - Cloud credentials connection ID for ``persist_dir`` URIs.

Output
------

Returns a dict with::

    {
        "document_count": int,
        "chunk_count": int,
        "persist_dir": str | None,
        "chunks": [
            {"text": str, "metadata": dict, "vector": list[float]},
            ...
        ],
    }

``vector`` is computed over the chunk's metadata-enriched content
(LlamaIndex's ``MetadataMode.EMBED``, the same content ``VectorStoreIndex``
embeds), while ``text`` is the raw chunk text without metadata.
