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

.. _howto/hook:llamaindex:

``LlamaIndexHook``
==================

Use :class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook` to
bridge an Airflow connection to `LlamaIndex <https://docs.llamaindex.ai/>`__
chat and embedding models. The hook reads credentials (API key, optional
base URL) from a connection of type ``llamaindex`` and returns native
LlamaIndex objects ready to pass to ``VectorStoreIndex(..., embed_model=...)``,
``load_index_from_storage(..., embed_model=...)``, or
``index.as_retriever(..., llm=...)``.

The hook deliberately does **not** mutate LlamaIndex's global ``Settings``
singleton. Operators pass the resolved model directly to LlamaIndex
constructors, so concurrent tasks in the same worker don't race on shared
state.

OpenAI by default, BYO for other vendors
----------------------------------------

LlamaIndex does not ship a universal ``init_chat_model`` /
``init_embedding_model`` equivalent (each vendor is a separate package
under ``llama-index-llms-*`` / ``llama-index-embeddings-*`` with its own
constructor kwargs). The hook therefore covers the OpenAI-compatible
surface that matches LlamaIndex's own ``resolve_embed_model("default")``
behaviour:

- ``hook.get_embedding_model()`` returns an ``OpenAIEmbedding`` configured
  from the connection.
- ``hook.get_llm()`` returns an ``OpenAI`` LLM configured from the
  connection.

For other vendors (Cohere, Bedrock, Vertex AI, HuggingFace, ...),
instantiate the LlamaIndex class directly in a ``@task`` and pass it to
the operator's ``embed_model=`` / ``llm=`` parameter -- both
:class:`~airflow.providers.common.ai.operators.llamaindex_embedding.LlamaIndexEmbeddingOperator`
and
:class:`~airflow.providers.common.ai.operators.llamaindex_retrieval.LlamaIndexRetrievalOperator`
accept a pre-built ``BaseEmbedding`` / ``LLM`` instance and bypass the
hook:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llamaindex_hook.py
    :language: python
    :start-after: [START howto_hook_llamaindex_byo_embed_model]
    :end-before: [END howto_hook_llamaindex_byo_embed_model]

Install the per-vendor LlamaIndex integration package separately:
``pip install llama-index-embeddings-cohere``, ``...-bedrock``,
``...-huggingface``, ``llama-index-llms-anthropic``, etc.

Connection Configuration
------------------------

The hook reads credentials from the Airflow connection of type ``llamaindex``:

- **password** -- API key (passed as ``api_key`` to ``OpenAIEmbedding`` /
  ``OpenAI``).
- **host** -- Optional base URL (passed as ``api_base``; useful for custom
  OpenAI-compatible endpoints, Ollama, vLLM).
- **extra** JSON --
  ``{"embed_model": "text-embedding-3-small", "llm_model": "gpt-4o"}`` --
  default model identifiers stored on the connection.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Parameter
     - Default
     - Description
   * - ``llm_conn_id``
     - ``llamaindex_default``
     - Airflow connection ID for the LLM/embedding provider.
   * - ``embed_conn_id``
     - ``None`` (falls back to ``llm_conn_id``)
     - Optional separate Airflow connection ID for the embedding provider.
   * - ``embed_model``
     - ``None`` (falls back to ``extra["embed_model"]``)
     - Embedding model name, e.g. ``text-embedding-3-small``.
   * - ``llm_model``
     - ``None`` (falls back to ``extra["llm_model"]``)
     - LLM model name, e.g. ``gpt-4o``. Required when calling ``get_llm()``.

Dependencies
------------

Install the ``llamaindex`` extra::

    pip install apache-airflow-providers-common-ai[llamaindex]

That extra installs ``llama-index-core``, ``llama-index-embeddings-openai``,
and ``llama-index-llms-openai`` -- enough to back the hook's default
OpenAI return values. For other LlamaIndex vendor packages, install
their integration package separately.
