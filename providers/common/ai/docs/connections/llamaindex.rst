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

.. _howto/connection:llamaindex:

LlamaIndex connection
=====================

The ``llamaindex`` connection type configures access to LLM and embedding
providers for `LlamaIndex <https://docs.llamaindex.ai/>`__. It backs
:class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook` (see
:doc:`../hooks/llamaindex` for hook usage and installation instructions).

Default Connection IDs
----------------------

The ``LlamaIndexHook`` uses ``llamaindex_default`` by default.

Configuring the Connection
---------------------------

Embedding Model (Extra field)
    Default LlamaIndex embedding model name (e.g. ``text-embedding-3-small``).
    This field appears as a dedicated input in the connection form
    (via ``conn-fields``) and stores its value in ``extra["embed_model"]``.

LLM Model (Extra field)
    Default LlamaIndex LLM model name (e.g. ``gpt-5``). This field appears
    as a dedicated input in the connection form (via ``conn-fields``) and
    stores its value in ``extra["llm_model"]``.

API Key (Password field)
    The API key for your LLM/embedding provider, passed as ``api_key=`` to
    the LlamaIndex model constructor.

Host (optional)
    Optional base URL, passed as ``api_base=`` (for example, to point at an
    OpenAI-compatible proxy that serves official OpenAI model names).

The ``schema``, ``port``, and ``login`` fields are hidden in the connection
form; they are not used by this connection type.

OpenAI models only
------------------

``get_llm()`` and ``get_embedding_model()`` return LlamaIndex's ``OpenAI`` and
``OpenAIEmbedding`` classes whatever ``host`` points at, and both classes check the
model name against LlamaIndex's own OpenAI model lists. Local or self-hosted servers
(Ollama, vLLM and similar) are therefore not usable through this connection type
unless they answer to an official OpenAI model name. For other vendors and for local
models, build the LlamaIndex class in a ``@task`` and pass it to the operator's
``embed_model=`` / ``llm=`` parameter; :doc:`../hooks/llamaindex` explains the check
and shows the pattern.

Model resolution order
-----------------------

Both ``get_embedding_model()`` and ``get_llm()`` resolve the model
identifier from, in order:

1. The ``embed_model`` / ``llm_model`` constructor argument on
   ``LlamaIndexHook``.
2. ``extra["embed_model"]`` / ``extra["llm_model"]`` on the connection.

If neither is set, the hook raises a ``ValueError`` when the model is needed.

Examples
--------

**OpenAI (embeddings and LLM)**

.. code-block:: json

    {
        "conn_type": "llamaindex",
        "password": "sk-...",
        "extra": "{\"embed_model\": \"text-embedding-3-small\", \"llm_model\": \"gpt-5\"}"
    }

**LLM only (embeddings unset)**

.. code-block:: json

    {
        "conn_type": "llamaindex",
        "password": "sk-...",
        "extra": "{\"llm_model\": \"gpt-5\"}"
    }
