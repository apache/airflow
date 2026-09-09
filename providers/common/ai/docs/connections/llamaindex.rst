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

LlamaIndex Connection
======================

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
    Default LlamaIndex LLM model name (e.g. ``gpt-4o``). This field appears
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

OpenAI models only, BYO for other vendors
------------------------------------------

``LlamaIndexHook.get_embedding_model()`` always returns an ``OpenAIEmbedding``
instance, and ``get_llm()`` always returns an ``OpenAI`` LLM instance,
regardless of the ``host`` you set. Setting ``host`` to point at a different
server does not relax any validation -- each class validates the model name
against its own built-in list: a chat/completion-model list
(``ALL_AVAILABLE_MODELS``, e.g. ``gpt-4o``) for ``OpenAI``, and a separate,
much smaller embedding-model list
(``OpenAIEmbeddingModelType``, e.g. ``text-embedding-3-small``) for
``OpenAIEmbedding``. The two lists mostly do not overlap -- current-generation
names such as ``gpt-4o`` or ``text-embedding-3-small`` are only valid for one
of the two classes -- though a handful of legacy names (``ada``, ``babbage``,
``curie``, ``davinci``) happen to appear in both. The classes differ only in
*when* their respective check runs:

* ``OpenAIEmbedding`` validates the model name in its constructor, so
  ``get_embedding_model()`` raises immediately for a name not in its list.
* ``OpenAI`` (the LLM class) accepts any model name string at construction
  time, but validates it lazily on first use, inside its ``metadata``
  property. Any call that touches ``metadata`` -- including ``.chat()`` and
  ``.complete()`` -- raises a ``ValueError`` for a name not in its list.
  There is no constructor argument on either class that overrides this
  check (no ``context_window=`` / ``is_chat_model=`` argument).

In practice this means local or self-hosted models (Ollama, vLLM, and
similar) are not usable through this connection type, even via ``host=``,
unless the server is configured to answer to an official OpenAI model name.
For other vendors and for local models, instantiate the LlamaIndex class
directly in your ``@task`` and pass it to the operator's ``embed_model=`` /
``llm=`` parameter -- this bypasses the hook and this connection type
entirely (see :doc:`../hooks/llamaindex`).

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
        "extra": "{\"embed_model\": \"text-embedding-3-small\", \"llm_model\": \"gpt-4o\"}"
    }

**LLM only (embeddings unset)**

.. code-block:: json

    {
        "conn_type": "llamaindex",
        "password": "sk-...",
        "extra": "{\"llm_model\": \"gpt-4o\"}"
    }
