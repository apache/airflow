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

.. _howto/connection:langchain:

LangChain Connection
====================

The ``langchain`` connection type configures access to LLM providers via
`LangChain <https://python.langchain.com/>`__'s universal
``init_chat_model`` / ``init_embeddings`` entry points. It backs
:class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook` (see
:doc:`../hooks/langchain` for hook usage and installation instructions).

Default Connection IDs
----------------------

The ``LangChainHook`` uses ``langchain_default`` by default.

Configuring the Connection
---------------------------

Chat Model (Extra field)
    Chat model identifier in ``provider:name`` format, dispatched via
    ``langchain.chat_models.init_chat_model`` (e.g. ``openai:gpt-4o``,
    ``anthropic:claude-sonnet-5``). This field appears as a dedicated input
    in the connection form (via ``conn-fields``) and stores its value in
    ``extra["model"]``.

Embedding Model (Extra field)
    Embedding model identifier in ``provider:name`` format, dispatched via
    ``langchain.embeddings.init_embeddings`` (e.g.
    ``openai:text-embedding-3-small``). This field appears as a dedicated
    input in the connection form (via ``conn-fields``) and stores its value
    in ``extra["embed_model"]``.

    The connection-type definition documents ``cohere:embed-english-v3.0``
    as an example of the ``provider:name`` format, but the hook only forwards
    ``api_key`` / ``base_url`` to ``init_embeddings`` -- vendors with bespoke
    embedding auth such as Cohere are not covered by this connection type yet
    (see :ref:`Supported providers <langchain-supported-providers>` below and
    :doc:`../hooks/langchain`).

API Key (Password field)
    The API key for your LLM provider, passed as ``api_key=`` to
    ``init_chat_model`` / ``init_embeddings``.

Host (optional)
    Optional base URL, passed as ``base_url=`` (custom OpenAI-compatible
    endpoints, Ollama, vLLM).

The ``schema``, ``port``, and ``login`` fields are hidden in the connection
form; they are not used by this connection type.

.. _langchain-supported-providers:

Supported providers
--------------------

Only OpenAI-compatible providers work with this hook's ``api_key`` +
optional ``base_url`` credential surface: OpenAI, Anthropic, Groq,
Mistral AI, DeepSeek, Ollama, and vLLM. Providers with bespoke auth (AWS
Bedrock, Google Vertex AI / GenAI, Azure OpenAI, Cohere, HuggingFace) reject
these kwargs and are not usable through this connection type.

Model resolution order
-----------------------

Both ``get_chat_model()`` and ``get_embedding_model()`` resolve the model
identifier from, in order:

1. The ``llm_model`` / ``embed_model`` constructor argument on ``LangChainHook``.
2. ``extra["model"]`` / ``extra["embed_model"]`` on the connection.

If neither is set, the hook raises a ``ValueError`` when the model is needed.

Examples
--------

**OpenAI (chat and embeddings)**

.. code-block:: json

    {
        "conn_type": "langchain",
        "password": "sk-...",
        "extra": "{\"model\": \"openai:gpt-4o\", \"embed_model\": \"openai:text-embedding-3-small\"}"
    }

**Anthropic (chat only)**

.. code-block:: json

    {
        "conn_type": "langchain",
        "password": "sk-ant-...",
        "extra": "{\"model\": \"anthropic:claude-sonnet-5\"}"
    }

**Ollama (local, custom endpoint)**

.. code-block:: json

    {
        "conn_type": "langchain",
        "host": "http://localhost:11434/v1",
        "extra": "{\"model\": \"ollama:llama3\"}"
    }
