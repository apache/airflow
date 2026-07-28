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

.. _howto/hook:langchain:

``LangChainHook``
=================

Use :class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook` to
bridge an Airflow connection to `LangChain <https://python.langchain.com/>`__
chat and embedding models. The hook reads credentials (API key, optional base
URL) from the connection and returns configured LangChain model objects via
two universal entry-point functions:

- ``langchain.chat_models.init_chat_model`` for chat models, dispatching to
  the right vendor based on the ``provider:name`` prefix.
- ``langchain.embeddings.init_embeddings`` for embedding models, same
  dispatch story.

The hook owns its own ``langchain`` connection type so the UI is honest about
which framework a connection configures.

Chat model usage
----------------

Pass ``llm_model`` to the constructor (or set ``extra["model"]`` on the
connection) and call ``get_chat_model()``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_langchain_hook.py
    :language: python
    :start-after: [START howto_hook_langchain_chat]
    :end-before: [END howto_hook_langchain_chat]

The returned model is a LangChain ``BaseChatModel``, so it composes with the
rest of LangChain's runnable surface
(``ChatPromptTemplate`` / ``StrOutputParser`` / ``RunnableSequence`` / ...).

Supported chat providers
~~~~~~~~~~~~~~~~~~~~~~~~

Any model identifier accepted by
`langchain.chat_models.init_chat_model <https://python.langchain.com/api_reference/langchain/chat_models/langchain.chat_models.base.init_chat_model.html>`__
works out of the box. Common identifiers:

- ``openai:gpt-4o``, ``openai:gpt-4o-mini`` -- requires ``langchain-openai``
- ``anthropic:claude-sonnet-5`` -- requires ``langchain-anthropic``
- ``groq:llama-3.3-70b-versatile`` -- requires ``langchain-groq``
- ``mistralai:mistral-large-latest`` -- requires ``langchain-mistralai``
- ``ollama:llama3`` -- requires ``langchain-ollama`` (point ``host`` at the Ollama URL)
- ``deepseek:deepseek-chat`` -- requires ``langchain-deepseek``

Cloud providers with non-standard auth (AWS Bedrock, Google Vertex AI, Azure
OpenAI) are not covered by the ``api_key`` + ``base_url`` surface here and are
deferred to per-vendor hooks (mirroring the pydantic-ai cloud-auth subclass
pattern).

Embedding model usage
---------------------

Pass ``embed_model`` to the constructor (or set ``extra["embed_model"]`` on
the connection) and call ``get_embedding_model()``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_langchain_hook.py
    :language: python
    :start-after: [START howto_hook_langchain_embedding]
    :end-before: [END howto_hook_langchain_embedding]

The same hook instance can serve both chat and embedding models when both
identifiers are set:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_langchain_hook.py
    :language: python
    :start-after: [START howto_hook_langchain_chat_and_embedding]
    :end-before: [END howto_hook_langchain_chat_and_embedding]

Supported embedding providers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The hook passes ``api_key`` and (optional) ``base_url`` from the connection to
`langchain.embeddings.init_embeddings <https://reference.langchain.com/python/langchain/embeddings/base/init_embeddings>`__.
Providers whose embedding classes accept this kwarg shape work directly:

- ``openai:text-embedding-3-small``, ``openai:text-embedding-3-large`` -- requires ``langchain-openai``
- ``openai:<model>`` against an OpenAI-compatible endpoint (point ``host`` at
  Ollama / vLLM / LM Studio) -- requires ``langchain-openai``

``init_embeddings`` advertises more providers (Cohere, Mistral AI, HuggingFace,
Bedrock, Vertex AI, Azure OpenAI, ...), but their embedding classes expect
provider-specific credential kwargs (``cohere_api_key``, AWS auth chain, GCP
service-account, ...) rather than the generic ``api_key`` / ``base_url`` this
hook forwards. Those are deferred to per-vendor subclasses mirroring the
pydantic-ai pattern (``PydanticAIBedrockHook`` / ``PydanticAIVertexHook`` /
``PydanticAIAzureHook``).

Different connections for chat and embeddings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If chat and embeddings live on different API keys (e.g. premium chat key vs
free-tier embeddings key), pass an explicit ``embed_conn_id``. When unset it
falls back to ``llm_conn_id``, so the common one-provider case stays simple:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_langchain_hook.py
    :language: python
    :start-after: [START howto_hook_langchain_different_conns]
    :end-before: [END howto_hook_langchain_different_conns]

Connection Configuration
------------------------

The hook reads credentials from the Airflow connection of type ``langchain``:

- **password** -- API key (passed as ``api_key`` to ``init_chat_model`` and
  ``init_embeddings``).
- **host** -- Optional base URL (passed as ``base_url``; useful for custom
  OpenAI-compatible endpoints, Ollama, vLLM).
- **extra** JSON -- ``{"model": "openai:gpt-4o", "embed_model": "openai:text-embedding-3-small"}``
  to set default chat and embedding model identifiers on the connection.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Parameter
     - Default
     - Description
   * - ``llm_conn_id``
     - ``langchain_default``
     - Airflow connection ID for the LLM provider.
   * - ``embed_conn_id``
     - ``None`` (falls back to ``llm_conn_id``)
     - Optional separate Airflow connection ID for the embedding provider.
       Useful when chat and embeddings live on different API keys; in the
       common one-provider case, leave unset and the hook reuses ``llm_conn_id``.
   * - ``llm_model``
     - ``None`` (falls back to ``extra["model"]`` on the connection)
     - Chat model identifier in ``provider:name`` form, e.g. ``openai:gpt-4o``.
       Only required when calling ``get_chat_model()``.
   * - ``embed_model``
     - ``None`` (falls back to ``extra["embed_model"]`` on the connection)
     - Embedding model identifier in ``provider:name`` form, e.g.
       ``openai:text-embedding-3-small``. Only required when calling
       ``get_embedding_model()``.

Dependencies
------------

Install the ``langchain`` extra to use this hook::

    pip install apache-airflow-providers-common-ai[langchain]

That extra installs only ``langchain`` itself, since the framework is
vendor-agnostic. Install the LangChain integration package for whichever
provider(s) you intend to use:

- ``langchain-openai`` -- OpenAI and OpenAI-compatible endpoints (Ollama, vLLM)
- ``langchain-anthropic`` -- Anthropic
- ``langchain-groq``, ``langchain-mistralai``, ``langchain-deepseek``, ``langchain-ollama``, ...
