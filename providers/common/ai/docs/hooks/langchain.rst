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
LangChain's universal initialisers:

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

.. code-block:: python

    from airflow.providers.common.ai.hooks.langchain import LangChainHook
    from airflow.sdk import task


    @task
    def run_chain(query: str) -> str:
        hook = LangChainHook(
            llm_conn_id="langchain_default",
            llm_model="openai:gpt-4o",
        )
        llm = hook.get_chat_model()

        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate

        prompt = ChatPromptTemplate.from_template("Summarize: {query}")
        chain = prompt | llm | StrOutputParser()
        return chain.invoke({"query": query})

Supported chat providers
~~~~~~~~~~~~~~~~~~~~~~~~

Anything dispatchable by
`langchain.chat_models.init_chat_model <https://python.langchain.com/api_reference/langchain/chat_models/langchain.chat_models.base.init_chat_model.html>`__
works out of the box. Common identifiers:

- ``openai:gpt-4o``, ``openai:gpt-4o-mini`` -- requires ``langchain-openai``
- ``anthropic:claude-3-7-sonnet`` -- requires ``langchain-anthropic``
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

.. code-block:: python

    @task
    def build_index(texts: list[str]) -> None:
        hook = LangChainHook(
            llm_conn_id="langchain_default",
            embed_model="openai:text-embedding-3-small",
        )
        embeddings = hook.get_embedding_model()
        # ... vectors = embeddings.embed_documents(texts); build vector store, etc.

The same hook instance can serve both chat and embedding models when both
identifiers are set:

.. code-block:: python

    hook = LangChainHook(
        llm_conn_id="langchain_default",
        llm_model="openai:gpt-4o",
        embed_model="openai:text-embedding-3-small",
    )
    chat_model = hook.get_chat_model()
    embedding_model = hook.get_embedding_model()

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

.. code-block:: python

    hook = LangChainHook(
        llm_conn_id="openai_chat",
        embed_conn_id="openai_embed",
        llm_model="openai:gpt-4o",
        embed_model="openai:text-embedding-3-small",
    )
    chat_model = hook.get_chat_model()
    embedding_model = hook.get_embedding_model()

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

That extra pulls in ``langchain`` itself plus ``langchain-openai``. To use
other providers, install their LangChain integration package separately
(``pip install langchain-anthropic``, ``langchain-groq``,
``langchain-cohere``, ...).
