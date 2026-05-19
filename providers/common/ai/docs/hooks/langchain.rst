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

Use :class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook`
to bridge Airflow connections to LangChain model constructors.  The hook
extracts credentials from an Airflow connection and returns configured
LangChain model objects (``ChatOpenAI``, ``OpenAIEmbeddings``).

The hook reuses the ``pydanticai`` connection type, so users configure a
single connection for PydanticAI operators, LlamaIndex operators, and
LangChain tasks.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

Use the hook in a ``@task`` function to get a configured chat model:

.. code-block:: python

    from airflow.providers.common.ai.hooks.langchain import LangChainHook

    @task
    def run_chain(query: str) -> str:
        hook = LangChainHook(llm_conn_id="pydanticai_default", llm_model="gpt-4o")
        llm = hook.get_chat_model()

        from langchain_core.prompts import ChatPromptTemplate
        from langchain_core.output_parsers import StrOutputParser

        prompt = ChatPromptTemplate.from_template("Summarize: {query}")
        chain = prompt | llm | StrOutputParser()
        return chain.invoke({"query": query})

Embedding Models
----------------

Use :meth:`~LangChainHook.get_embedding_model` for embeddings.  A
separate ``embed_conn_id`` can be used when embedding and chat models
use different API keys:

.. code-block:: python

    hook = LangChainHook(
        llm_conn_id="chat_conn",
        embed_conn_id="embed_conn",
        embed_model="text-embedding-3-large",
        llm_model="gpt-4o",
    )
    embeddings = hook.get_embedding_model()
    chat_model = hook.get_chat_model()

Connection Configuration
------------------------

The hook reads credentials from the Airflow connection:

- **password** -- API key (passed as ``api_key`` to model constructors)
- **host** -- Base URL (passed as ``base_url``; optional, for custom
  endpoints or Ollama)

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Default
     - Description
   * - ``llm_conn_id``
     - ``pydanticai_default``
     - Airflow connection ID for the LLM provider.
   * - ``embed_conn_id``
     - ``None`` (falls back to ``llm_conn_id``)
     - Separate connection for embeddings.
   * - ``embed_model``
     - ``text-embedding-3-small``
     - Embedding model name.
   * - ``llm_model``
     - ``None``
     - Chat model name.  Required for ``get_chat_model()``.

Dependencies
------------

Install the ``langchain`` extra to use this hook::

    pip install apache-airflow-providers-common-ai[langchain]
