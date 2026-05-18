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
bridge Airflow connections to LlamaIndex's ``Settings`` singleton. The hook
reuses the ``pydanticai`` connection type, so users configure a single
connection for both pydantic-ai operators and LlamaIndex operators.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

What It Does
------------

The hook resolves API keys and base URLs from Airflow connections and uses
them to configure LlamaIndex's embedding models, LLMs, and global settings.
This eliminates manual ``Settings.embed_model = ...`` boilerplate in every
task that uses LlamaIndex.

Configuration
-------------

``LlamaIndexHook`` reuses the ``pydanticai`` connection type. Set the API key
in the **Password** field and optionally a custom endpoint in the **Host**
field.

Separate Embedding and LLM Connections
--------------------------------------

RAG pipelines often use different providers for embeddings and chat. The hook
supports an optional ``embed_conn_id`` parameter that defaults to the main
``llm_conn_id``:

.. code-block:: python

    from airflow.providers.common.ai.hooks.llamaindex import LlamaIndexHook

    hook = LlamaIndexHook(
        llm_conn_id="openai_default",
        embed_conn_id="embedding_provider",
        embed_model="text-embedding-3-large",
        llm_model="gpt-4o",
    )
    hook.configure_settings()

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
     - Airflow connection ID for the LLM/embedding provider.
   * - ``embed_conn_id``
     - Same as ``llm_conn_id``
     - Separate connection for embeddings (optional).
   * - ``embed_model``
     - ``text-embedding-3-small``
     - Embedding model name.
   * - ``llm_model``
     - ``None``
     - LLM model name. Required for ``get_llm()`` and ``configure_settings()``
       LLM setup.
