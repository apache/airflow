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

Common AI Hooks
===============

The common-ai provider ships hooks that bridge an Airflow connection to a specific
LLM framework's model objects. Each hook is a thin adapter: it reads credentials and
config from the connection, then returns native framework objects (a ``pydantic_ai``
``Agent`` / ``Model``, a LangChain ``BaseChatModel`` or ``Embeddings``, an MCP client,
...). Operators and ``@task`` decorators in this provider use these hooks internally.

Choosing a hook
---------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Hook
     - When to use
   * - :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
     - Default for ``common.ai`` operators (``LLMOperator``, ``AgentOperator``,
       ``LLMBranchOperator``, ...). Returns a pydantic-ai ``Agent`` / ``Model``.
   * - :class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook`
     - Direct LangChain access for tasks that compose ``Runnable``\\s, use the
       LangChain agent surface, or need LangChain-native chat / embedding model
       objects. Independent of the pydantic-ai-backed operators.
   * - :class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook`
     - Backs the LlamaIndex ``LlamaIndexEmbeddingOperator`` and
       ``LlamaIndexRetrievalOperator``.
       Returns LlamaIndex-native ``BaseEmbedding`` / ``LLM`` objects (OpenAI
       by default). For non-OpenAI vendors, pass a pre-built
       ``BaseEmbedding`` / ``LLM`` instance straight to the operator and
       bypass the hook.

Hook guides
-----------

.. toctree::
    :maxdepth: 1
    :glob:

    *
