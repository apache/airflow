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

.. _howto/model-providers:

Supported model providers
=========================

Every pydantic-ai model call in this provider, from an operator, a decorator or the hook,
reaches its model through one Airflow connection, and the ``provider:`` prefix on the model
name picks the vendor. Switching vendors is a connection
change, not a Dag change. Find your vendor below, install the extra, create the connection
type shown, and set the model name with that prefix.

.. list-table::
   :header-rows: 1
   :widths: 18 16 18 20 28

   * - Vendor
     - Model prefix
     - Install
     - Connection type
     - Credentials
   * - :ref:`OpenAI <conn-example-openai>`
     - ``openai:``
     - ``[openai]``
     - ``pydanticai``
     - API key in **Password**
   * - :ref:`Anthropic <conn-example-anthropic>`
     - ``anthropic:``
     - ``[anthropic]``
     - ``pydanticai``
     - API key in **Password**
   * - :ref:`Google Gemini API <conn-example-google>`
     - ``google:``
     - ``[google]``
     - ``pydanticai``
     - API key in **Password**, or ``GOOGLE_API_KEY`` in the environment
   * - Google Vertex AI
     - ``google-cloud:``
     - ``[google]``
     - ``pydanticai_vertex`` (:doc:`connections/pydantic_ai_vertex`)
     - Service account or Application Default Credentials
   * - AWS Bedrock
     - ``bedrock:``
     - ``[bedrock]``
     - ``pydanticai_bedrock`` (:doc:`connections/pydantic_ai_bedrock`), or ``pydanticai``
       with AWS credentials in the environment
     - IAM keys, profile or role
   * - Azure OpenAI
     - ``azure:``
     - ``[openai]``
     - ``pydanticai_azure`` (:doc:`connections/pydantic_ai_azure`)
     - API key in **Password**, resource endpoint in **Host**
   * - Groq
     - ``groq:``
     - ``pydantic-ai-slim[groq]``
     - ``pydanticai``
     - API key in **Password**
   * - Mistral AI
     - ``mistral:``
     - ``pydantic-ai-slim[mistral]``
     - ``pydanticai``
     - API key in **Password**
   * - DeepSeek
     - ``deepseek:``
     - ``[openai]``
     - ``pydanticai``
     - API key in **Password**
   * - :ref:`Ollama <conn-example-ollama>`, vLLM and other OpenAI-compatible servers
     - ``openai:``
     - ``[openai]``
     - ``pydanticai`` with the server URL in **Host** (:doc:`self_hosted_models`)
     - Usually none
   * - Snowflake Cortex
     - ``snowflake:``
     - ``pydantic-ai-slim[snowflake]``
     - ``pydanticai``
     - ``SNOWFLAKE_ACCOUNT`` and ``SNOWFLAKE_TOKEN`` in the worker environment; leave
       **Password** empty
   * - TypeSafe Jev (classifier, does not write text)
     - ``typesafe:``
     - ``[typesafe]``
     - ``pydanticai`` (:doc:`classifier_models`)
     - API key in **Password**

``[name]`` in the Install column is an extra of this provider, installed as
``pip install "apache-airflow-providers-common-ai[name]"``. The Groq, Mistral and Snowflake entries
name the matching ``pydantic-ai-slim`` extra instead, because this provider does not ship one
for them.

Any other vendor that `pydantic-ai supports <https://ai.pydantic.dev/models/overview/>`__
(Cohere, OpenRouter, Hugging Face and more) works the same
way: install the ``pydantic-ai-slim`` extra named on that vendor's pydantic-ai page, create a
``pydanticai`` connection, and use the prefix from that page.

Where the model name goes
-------------------------

Set the model on the connection's **Model** field in ``provider:model`` form, for example
``anthropic:claude-sonnet-5``. An operator's ``model_id`` overrides it for that task, so one
connection can serve several models from the same vendor. The generic ``pydanticai``
connection has no vendor of its own, so a bare model name without a prefix is rejected; the
Azure, Bedrock and Vertex connection types each supply their own prefix and accept a bare
name. :doc:`connections/pydantic_ai` has every field and the full resolution order.

Reliability across vendors
--------------------------

A connection can name other connections to fail over to when its vendor is unavailable, so
one task can span OpenAI and Anthropic without a code change. See :doc:`provider_fallback`.

Using a model outside an operator
---------------------------------

:doc:`hooks/pydantic_ai` returns the pydantic-ai ``Agent`` or ``Model`` behind a connection
for use in a plain ``@task``. :doc:`hooks/langchain` does the same for LangChain chat and
embedding models, which have their own ``langchain`` connection type.

Pages in this section
---------------------

.. toctree::
    :titlesonly:

    Azure OpenAI <connections/pydantic_ai_azure>
    AWS Bedrock <connections/pydantic_ai_bedrock>
    Google Vertex AI <connections/pydantic_ai_vertex>
    Self-hosted models <self_hosted_models>
    Classifier models <classifier_models>
    Provider fallback <provider_fallback>
    Connection reference <connections/pydantic_ai>
    Using the hook directly <hooks/pydantic_ai>
    LangChain models <hooks/langchain>
