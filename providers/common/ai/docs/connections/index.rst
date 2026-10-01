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

Connections
===========

Every model call and every MCP server in this provider goes through an Airflow
connection. The connection holds the credential and, for model connections, the model
name in ``provider:model`` form, so a Dag names a connection id and never a key or a
vendor. Create connections in the UI under **Admin > Connections**, with an
``AIRFLOW_CONN_<ID>`` environment variable, or through a secrets backend.

.. list-table::
   :header-rows: 1
   :widths: 22 38 40

   * - Connection type
     - Use it for
     - Page
   * - ``pydanticai``
     - Any vendor pydantic-ai supports through an API key and an optional base URL:
       OpenAI, Anthropic, Google Gemini, Groq, Mistral, DeepSeek, Ollama, vLLM,
       TypeSafe and others. The default for every operator and decorator.
     - :doc:`pydantic_ai`
   * - ``pydanticai_azure``
     - Azure OpenAI, with the endpoint and API version Azure needs.
     - :doc:`pydantic_ai_azure`
   * - ``pydanticai_bedrock``
     - AWS Bedrock, with IAM keys, a bearer token or the default credential chain.
     - :doc:`pydantic_ai_bedrock`
   * - ``pydanticai_vertex``
     - Google Vertex AI, with a project, a location and a service account.
     - :doc:`pydantic_ai_vertex`
   * - ``mcp``
     - An MCP server the agent calls as a toolset, over HTTP, SSE or a stdio subprocess.
     - :doc:`mcp`
   * - ``langchain``
     - Chat and embedding models built through LangChain, for the LangChain hook and
       toolset bridge.
     - :doc:`langchain`
   * - ``llamaindex``
     - OpenAI models for the LlamaIndex embedding and retrieval operators.
     - :doc:`llamaindex`

Which vendors work, and the extra and model prefix each one needs, is on
:doc:`../model_providers`. Vendor outages can fail over to a second connection with
:doc:`../provider_fallback`. Error messages from a misconfigured connection are listed
with their fixes in :doc:`../troubleshooting`.
