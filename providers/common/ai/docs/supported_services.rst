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

Supported services
===================

``common.ai`` reaches external services on two axes: the model providers its connections
talk to, and the systems its toolsets talk to. For per-model detail beyond the vendor level
(exact model ids, pricing, capabilities), see `pydantic-ai's model list
<https://ai.pydantic.dev/models/>`__.

This page is the complete list of what this provider reaches; for a curated, install- and
credential-oriented walkthrough of each model vendor, see :doc:`model_providers`.

Connections
-----------

.. provider-connection-services:: apache-airflow-providers-common-ai

Toolsets
--------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Toolset
     - External services
   * - :doc:`HookToolset <toolsets/hook>`
     - Any Airflow connection, through its provider hook
   * - :doc:`SQLToolset <toolsets/sql>`
     - Any common.sql database connection (with table allowlists and AST validation)
   * - :doc:`DataFusionToolset <toolsets/datafusion>`
     - Object storage (S3, local filesystem, Apache Iceberg) via Apache DataFusion
   * - :doc:`LoggingToolset <toolsets/logging>`
     - Airflow task logs
   * - :doc:`MCPToolset <toolsets/mcp>`
     - Any MCP server (user-supplied endpoint)
   * - :doc:`ObjectStorageToolset <toolsets/object_storage>`
     - Object storage (S3, GCS, Azure Blob Storage, or any store
       :class:`~airflow.sdk.ObjectStoragePath` can open), read-only
   * - :doc:`SandboxToolset <sandbox/index>`
     - Docker Sandboxes (the shipped sbx backend; other backends can be added via SandboxBackend)
   * - :doc:`AgentSkillsToolset <toolsets/skills>`
     - Agent skills from local paths and remote Git repositories
   * - :doc:`LangChain Bridge <toolsets/langchain>`
     - LangChain tools
   * - :doc:`Managed Agent Toolsets <toolsets/managed_agent>`
     - Snowflake Cortex Agents; Amazon Bedrock AgentCore; Azure AI Foundry; Vertex AI Agent Engine

Notes
-----

* The ``langchain`` connection type's row above is limited to OpenAI-compatible credential
  surfaces (``api_key`` + optional ``base_url``); see :ref:`Supported providers
  <langchain-supported-providers>` for the providers that reject those kwargs and are not
  usable through it.
* Azure OpenAI, Google Vertex AI, and AWS Bedrock are also reachable through the generic
  ``pydanticai`` connection type, but each has its own dedicated connection type
  (:doc:`connections/pydantic_ai_azure`, :doc:`connections/pydantic_ai_vertex`,
  :doc:`connections/pydantic_ai_bedrock`) for their non-standard auth.
* Most model providers need an extra installed alongside ``apache-airflow-providers-common-ai``
  — see the "Choosing extras" section of :doc:`installation`.
* "Pydantic AI Gateway" in the ``pydanticai`` row is a routing layer, not an upstream vendor
  in its own right: set the connection's Model field to ``gateway/<vendor>:<model>`` (for
  example ``gateway/anthropic:claude-sonnet-5``) to send the request through it instead of
  directly to the vendor. It currently routes to Anthropic, AWS Bedrock, Google Gemini,
  Google Vertex AI, Groq, and OpenAI — all already listed above in their own right.
