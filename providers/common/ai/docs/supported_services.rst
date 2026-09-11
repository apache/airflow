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

Connections
-----------

.. provider-connection-services:: apache-airflow-providers-common-ai

Toolsets
--------

.. provider-toolset-services:: apache-airflow-providers-common-ai

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
  — see the "Choosing extras" section of :doc:`index`.
* "Pydantic AI Gateway" in the ``pydanticai`` row is a routing layer, not an upstream vendor
  in its own right: set the connection's Model field to ``gateway/<vendor>:<model>`` (for
  example ``gateway/anthropic:claude-sonnet-5``) to send the request through it instead of
  directly to the vendor. It currently routes to Anthropic, AWS Bedrock, Google Gemini,
  Google Vertex AI, Groq, and OpenAI — all already listed above in their own right.
