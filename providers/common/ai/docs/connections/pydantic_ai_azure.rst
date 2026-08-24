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

.. _howto/connection:pydanticai-azure:

Pydantic AI (Azure OpenAI) Connection
======================================

The ``pydanticai-azure`` connection type configures access to
`Azure OpenAI <https://azure.microsoft.com/en-us/products/ai-services/openai-service>`__
via the pydantic-ai framework. It backs ``PydanticAIAzureHook``, the dedicated
subclass of ``PydanticAIHook`` for Azure's non-standard auth (an endpoint URL
plus an API version, rather than the plain ``api_key`` + optional ``base_url``
that the generic :doc:`pydantic_ai` connection assumes).

Default Connection IDs
----------------------

The ``PydanticAIAzureHook`` uses ``pydanticai_azure_default`` by default.

Configuring the Connection
--------------------------

Model
    Azure model identifier (e.g. ``azure:gpt-4o``). This field appears as a
    dedicated input in the connection form (via ``conn-fields``) and stores its
    value in ``extra["model"]``.

    The ``azure:`` prefix is required — it is what makes pydantic-ai instantiate
    the Azure OpenAI provider instead of the plain OpenAI one.

API Key (Password field)
    The Azure OpenAI API key.

Azure Endpoint (Host field)
    The Azure OpenAI resource endpoint, e.g.
    ``https://<resource>.openai.azure.com/openai/deployments/<deployment>``.

API Version (Extra field)
    Azure OpenAI API version (e.g. ``2024-07-01-preview``). Falls back to the
    ``OPENAI_API_VERSION`` environment variable when omitted.

Fallback Connections
    Other connection IDs to fail over to, in order, while this provider is
    unavailable. Stored in ``extra["fallback_conn_ids"]``. Entries may name any
    ``pydanticai`` connection type, so one chain can span vendors. See
    :doc:`/provider_fallback`.

Examples
--------

.. code-block:: json

    {
        "conn_type": "pydanticai-azure",
        "password": "<azure-api-key>",
        "host": "https://<resource>.openai.azure.com",
        "extra": "{\"model\": \"azure:gpt-4o\", \"api_version\": \"2024-07-01-preview\"}"
    }

Relationship to the hook
-------------------------

``PydanticAIAzureHook`` maps the connection's ``password`` to the provider's
``api_key``, ``host`` to ``azure_endpoint``, and ``extra["api_version"]`` to
``api_version``, then constructs pydantic-ai's Azure provider with those values.
If none of them are set, the hook falls back to pydantic-ai's own environment-variable
resolution (``AZURE_OPENAI_API_KEY``, ``AZURE_OPENAI_ENDPOINT``, ``OPENAI_API_VERSION``).
