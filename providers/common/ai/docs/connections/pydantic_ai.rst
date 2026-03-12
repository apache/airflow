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

.. _howto/connection:pydanticai:
.. _howto/connection:pydantic_ai:

Pydantic AI Connection
======================

The `Pydantic AI <https://ai.pydantic.dev/>`__ connection type configures access
to LLM providers via the pydantic-ai framework. A single connection type works with
any provider that pydantic-ai supports: OpenAI, Anthropic, Google, Bedrock, Groq,
Mistral, Ollama, vLLM, and others.

Default Connection IDs
----------------------

The ``PydanticAIHook`` uses ``pydanticai_default`` by default.

Configuring the Connection
--------------------------

Model
    The model identifier in ``provider:model`` format. This field appears as a
    dedicated input in the connection form (via ``conn-fields``) and stores its
    value in ``extra["model"]``.

    Examples: ``openai:gpt-5.3``, ``anthropic:claude-sonnet-4-20250514``,
    ``bedrock:us.anthropic.claude-opus-4-6-v1:0``, ``google:gemini-2.0-flash``

    The model can also be overridden at the hook/operator level via the
    ``model_id`` parameter.

API Key (Password field)
    The API key for your LLM provider. Required for API-key-based providers
    (OpenAI, Anthropic, Groq, Mistral). Leave empty for providers using
    environment-based auth (Bedrock via ``AWS_PROFILE``, Vertex via
    ``GOOGLE_APPLICATION_CREDENTIALS``).

Host (optional)
    Base URL for the provider's API. Only needed for custom endpoints:

    - Ollama: ``http://localhost:11434/v1``
    - vLLM: ``http://localhost:8000/v1``
    - Azure OpenAI: ``https://<resource>.openai.azure.com/openai/deployments/<deployment>``
    - Any OpenAI-compatible API: the base URL of that service

Extra (JSON, optional)
    A JSON object with additional configuration. Programmatic users can set the
    model directly in extra:

    .. code-block:: json

        {"model": "openai:gpt-5.3"}

    When using the UI, the "Model" field above writes to this same location
    automatically.

Examples
--------

**OpenAI**

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "password": "sk-...",
        "extra": "{\"model\": \"openai:gpt-5.3\"}"
    }

**Anthropic**

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "password": "sk-ant-...",
        "extra": "{\"model\": \"anthropic:claude-opus-4-6\"}"
    }

**Ollama (local)**

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "host": "http://localhost:11434/v1",
        "extra": "{\"model\": \"openai:llama3\"}"
    }

**AWS Bedrock**

Leave password empty and configure ``AWS_PROFILE`` or IAM role in the environment:

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "extra": "{\"model\": \"bedrock:us.anthropic.claude-opus-4-6-v1:0\"}"
    }

**Google Vertex AI**

Leave password empty and configure ``GOOGLE_APPLICATION_CREDENTIALS`` in the environment:

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "extra": "{\"model\": \"google:gemini-2.0-flash\"}"
    }

Model Resolution Order
----------------------

The hook reads the model from these sources in priority order:

1. ``model_id`` parameter on the hook/operator
2. ``model`` in the connection's extra JSON (set by the "Model" conn-field in the UI)
