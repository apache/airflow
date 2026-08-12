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

Pydantic AI Connection
======================

The `Pydantic AI <https://ai.pydantic.dev/>`__ connection type configures access
to LLM and embedding models via the pydantic-ai framework. A single connection
type works with any provider that pydantic-ai supports. Supported LLM providers
include OpenAI, Anthropic, Google, Bedrock, Groq, Mistral, Ollama, vLLM, and
others. Embedding model availability depends on the providers supported by
pydantic-ai's ``Embedder``.

Default Connection IDs
----------------------

The ``PydanticAIHook`` uses ``pydanticai_default`` by default.

Configuring the Connection
--------------------------

Model
    The model identifier in ``provider:model`` format. This field appears as a
    dedicated input in the connection form (via ``conn-fields``) and stores its
    value in ``extra["model"]``.

    Examples: ``openai:gpt-5.6-sol``, ``anthropic:claude-sonnet-5``,
    ``bedrock:us.anthropic.claude-opus-4-6-v1:0``, ``google:gemini-2.0-flash``

    See `Anthropic's models overview <https://platform.claude.com/docs/en/about-claude/models/overview#latest-models-comparison>`__
    for the current list of Claude model IDs across the Claude API, Amazon Bedrock, and Google Cloud.
    See `OpenAI's models reference <https://developers.openai.com/api/docs/models/all>`__
    for the current list of OpenAI model IDs.

    The model can also be overridden at the hook/operator level via the
    ``model_id`` parameter.

Embedding Model
    The embedding model identifier in ``provider:model`` format. This field
    appears as a dedicated input in the connection form and stores its value in
    ``extra["embed_model"]``.

    Example: ``openai:text-embedding-3-small``

    The embedding model and connection can also be overridden at the hook level
    via the ``embed_model_id`` and ``embed_conn_id`` parameters.

API Key (Password field)
    The API key for your model provider. Required for API-key-based providers
    (OpenAI, Anthropic, Groq, Mistral). Leave empty for providers using
    environment-based auth (Bedrock via ``AWS_PROFILE``, Vertex via
    ``GOOGLE_APPLICATION_CREDENTIALS``).

Host (optional)
    Base URL for the model provider's API. Only needed for custom endpoints:

    - Ollama: ``http://localhost:11434/v1``
    - vLLM: ``http://localhost:8000/v1``
    - Azure OpenAI: ``https://<resource>.openai.azure.com/openai/deployments/<deployment>``
    - Any OpenAI-compatible API: the base URL of that service

Extra (JSON, optional)
    A JSON object with additional configuration. Programmatic users can set the
    LLM and embedding models directly in extra:

    .. code-block:: json

        {
            "model": "openai:gpt-5.6-sol",
            "embed_model": "openai:text-embedding-3-small"
        }

    When using the UI, the "Model" and "Embedding Model" fields above write to
    this same location automatically.

Examples
--------

**OpenAI**

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "password": "sk-...",
        "extra": "{\"model\": \"openai:gpt-5.6-sol\"}"
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

This still works — the ``bedrock:`` model prefix and the environment-variable
credential chain are unchanged. For AWS-specific fields with dedicated UI
inputs (region, IAM keys, profile, bearer token, timeouts) instead of raw
``extra`` JSON, use the :doc:`pydantic_ai_bedrock` connection type.

**Google Vertex AI / Gemini API**

Leave password empty and configure ``GOOGLE_API_KEY`` (or ``GEMINI_API_KEY``)
in the environment:

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "extra": "{\"model\": \"google:gemini-2.0-flash\"}"
    }

This connects to the Gemini API (Google AI Studio), not Vertex AI — pydantic-ai's
plain ``google:`` provider only reads an API key
(``GOOGLE_API_KEY``/``GEMINI_API_KEY``); it does not fall back to
``GOOGLE_APPLICATION_CREDENTIALS`` or any other Application Default
Credentials source. For project/location-scoped Vertex AI access — service
account or Application Default Credentials — use the
:doc:`pydantic_ai_vertex` connection type with a ``google-cloud:`` model
prefix instead.

Model Resolution Order
----------------------

The hook reads the model from these sources in priority order:

1. ``model_id`` parameter on the hook/operator
2. ``model`` in the connection's extra JSON (set by the "Model" conn-field in the UI)

The embedding model is resolved separately:

1. ``embed_model_id`` parameter on the hook
2. ``embed_model`` in the connection's extra JSON

Embedding credentials and endpoints are read from ``embed_conn_id`` when set;
otherwise they are read from ``llm_conn_id``.
