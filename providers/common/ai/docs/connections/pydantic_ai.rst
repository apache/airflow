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

Pydantic AI connection
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

    Without Default Provider (below), the ``provider:`` prefix is required
    here: this generic connection type has no platform of its own (unlike the
    vendor connection types below), so a bare name (e.g. ``gpt-5`` without
    ``openai:``) raises ``ValueError`` naming this connection.

    Examples: ``openai:gpt-5``, ``anthropic:claude-sonnet-5``,
    ``bedrock:us.anthropic.claude-opus-4-6-v1:0``, ``google:gemini-2.5-flash``

    See `Anthropic's models overview <https://platform.claude.com/docs/en/about-claude/models/overview#latest-models-comparison>`__
    for the current list of Claude model IDs across the Claude API, Amazon Bedrock, and Google Cloud.
    See `OpenAI's models reference <https://developers.openai.com/api/docs/models/all>`__
    for the current list of OpenAI model IDs.

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
    - Azure OpenAI with an ``openai:`` model:
      ``https://<resource>.openai.azure.com/openai/v1``. For an ``azure:`` model,
      use the dedicated :doc:`pydantic_ai_azure` connection instead.
    - Any OpenAI-compatible API: the base URL of that service

Extra (JSON, optional)
    A JSON object with additional configuration. Programmatic users can set the
    model directly in extra:

    .. code-block:: json

        {"model": "openai:gpt-5"}

    When using the UI, the "Model" field above writes to this same location
    automatically.

Default Provider
    The pydantic-ai provider to prefix a bare model name with, e.g.
    ``openai-chat``. Stored in ``extra["provider"]``. With it set, ``model_id``
    and the Model field can name a model the way the endpoint does (e.g.
    ``claude-sonnet-4-5`` instead of ``openai-chat:claude-sonnet-4-5``). A name
    that already starts with a provider pydantic-ai recognizes is used as is.
    An unknown provider raises ``ValueError`` when the model is resolved.
    ``@task.llm_batch`` applies it the same way, and sends ``openai-chat`` models
    to its OpenAI adapter.

Fallback Connections
    Other connection IDs to fail over to, in order, while this provider is
    unavailable. Stored in ``extra["fallback_conn_ids"]``. Entries may name any
    ``pydanticai`` connection type, so one chain can span vendors. See
    :doc:`/provider_fallback`.

Examples
--------

.. _conn-example-openai:

OpenAI
^^^^^^

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "password": "sk-...",
        "extra": "{\"model\": \"openai:gpt-5\"}"
    }

.. _conn-example-anthropic:

Anthropic
^^^^^^^^^

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "password": "sk-ant-...",
        "extra": "{\"model\": \"anthropic:claude-sonnet-5\"}"
    }

.. _conn-example-ollama:

Ollama (local)
^^^^^^^^^^^^^^

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "host": "http://localhost:11434/v1",
        "extra": "{\"model\": \"openai:llama3\"}"
    }

.. _conn-example-openai-compatible-gateway:

OpenAI-compatible gateway
^^^^^^^^^^^^^^^^^^^^^^^^^

A gateway or proxy that speaks the OpenAI API (LiteLLM, vLLM, an in-house
gateway) often serves models from several vendors and implements only Chat
Completions. Set Default Provider to ``openai-chat`` so DAG code can use the
gateway's own model names:

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "host": "https://llm-gateway.example.com/v1",
        "password": "gateway-key",
        "extra": "{\"provider\": \"openai-chat\"}"
    }

.. code-block:: python

    @task.llm(llm_conn_id="my_gateway", model_id="claude-sonnet-4-5")
    def summarize(text: str) -> str:
        return f"Summarize: {text}"

``openai:`` resolves to OpenAI's Responses API and ``openai-chat:`` to Chat
Completions, so pick the one the gateway implements.

A ``name:tag`` model id such as ``llama3:70b`` gets the default too. The
exception is a name that is itself a pydantic-ai provider: ``mistral:7b`` goes
to pydantic-ai's own ``mistral`` provider, so write it as
``openai-chat:mistral:7b`` to send it through the gateway.

.. _conn-example-bedrock:

AWS Bedrock
^^^^^^^^^^^

Leave password empty and configure ``AWS_PROFILE`` or IAM role in the environment:

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "extra": "{\"model\": \"bedrock:us.anthropic.claude-opus-4-6-v1:0\"}"
    }

This still works: the ``bedrock:`` model prefix and the environment-variable
credential chain are unchanged. For AWS-specific fields with dedicated UI
inputs (region, IAM keys, profile, bearer token, timeouts) instead of raw
``extra`` JSON, use the :doc:`pydantic_ai_bedrock` connection type.

.. _conn-example-google:

Google Vertex AI / Gemini API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Leave password empty and configure ``GOOGLE_API_KEY`` (or ``GEMINI_API_KEY``)
in the environment:

.. code-block:: json

    {
        "conn_type": "pydanticai",
        "extra": "{\"model\": \"google:gemini-2.5-flash\"}"
    }

This connects to the Gemini API (Google AI Studio), not Vertex AI: pydantic-ai's
plain ``google:`` provider only reads an API key
(``GOOGLE_API_KEY``/``GEMINI_API_KEY``); it does not fall back to
``GOOGLE_APPLICATION_CREDENTIALS`` or any other Application Default
Credentials source. For project/location-scoped Vertex AI access (service
account or Application Default Credentials), use the
:doc:`pydantic_ai_vertex` connection type with a ``google-cloud:`` model
prefix instead.

.. _pydanticai-model-resolution:

Model Resolution Order
----------------------

The hook reads the model from these sources in priority order:

1. ``model_id`` parameter on the hook/operator
2. ``model`` in the connection's extra JSON (set by the "Model" conn-field in the UI)
3. When this connection is used as a fallback and neither of the above is set, the
   *bare* ``model_id`` forwarded from the primary connection (see :doc:`/provider_fallback`) --
   a forwarded name that already pins a platform is not applied here, since it names a
   model of the primary's own platform.
4. With ``PydanticAIHook.create_agent(spec_file=...)``, the ``model`` declared in the
   spec file, when neither ``model_id`` nor the connection's ``model`` extra is set. A
   connection that declares
   ``fallback_conn_ids`` but no model raises instead, because a spec-file model cannot
   be wrapped in a fallback chain (see :doc:`/hooks/pydantic_ai`).

Whichever of sources 1-3 supplies the name, a name that already pins a recognized platform
(its segment before the first ``:`` is itself a pydantic-ai provider) is used verbatim; a
bare name is qualified with this connection's platform. This generic connection type takes
its platform from Default Provider (``extra["provider"]``); without one, a bare name reaching
this step raises. The spec-file model of source 4 goes to pydantic-ai unchanged, so write it
with its provider prefix.
