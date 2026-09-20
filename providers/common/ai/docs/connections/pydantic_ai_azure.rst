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

.. _howto/connection:pydanticai_azure:

Pydantic AI (Azure OpenAI) Connection
======================================

The ``pydanticai_azure`` connection type configures access to
`Azure OpenAI <https://azure.microsoft.com/en-us/products/ai-services/openai-service>`__
via the pydantic-ai framework. It backs ``PydanticAIAzureHook``, the dedicated
subclass of ``PydanticAIHook`` that maps Airflow connection fields to
Azure-specific parameters: ``api_key``, ``azure_endpoint``, and, for endpoints
that do not use the OpenAI-compatible v1 API, ``api_version``. The generic
:doc:`pydantic_ai` connection instead supplies ``api_key`` and an optional
``base_url``.

.. note::

    This connection type was previously named ``pydanticai-azure``.

    Connections stored as a URI or as JSON need no change: ``-`` is how ``_`` is
    encoded in a URI scheme, so ``pydanticai-azure`` is decoded to ``pydanticai_azure``
    on read and resolves as before. That covers ``AIRFLOW_CONN_*`` environment
    variables and secrets backends such as HashiCorp Vault, AWS Secrets Manager and
    GCP Secret Manager.

    A connection whose type is stored verbatim does need updating, because the
    hyphen is preserved and no longer matches a registered hook. That means rows in
    the metadata database, including any created through the UI, and connections
    imported in object form from a local file:

    .. code-block:: bash

        airflow connections get <conn_id> -o json    # confirm conn_type is 'pydanticai-azure'
        airflow connections delete <conn_id>
        airflow connections add <conn_id> --conn-type pydanticai_azure ...

    In the UI, edit the connection and re-pick its type.

Default Connection IDs
----------------------

The ``PydanticAIAzureHook`` uses ``pydanticai_azure_default`` by default.

Configuring the Connection
--------------------------

Model
    Azure model identifier (e.g. ``azure:gpt-4o``, or the bare ``gpt-4o``). This
    field appears as a dedicated input in the connection form (via
    ``conn-fields``) and stores its value in ``extra["model"]``.

    A bare name is automatically resolved to ``azure:<name>`` -- Azure OpenAI is
    this connection type's own platform, so nothing else needs naming it
    explicitly. Writing the ``azure:`` prefix yourself has the same effect and is
    still accepted. A name prefixed with a *different*, recognized platform (e.g.
    ``openai:gpt-4o``) is used verbatim instead, pinning that platform and
    bypassing Azure OpenAI entirely -- a name is only treated as already prefixed
    when the segment before its first ``:`` is itself a real pydantic-ai
    provider, not merely present.

API Key (Password field)
    The Azure OpenAI API key.

Azure Endpoint (Host field)
    The Azure OpenAI resource endpoint, e.g.
    ``https://<resource>.openai.azure.com/openai/v1``.

API Version (Extra field)
    Azure OpenAI API version (e.g. ``2024-07-01-preview``). Set it when the
    endpoint path does not end in ``/v1`` and the host is not
    ``*.models.ai.azure.com``. When required, it falls back to the
    ``OPENAI_API_VERSION`` environment variable if omitted. Endpoints matching
    either OpenAI-compatible v1 form reject this field.

Fallback Connections
    Other connection IDs to fail over to, in order, while this provider is
    unavailable. Stored in ``extra["fallback_conn_ids"]``. Entries may name any
    ``pydanticai`` connection type, so one chain can span vendors. See
    :doc:`/provider_fallback`.

Examples
--------

.. code-block:: json

    {
        "conn_type": "pydanticai_azure",
        "password": "<azure-api-key>",
        "host": "https://<resource>.openai.azure.com/openai/v1",
        "extra": "{\"model\": \"azure:gpt-4o\"}"
    }

Relationship to the hook
-------------------------

``PydanticAIAzureHook`` maps the connection's ``password`` to the provider's
``api_key`` and ``host`` to ``azure_endpoint``, and maps
``extra["api_version"]`` to ``api_version`` when provided. The API version is
valid only when the endpoint path does not end in ``/v1`` and the host is not
``*.models.ai.azure.com``. The hook then constructs pydantic-ai's Azure provider
with those values. If none of them are set, it falls back to pydantic-ai's own
environment-variable resolution (``AZURE_OPENAI_API_KEY``,
``AZURE_OPENAI_ENDPOINT``, ``OPENAI_API_VERSION``).
