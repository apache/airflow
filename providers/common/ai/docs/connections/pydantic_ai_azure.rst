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
subclass of ``PydanticAIHook`` for Azure's non-standard auth (an endpoint URL
plus an API version, rather than the plain ``api_key`` + optional ``base_url``
that the generic :doc:`pydantic_ai` connection assumes).

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

Examples
--------

.. code-block:: json

    {
        "conn_type": "pydanticai_azure",
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
