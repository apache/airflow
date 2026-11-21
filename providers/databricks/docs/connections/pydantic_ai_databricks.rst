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

.. _howto/connection:pydanticai_databricks:

Pydantic AI (Databricks Model Serving) connection
=================================================

The ``pydanticai_databricks`` connection type configures access to
`Databricks Model Serving <https://docs.databricks.com/aws/en/machine-learning/model-serving/>`__
through its OpenAI-compatible Chat Completions API, via the pydantic-ai framework. It backs
``PydanticAIDatabricksHook``, the dedicated subclass of ``PydanticAIHook`` for Databricks.

This connection type sources credentials from an existing
:ref:`Databricks connection <howto/connection:databricks>`, so the token-based authentication
methods that connection supports (OAuth, Azure AD, managed identity, federation) are reused and
refreshed on every request, except for the settings this hook rejects (see Authentication below).

Install with:

.. code-block:: bash

    pip install 'apache-airflow-providers-databricks[common.ai]' 'apache-airflow-providers-common-ai[openai]'

The ``common.ai`` extra provides the Common AI hook this one builds on, but not the OpenAI client
the Chat Completions API calls go through, so also install ``apache-airflow-providers-common-ai[openai]``.

Choosing a connection type
--------------------------

For a personal access token, a plain ``pydanticai`` connection is enough: set the Model to
``openai-chat:<endpoint>``, the Host to ``https://<workspace>/serving-endpoints`` and put the token
in Password (see :doc:`apache-airflow-providers-common-ai:connections/pydantic_ai`). Use
``pydanticai_databricks`` when you want to reuse an existing Databricks connection, or when the
credential needs a fresh token per request, such as OAuth for a service principal, Azure or
token federation. A personal access token also works with this connection type.

Default Connection IDs
----------------------

The ``PydanticAIDatabricksHook`` uses ``pydanticai_databricks_default`` by default.

Configuring the Connection
--------------------------

This connection type needs two connections: this one, and an existing ``databricks`` connection it
points at. All fields below are ``extra`` (JSON) fields on the ``pydanticai_databricks``
connection; ``Schema``, ``Port``, ``Login``, ``Host``, and ``Password`` are hidden in the
connection form because the workspace host and credentials come from the Databricks connection
instead.

Model
    Serving endpoint name (e.g. ``my-serving-endpoint``).

    A bare name is resolved to ``openai-chat:<name>``, which uses the Chat Completions API.
    Databricks documents querying chat models through the OpenAI client for
    `Foundation Model APIs
    <https://docs.databricks.com/aws/en/machine-learning/model-serving/score-foundation-models>`__,
    `external models <https://docs.databricks.com/aws/en/machine-learning/foundation-models/external-models>`__ and
    `custom LLM endpoints
    <https://docs.databricks.com/aws/en/machine-learning/model-serving/serve-custom-llms>`__
    served with the chat task. Databricks does not document Chat Completions for other endpoint types,
    such as embedding or traditional ML endpoints; this hook is not intended for them.

    In pydantic-ai, the ``openai:`` prefix selects the Responses API and ``openai-chat:`` selects
    Chat Completions, as described in the
    `pydantic-ai OpenAI documentation <https://pydantic.dev/docs/ai/models/openai/>`__. An explicit
    ``openai:<name>`` therefore sends Responses API requests. Databricks documents its Responses API
    passthrough for OpenAI models; see
    `Query chat models
    <https://docs.databricks.com/aws/en/machine-learning/model-serving/query-chat-models>`__.

Fallback Connections
    Other connection IDs to fail over to, in order, while this provider is unavailable. Stored in
    ``extra["fallback_conn_ids"]``. Entries may name any ``pydanticai`` connection type, so one
    chain can span vendors. Fail-over also happens when the Databricks token endpoint cannot be
    reached or keeps failing after its retries; a configuration error, such as unset or rejected
    credentials, does not fail over and fails the task. This needs ``openai>=3.15.0``: with an
    earlier version, a configuration error also fails over to the next connection, so upgrade
    ``openai`` if you want it to fail the task. See
    :doc:`apache-airflow-providers-common-ai:provider_fallback`.

Databricks Connection ID
    Connection ID of an existing :ref:`Databricks connection <howto/connection:databricks>` to
    source credentials from. Stored in ``extra["databricks_conn_id"]``. Also
    settable via the ``PydanticAIDatabricksHook(databricks_conn_id=...)`` constructor argument,
    which takes precedence over the extra field. One of the two is required.

Authentication
--------------

Credentials are not configured on this connection. A token is requested from the referenced
Databricks connection on every request, so it works with the token-based methods described on the
:doc:`Databricks connection <databricks>` page, except for the settings listed below that this hook
rejects. A personal access token works with the Login left empty or set to ``token``. Username/password
basic auth is not supported: the token request fails with "Token authentication isn't configured",
which surfaces in **Test connection** and on the first request.

**Test connection** only checks that a token is available. A personal access token is not sent to the
workspace, so a revoked or mistyped one still passes; the token types fetched from an identity
provider are validated by that fetch.

Databricks documents that
`route-optimized endpoints <https://docs.databricks.com/aws/en/machine-learning/model-serving/route-optimization>`__
are queried differently from other endpoints, including using a different URL and authentication
using OAuth tokens. This hook only sends requests to ``/serving-endpoints``, so it does not support
route-optimized endpoints.

This hook does not route requests through a proxy and does not send the Azure workspace resource
headers. A Databricks connection that sets the ``proxies`` or ``azure_resource_id`` extra is
rejected with a ``ValueError`` when the model is built, instead of the setting being ignored
silently.

Limitations
-----------

The hook does not rewrite requests, so the limits of Databricks Model Serving apply. Those limits
vary by model and endpoint type and change over time. At the time of writing, Databricks' pages on
`function calling <https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling>`__
and
`structured outputs <https://docs.databricks.com/aws/en/machine-learning/model-serving/structured-outputs>`__
list the following, among others, which are the authoritative and more complete source:

- For Foundation Model APIs and external model endpoints, function calling does not support parallel
  function calls, allows at most 32 functions in ``tools``, and rejects JSON schema keywords such as
  ``pattern``, ``anyOf``, ``oneOf``, ``allOf``, ``prefixItems`` and ``$ref``. Databricks lists
  these under limitations during Public Preview.
- For Claude models, ``response_format`` cannot be combined with ``tools`` or ``tool_choice``.
  Databricks also lists newer Claude models that return HTTP 400 for ``response_format`` on Chat
  Completions.
- Databricks' structured outputs page also lists that structured outputs are not supported with
  streaming.

The hook sends requests to the ``/serving-endpoints`` base URL that Databricks documents for the
OpenAI client (see the
`custom LLM endpoints <https://docs.databricks.com/aws/en/machine-learning/model-serving/serve-custom-llms>`__
page); other base URLs are not supported.

Example
-------

Two connections: the Databricks connection holding credentials, here a Databricks-managed service
principal using OAuth (see :doc:`databricks`), and this one pointing at it.

.. code-block:: json

    {
        "conn_type": "databricks",
        "host": "https://adb-1234567890123456.7.azuredatabricks.net",
        "login": "<service_principal_client_id>",
        "password": "<service_principal_secret>",
        "extra": "{\"service_principal_oauth\": true}"
    }

.. code-block:: json

    {
        "conn_type": "pydanticai_databricks",
        "extra": "{\"model\": \"my-serving-endpoint\", \"databricks_conn_id\": \"databricks_default\"}"
    }

Then use it from a task:

.. code-block:: python

    from airflow.providers.common.ai.operators.llm import LLMOperator

    summarize = LLMOperator(
        task_id="summarize",
        llm_conn_id="pydanticai_databricks_default",
        prompt="Summarize the latest run in one sentence.",
    )
