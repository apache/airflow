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
`Databricks Model Serving <https://docs.databricks.com/aws/en/machine-learning/model-serving/>`__'s
OpenAI-compatible chat endpoint via the pydantic-ai framework. It backs
``PydanticAIDatabricksHook``, the dedicated subclass of ``PydanticAIHook`` for Databricks.

A plain ``pydanticai`` connection with ``https://<workspace>/serving-endpoints`` in **Host** and a
personal access token in **Password** also works, but only for a static token. This connection type
sources credentials from an existing :ref:`Databricks connection <howto/connection:databricks>`
instead, so every token-based authentication method that connection supports (OAuth, Azure AD,
managed identity, federation) is reused and refreshed on every request.

Install with:

.. code-block:: bash

    pip install 'apache-airflow-providers-databricks[common.ai]'

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
    Serving endpoint name (e.g. ``databricks-claude-sonnet-4-5``).

    A bare name is resolved to ``openai-chat:<name>``, which uses the Chat Completions API that
    every serving endpoint speaks. An explicit ``openai:<name>`` uses the Responses API instead; see the
    `Databricks API reference
    <https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference>`__
    for the models it supports.

Fallback Connections
    Other connection IDs to fail over to, in order, while this provider is unavailable. Stored in
    ``extra["fallback_conn_ids"]``. Entries may name any ``pydanticai`` connection type, so one
    chain can span vendors. See :doc:`apache-airflow-providers-common-ai:provider_fallback`.

Databricks Connection ID
    Connection ID of an existing :ref:`Databricks connection <howto/connection:databricks>` to
    source credentials from. Stored in ``extra["databricks_conn_id"]``. Also
    settable via the ``PydanticAIDatabricksHook(databricks_conn_id=...)`` constructor argument,
    which takes precedence over the extra field. One of the two is required.

Authentication
--------------

Credentials are not configured on this connection. A token is requested from the referenced
Databricks connection on every request, so it works with every token-based method described on the
:doc:`Databricks connection <databricks>` page. Username/password basic auth is not supported: the
token request fails with "Token authentication isn't configured", which surfaces in
**Test connection** and on the first request. The Databricks connection's ``proxies`` extra and
TLS settings are not applied to these requests.

Limitations
-----------

The hook does not rewrite requests, so the limits of Databricks Model Serving apply. As documented
by Databricks on its
`function calling <https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling>`__
and
`structured outputs <https://docs.databricks.com/aws/en/machine-learning/model-serving/structured-outputs>`__
pages:

- Parallel function calling is not supported, and at most 32 functions can be defined in ``tools``.
- Function definitions do not support ``pattern``, ``anyOf``, ``oneOf``, ``allOf``, ``prefixItems``
  or ``$ref``.
- For Claude models, ``response_format`` cannot be combined with ``tools`` or ``tool_choice``.

Limits vary by model and change over time; see the Databricks documentation for the current ones.
If an endpoint rejects parallel tool calls, pass ``model_settings={"parallel_tool_calls": False}``
to the operator or agent.

Only the ``/serving-endpoints`` base URL is supported. The hook has been unit-tested only and has
not been run against a live workspace.

Example
-------

Two connections: the Databricks connection holding credentials, and this one pointing at it.

.. code-block:: json

    {
        "conn_type": "databricks",
        "host": "https://adb-1234567890123456.7.azuredatabricks.net",
        "password": "<personal_access_token>"
    }

.. code-block:: json

    {
        "conn_type": "pydanticai_databricks",
        "extra": "{\"model\": \"databricks-claude-sonnet-4-5\", \"databricks_conn_id\": \"databricks_default\"}"
    }

Then use it from a task:

.. code-block:: python

    from airflow.providers.common.ai.operators.llm import LLMOperator

    summarize = LLMOperator(
        task_id="summarize",
        llm_conn_id="pydanticai_databricks_default",
        prompt="Summarize the latest run in one sentence.",
    )
