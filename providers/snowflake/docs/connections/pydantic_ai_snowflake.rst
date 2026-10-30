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

.. _howto/connection:pydanticai_snowflake:

Pydantic AI (Snowflake Cortex) connection
==========================================

The ``pydanticai_snowflake`` connection type configures access to
`Snowflake Cortex <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api>`__'s
OpenAI-compatible chat endpoint via the pydantic-ai framework. It backs
``PydanticAISnowflakeHook``, the dedicated subclass of ``PydanticAIHook`` for Snowflake Cortex.

Unlike using a plain ``pydanticai`` connection with a ``snowflake:`` model and pydantic-ai's own
``SNOWFLAKE_ACCOUNT`` / ``SNOWFLAKE_TOKEN`` environment variables, this connection type sources
credentials from an existing :ref:`Snowflake connection <howto/connection:snowflake>` instead (the same
connection your other Snowflake hooks use): the credential lives in one place, goes through Airflow's
secrets backend, can be rotated in one place, and -- for key-pair JWT authentication -- is refreshed
automatically as the token nears expiry, which a static environment variable cannot do.

Install with:

.. code-block:: bash

    pip install 'apache-airflow-providers-snowflake[common.ai]'

Default Connection IDs
----------------------

The ``PydanticAISnowflakeHook`` uses ``pydanticai_snowflake_default`` by default.

Configuring the Connection
--------------------------

This connection type needs two connections: this one, and an existing ``snowflake`` connection it
points at. All fields below are ``extra`` (JSON) fields on the ``pydanticai_snowflake`` connection;
``Schema``, ``Port``, ``Login``, ``Host``, and ``Password`` are hidden in the connection form
because credentials and host come from the Snowflake connection instead.

Model
    Cortex model identifier (e.g. ``snowflake:claude-4-sonnet``).

    A bare name is automatically resolved to ``snowflake:<name>`` -- Snowflake Cortex is this
    connection type's own platform.

Fallback Connections
    Other connection IDs to fail over to, in order, while this provider is unavailable. Stored in
    ``extra["fallback_conn_ids"]``. Entries may name any ``pydanticai`` connection type, so one
    chain can span vendors. See :doc:`apache-airflow-providers-common-ai:provider_fallback`.

Snowflake Connection ID
    Connection ID of an existing :ref:`Snowflake connection <howto/connection:snowflake>` to
    source credentials, account, and host from. Stored in ``extra["snowflake_conn_id"]``. Also
    settable via the ``PydanticAISnowflakeHook(snowflake_conn_id=...)`` constructor argument, which
    takes precedence over the extra field. One of the two is required.

Authentication
--------------

Credentials are not configured on this connection -- they come from whichever authentication
method the referenced Snowflake connection uses, set the same way as for
:ref:`SnowflakeSqlApiHook and SnowflakeCortexAgentHook <howto/connection:snowflake>`:

- **OAuth**: set ``authenticator`` to ``oauth`` in the Snowflake connection's extra, and configure
  a refresh token, a client credentials grant, or ``azure_conn_id``.
- **PAT (Programmatic Access Token)**: set ``authenticator`` to ``programmatic_access_token`` and
  put the PAT value in the Snowflake connection's Password field.
- **Key-pair JWT**: the default when neither of the above is set. Configure
  ``private_key_file`` or ``private_key_content`` (optionally with a passphrase in Password), and
  set Login to the Snowflake user name the key is registered to.

See the :doc:`Snowflake connection <snowflake>` page for the full field reference.

Example
-------

Two connections: the Snowflake connection holding credentials, and this one pointing at it.

.. code-block:: json

    {
        "conn_type": "snowflake",
        "extra": "{\"account\": \"myorg-myaccount\", \"authenticator\": \"programmatic_access_token\"}",
        "password": "<programmatic_access_token>"
    }

.. code-block:: json

    {
        "conn_type": "pydanticai_snowflake",
        "extra": "{\"model\": \"snowflake:claude-4-sonnet\", \"snowflake_conn_id\": \"snowflake_default\"}"
    }

Model family support
---------------------

Support varies by model family, as defined by pydantic-ai's ``SnowflakeProvider.model_profile``
(check it for the version you have installed):

.. list-table::
   :header-rows: 1

   * - Model family
     - Tools
     - Structured output
   * - Claude (``claude*``)
     - Yes
     - Native (JSON schema)
   * - OpenAI (``openai-*``)
     - Yes
     - Native
   * - Others (``llama*``, ``snowflake-llama*``, ``mistral*``, ``mixtral*``, ``deepseek*``,
       and any unlisted family)
     - No
     - Prompted fallback

For a tool-using agent (``AgentOperator``, or any agent with toolsets), use a Claude or OpenAI
family model.
