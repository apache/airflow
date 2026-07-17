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

.. _howto/connection:mcp:

MCP Server Connection
=====================

The MCP connection type configures access to
`MCP (Model Context Protocol) <https://modelcontextprotocol.io/>`__ servers.
Three transport types are supported: Streamable HTTP, SSE, and stdio.

Default Connection IDs
----------------------

The ``MCPHook`` uses ``mcp_default`` by default.

Configuring the Connection
--------------------------

Transport (Extra field)
    The transport type: ``http`` (default), ``sse``, or ``stdio``.

    - ``http``: Streamable HTTP — the recommended transport for remote servers.
    - ``sse``: Server-Sent Events — deprecated in favor of Streamable HTTP.
    - ``stdio``: Run the MCP server as a subprocess communicating over stdin/stdout.

Host
    The server URL. Required for ``http`` and ``sse`` transports.

    Examples: ``http://localhost:3001/mcp``, ``https://mcp.example.com/v1``

Auth Token (Password field)
    Optional authentication token for the MCP server. Sent as a static
    ``Authorization: Bearer <token>`` header on HTTP/SSE transports. For
    short-lived or minted tokens, use a ``token_provider`` instead (see below).

Command (Extra field)
    The command to run for ``stdio`` transport. Required when transport is ``stdio``.

    Examples: ``uvx``, ``python``, ``node``

Arguments (Extra field)
    JSON array of arguments for the stdio command.

    Examples: ``["mcp-run-python"]``, ``["-m", "my_mcp_server"]``

Environment (Extra field)
    JSON object of environment variables for the ``stdio`` subprocess. Ignored
    for ``http``/``sse``. For a secret that lives in a different connection or is
    minted fresh per call, use an ``env_provider`` instead of storing it here
    (see below).

    Examples: ``{"MY_SERVER_MODE": "readonly"}``

Examples
--------

**HTTP transport (remote MCP server)**

.. code-block:: json

    {
        "conn_type": "mcp",
        "host": "http://localhost:3001/mcp"
    }

**SSE transport**

.. code-block:: json

    {
        "conn_type": "mcp",
        "host": "http://localhost:3001/sse",
        "extra": "{\"transport\": \"sse\"}"
    }

**Stdio transport (subprocess)**

.. code-block:: json

    {
        "conn_type": "mcp",
        "extra": "{\"transport\": \"stdio\", \"command\": \"uvx\", \"args\": [\"mcp-run-python\"]}"
    }

**Stdio with custom timeout**

.. code-block:: json

    {
        "conn_type": "mcp",
        "extra": "{\"transport\": \"stdio\", \"command\": \"python\", \"args\": [\"-m\", \"my_server\"], \"timeout\": 30}"
    }

**Stdio with subprocess environment variables**

.. code-block:: json

    {
        "conn_type": "mcp",
        "extra": "{\"transport\": \"stdio\", \"command\": \"my-mcp-server\", \"args\": [], \"env\": {\"SERVER_MODE\": \"readonly\"}}"
    }

Short-lived or minted tokens
----------------------------

Some MCP endpoints require a freshly minted, short-lived token rather than a
static one. For example, `Snowflake managed MCP servers
<https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp>`__
are best authenticated with a `key-pair JWT
<https://docs.snowflake.com/en/user-guide/key-pair-auth>`__: the private key never
leaves your environment and the signed JWT expires after about an hour, so it
cannot be stored as a static connection ``password``. The same applies to OAuth /
refresh tokens, Workload Identity Federation, and GitHub App installation tokens.

For these, pass a ``token_provider`` callable to ``MCPHook`` or ``MCPToolset``
instead of a static token. It is called once, the first time a given hook or
toolset instance establishes a connection (the result is then cached for that
instance's lifetime), and its return value is used as the bearer token, so a
fresh token is minted (and registered with secret masking so it does not leak
into task logs) without ever being written to the connection:

.. code-block:: python

    from airflow.providers.common.ai.toolsets.mcp import MCPToolset


    def mint_snowflake_jwt() -> str:
        # Sign a short-lived JWT from the Snowflake connection's key-pair.
        ...


    toolset = MCPToolset(
        mcp_conn_id="snowflake_managed_mcp",
        token_provider=mint_snowflake_jwt,
    )

``token_provider`` is resolved in Dag code (it is a Python callable, not a stored
connection field), so the signing key stays in your environment and is never baked
into the serialized Dag.

Secrets in stdio subprocess environments
-----------------------------------------

The ``stdio`` transport runs the MCP server as a local subprocess, and many such
servers read credentials from their own environment rather than accepting them
as arguments -- for example, a server that reaches Splunk needs a Splunk API key
in ``SPLUNK_API_KEY``. ``Extra.env`` (like the rest of ``extra``) is Fernet-encrypted
at rest, the same as ``password``, so it is a fine place for a static value that
genuinely belongs to *this* connection.

Use ``env_provider`` instead when the credential has no stable static form to
store here at all -- the same situation ``token_provider`` exists for on
HTTP/SSE:

- **It lives in a different connection.** A server that reaches Splunk needs a
  Splunk credential, not a credential for the MCP server itself; duplicating
  it into this connection's ``Extra.env`` means two places to rotate and a
  real chance the copies drift.
- **It's minted fresh per call** -- an OAuth token, a Vault lease, an
  STS-assumed role -- so there is no fixed value to store anywhere, on this
  connection or any other.

``env_provider`` is called once, the first time a given hook or toolset instance
establishes a connection (the result is then cached for that instance's
lifetime). Its return value is merged over ``Extra.env`` (``env_provider`` keys
win on conflicts), and -- as a secondary benefit -- every value it returns is
explicitly registered with secret masking regardless of key name, unlike
``Extra.env``, which is only masked in the Connections UI/API and task logs if
the key name happens to match a fixed set of sensitive-looking names
(``api_key``, ``token``, ``secret``, ``password``, etc.).

The example below covers the first case -- the Splunk credential is already
managed as its own Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_mcp.py
    :language: python
    :start-after: [START howto_toolset_mcp_env_provider]
    :end-before: [END howto_toolset_mcp_env_provider]

Like ``token_provider``, ``env_provider`` is resolved in Dag code, so the secret is
fetched at task-execution time and never baked into the serialized Dag.
