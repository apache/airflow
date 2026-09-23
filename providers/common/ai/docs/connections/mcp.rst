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

MCP server connection
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

    - ``http``: Streamable HTTP, the recommended transport for remote servers.
    - ``sse``: Server-Sent Events, deprecated in favor of Streamable HTTP.
    - ``stdio``: Run the MCP server as a subprocess communicating over stdin/stdout.

Host
    The server URL. Required for ``http`` and ``sse`` transports.

    Examples: ``http://localhost:3001/mcp``, ``https://mcp.example.com/v1``

Auth Token (Password field)
    Optional authentication token for the MCP server. Sent as a static
    ``Authorization: Bearer <token>`` header on HTTP/SSE transports. For
    short-lived or minted tokens, pass a ``token_provider`` to ``MCPToolset``
    instead (see :doc:`../toolsets/mcp`).

Command (Extra field)
    The command to run for ``stdio`` transport. Required when transport is ``stdio``.

    Examples: ``uvx``, ``python``, ``node``

Arguments (Extra field)
    JSON array of arguments for the stdio command.

    Examples: ``["mcp-run-python"]``, ``["-m", "my_mcp_server"]``

Environment (Extra field)
    JSON object of environment variables for the ``stdio`` subprocess. Ignored
    for ``http``/``sse``. For a secret that lives in a different connection or is
    minted fresh per call, pass an ``env_provider`` to ``MCPToolset`` instead of
    storing it here (see :doc:`../toolsets/mcp`).

    Examples: ``{"MY_SERVER_MODE": "readonly"}``

Timeout (Extra field)
    Connection init timeout in seconds for the ``stdio`` transport. Ignored
    for ``http``/``sse``. Default: ``10``.

    Examples: ``30``

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
