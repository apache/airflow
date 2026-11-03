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

.. _howto/hook:mcp:

``MCPHook``
===========

Use :class:`~airflow.providers.common.ai.hooks.mcp.MCPHook` to connect a Dag
task to an `MCP (Model Context Protocol) <https://modelcontextprotocol.io/>`__
server. The hook manages the server's connection configuration -- transport
type, URL or subprocess command, and credentials -- and builds the matching
client transport, so a Dag never hard-codes a server URL or an auth token.
Three transport types are supported: HTTP (Streamable HTTP), SSE, and stdio.

.. seealso::
    :ref:`howto/connection:mcp` for connection configuration and JSON examples.

Role in a Dag: use ``MCPToolset``, not the hook directly
----------------------------------------------------------

Most Dags should not instantiate ``MCPHook`` directly. The recommended entry
point is :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` (see
:doc:`../toolsets/mcp`), which resolves the hook lazily from an Airflow connection
and manages the underlying session lifecycle for you:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_mcp.py
    :language: python
    :start-after: [START howto_toolset_mcp_connection]
    :end-before: [END howto_toolset_mcp_connection]

Pass one or more ``MCPToolset`` instances to
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator`'s
``toolsets`` parameter and it drives ``MCPHook`` internally on every call.
Under the hood, ``MCPToolset`` builds an ``MCPHook`` from ``mcp_conn_id``,
``tool_prefix``, ``token_provider``, and ``env_provider``, then calls
``hook.get_conn()`` the first time it needs the server.

What ``MCPHook`` itself does
-----------------------------

``MCPHook.get_conn()`` reads the connection's transport type from
``Extra.transport`` and returns a configured pydantic-ai
`MCPToolset <https://ai.pydantic.dev/mcp/client/>`__ -- an *upstream*
pydantic-ai class, distinct from Airflow's own
:class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` described above
-- built over the matching `FastMCP <https://gofastmcp.com/>`__ transport:

- ``http`` (default): ``fastmcp.client.transports.StreamableHttpTransport``
- ``sse``: ``fastmcp.client.transports.SSETransport``
- ``stdio``: ``fastmcp.client.transports.StdioTransport``

When ``tool_prefix`` is set, the returned toolset is wrapped so every tool
name gets that prefix (e.g. ``"weather"`` yields ``weather_get_forecast``).
The result is cached for the lifetime of the hook instance.

``test_connection()`` validates that the connection has the fields required
for its configured transport, but does not connect to the server -- doing so
requires the async context manager that ``MCPToolset`` drives.

``token_provider`` and ``env_provider`` replace a static token or a static
``Extra.env`` value with a zero-argument callable, invoked once the first time the
hook establishes a connection; :doc:`../toolsets/mcp` explains both.

Connection fields
------------------

``MCPHook`` uses the ``mcp`` connection type. :ref:`howto/connection:mcp` documents
each field (``host``, ``password`` and the ``transport``, ``command``, ``args``,
``env`` and ``timeout`` keys in ``extra``) with a JSON example per transport.

Dependencies
------------

Install the ``mcp`` extra::

    pip install "apache-airflow-providers-common-ai[mcp]"
