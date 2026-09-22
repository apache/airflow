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

``MCPToolset``
==============

.. toctree::
    :hidden:
    :maxdepth: 1

    MCP connection <../connections/mcp>
    MCPHook <../hooks/mcp>

Connects to an `MCP (Model Context Protocol) <https://modelcontextprotocol.io/>`__
server configured via an Airflow connection. MCP is an open protocol that lets
LLMs interact with external tools and data sources through a standardized
interface.

.. code-block:: python

    from airflow.providers.common.ai.toolsets.mcp import MCPToolset

    toolset = MCPToolset(
        mcp_conn_id="my_mcp_server",
        tool_prefix="weather",
    )

The MCP server is resolved lazily from the Airflow connection on the first
tool call. See :ref:`howto/connection:mcp` for connection configuration.

Requires the ``mcp`` extra: ``pip install "apache-airflow-providers-common-ai[mcp]"``

Parameters
----------

- ``mcp_conn_id``: Airflow connection ID for the MCP server.
- ``tool_prefix``: Optional prefix prepended to tool names to avoid
  collisions when using multiple MCP servers (e.g. ``"weather"`` produces
  ``"weather_get_forecast"``).
- ``token_provider``: Optional zero-argument callable returning a bearer token.
  When set, it overrides the connection's static ``password`` for the
  ``Authorization`` header. Called once, the first time this toolset
  establishes a connection -- use it for short-lived or minted tokens (e.g. a
  Snowflake managed MCP server authenticated with a key-pair JWT). See
  :ref:`howto/connection:mcp`.
- ``env_provider``: Optional zero-argument callable returning a
  ``dict[str, str]`` merged over the connection's ``Extra.env`` (winning on key
  conflicts) for the ``stdio`` subprocess environment -- use it when the
  credential a local stdio MCP server needs lives in a different connection, or
  is minted fresh per call (e.g. a Splunk/Vault token), rather than storing it
  statically here. Called once, the first time this toolset establishes a
  connection. See :ref:`howto/connection:mcp`.

Using multiple MCP servers
--------------------------

.. code-block:: python

    AgentOperator(
        task_id="multi_mcp",
        prompt="Get the weather in London and run a calculation",
        llm_conn_id="pydanticai_default",
        toolsets=[
            MCPToolset(mcp_conn_id="weather_mcp", tool_prefix="weather"),
            MCPToolset(mcp_conn_id="code_runner_mcp", tool_prefix="code"),
        ],
    )

Direct pydantic-ai MCP toolsets
-------------------------------

For prototyping or when you want full PydanticAI control, you can pass
``MCPToolset`` instances directly — no Airflow connection needed:

.. code-block:: python

    from fastmcp.client.transports import StdioTransport
    from pydantic_ai.mcp import MCPToolset

    AgentOperator(
        task_id="direct_mcp",
        prompt="What tools are available?",
        llm_conn_id="pydanticai_default",
        toolsets=[
            MCPToolset("http://localhost:3001/mcp"),
            MCPToolset(StdioTransport(command="uvx", args=["mcp-run-python"])),
        ],
    )

This works because PydanticAI's ``MCPToolset`` implements ``AbstractToolset``.
The tradeoff: URLs and credentials are hardcoded in Dag code instead of being
managed through Airflow connections and secret backends.

When to choose it
-----------------

**Choose it when** someone already publishes a server built for agents that
covers your target. You inherit a tool surface that was designed to be called by
a model — retry semantics and error wording are decided upstream, and a
destructive tool can simply be absent — instead of maintaining a per-API wrapper
yourself.

**What it cannot do**

- Its allow-list is client-side and opt-in, not server-side and required.
  :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` forwards
  ``get_tools`` and ``call_tool`` straight to the underlying server, so
  whatever the server exposes, the agent gets by default. Because
  ``MCPToolset`` is itself built on the same ``AbstractToolset`` base every
  toolset in this provider extends, a Dag author can call ``.filtered()`` to
  subset the advertised tool list using a filter function that inspects each
  tool's definition. That filtering happens on the client: it narrows what
  the agent is offered, it does not revoke or authorize anything on the
  server, and unlike ``allowed_methods`` on ``HookToolset``, which is required
  and rejects an empty list, nothing here requires you to set a filter. The
  defense-layer table is explicit that a server can expose shell, filesystem
  or network access.
- It cannot guarantee the credential came from a connection. ``mcp_conn_id`` is
  the default path, but ``token_provider`` and ``env_provider`` are your own
  callables and are free to read an environment variable, a file, or an entirely
  different secret store. That is the point of them — it also means the
  connection is no longer the whole story for anyone auditing the Dag.
- ``stdio`` transport is not isolation. It starts a child process on the worker
  host. It is not a sandbox, and when no environment is supplied the child
  inherits a small allowlist of variables rather than the full parent
  environment — so it is both less contained and less predictable than it looks.

**A real example.** ``example_mcp.py`` drives setup from a connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_mcp.py
    :language: python
    :start-after: [START howto_toolset_mcp_connection]
    :end-before: [END howto_toolset_mcp_connection]

and puts several servers on one agent, prefixed so their tool names stay apart:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_mcp.py
    :language: python
    :start-after: [START howto_toolset_mcp_multiple]
    :end-before: [END howto_toolset_mcp_multiple]

No MCP server for object storage ships with this provider. If one exists for
your target, it is a third route alongside the hook and DataFusion routes (:doc:`hook`,
:doc:`datafusion`) — and the two questions in :ref:`Choosing a toolset <howto/toolsets>` settle
it. Its tool list is the server's rather than
yours, and its token comes from wherever ``mcp_conn_id`` or your own callable
says. So where a hook already reaches the same target, the hook wins; the server
wins where it covers work the hook does not expose.

**Credentials and where it runs.** ``mcp_conn_id`` supplies host, credentials and
transport, unless a provider callable overrides that. ``http`` and ``sse`` reach
a remote server; ``stdio`` runs a child process on the worker host.
