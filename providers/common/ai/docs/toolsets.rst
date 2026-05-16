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

.. _howto/toolsets:

Toolsets — Airflow Hooks as AI Agent Tools
==========================================

Airflow's 350+ provider hooks already have typed methods, rich docstrings,
and managed credentials. Toolsets expose them as pydantic-ai tools so that
LLM agents can call them during multi-turn reasoning.

Three toolsets are included:

- :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset` — generic
  adapter for any Airflow Hook.
- :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` — curated
  4-tool database toolset.
- :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` — connect to
  `MCP servers <https://modelcontextprotocol.io/>`__ configured via Airflow
  connections.

All three implement pydantic-ai's
`AbstractToolset <https://ai.pydantic.dev/toolsets/>`__ interface and can be
passed to any pydantic-ai ``Agent``, including via
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.

.. note::

    ``AgentOperator`` accepts **any** ``AbstractToolset`` implementation — not
    just the Airflow-native toolsets above. PydanticAI's own MCP server
    classes (``MCPServerStreamableHTTP``, ``MCPServerSSE``, ``MCPServerStdio``)
    and third-party toolsets work too. The Airflow-native toolsets add
    connection management, secret backend integration, and the connection UI,
    but you are not locked in.


Using Toolsets Directly with PydanticAI
---------------------------------------

Toolsets are standard pydantic-ai ``AbstractToolset`` implementations with no
dependency on ``AgentOperator`` or ``@task.agent``. You can use them anywhere
you can run Python within Airflow -- ``@task`` functions, ``PythonOperator``
callables, or any custom operator's ``execute()`` method -- by creating a
``pydantic_ai.Agent`` yourself:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_hook.py
    :language: python
    :start-after: [START howto_task_with_toolsets]
    :end-before: [END howto_task_with_toolsets]

This works because toolsets resolve Airflow connections lazily via
``BaseHook.get_connection()``, which is available in any task execution
context.

This approach gives you full control over the agent lifecycle -- you can call
``agent.run_sync()`` multiple times, swap models at runtime, or combine
results from several agents in a single task. The tradeoff is that you lose
the durable execution (step-level caching with retry replay), HITL review
integration, and automatic tool call logging that ``AgentOperator`` provides.


``HookToolset``
---------------

Generic adapter that exposes selected methods of any Airflow Hook as
pydantic-ai tools via introspection. Requires an explicit ``allowed_methods``
list — there is no auto-discovery.

.. code-block:: python

    from airflow.providers.http.hooks.http import HttpHook
    from airflow.providers.common.ai.toolsets.hook import HookToolset

    http_hook = HttpHook(http_conn_id="my_api")

    toolset = HookToolset(
        http_hook,
        allowed_methods=["run"],
        tool_name_prefix="http_",
    )

For each listed method, the introspection engine:

1. Builds a JSON Schema from the method signature (``inspect.signature`` +
   ``get_type_hints``).
2. Extracts the description from the first paragraph of the docstring.
3. Enriches parameter descriptions from Sphinx ``:param:`` or Google
   ``Args:`` blocks.

Parameters
^^^^^^^^^^

- ``hook``: An instantiated Airflow Hook.
- ``allowed_methods``: Method names to expose as tools. Required. Methods
  are validated with ``hasattr`` + ``callable`` at instantiation time.
- ``tool_name_prefix``: Optional prefix prepended to each tool name
  (e.g. ``"s3_"`` produces ``"s3_list_keys"``).


``SQLToolset``
--------------

Curated toolset wrapping
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` with four tools:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Tool
     - Description
   * - ``list_tables``
     - Lists available table names (filtered by ``allowed_tables`` if set)
   * - ``get_schema``
     - Returns column names and types for a table
   * - ``query``
     - Executes a SQL query and returns rows as JSON
   * - ``check_query``
     - Validates SQL syntax without executing it

.. code-block:: python

    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    toolset = SQLToolset(
        db_conn_id="postgres_default",
        allowed_tables=["customers", "orders"],
        max_rows=20,
    )

The ``DbApiHook`` is resolved lazily from ``db_conn_id`` on first tool call
via ``BaseHook.get_connection(conn_id).get_hook()``.

Parameters
^^^^^^^^^^

- ``db_conn_id``: Airflow connection ID for the database.
- ``allowed_tables``: Restrict which tables the agent can discover via
  ``list_tables`` and ``get_schema``. ``None`` (default) exposes all tables.
  See :ref:`allowed-tables-limitation` for an important caveat.
- ``schema``: Database schema/namespace for table listing and introspection.
- ``allow_writes``: Allow data-modifying SQL (INSERT, UPDATE, DELETE, etc.).
  Default ``False`` — only SELECT-family statements are permitted.
- ``max_rows``: Maximum rows returned from the ``query`` tool. Default ``50``.

``DataFusionToolset``
---------------------

Curated toolset wrapping
:class:`~airflow.providers.common.sql.datafusion.engine.DataFusionEngine`
with three tools — ``list_tables``, ``get_schema``, and ``query`` — for
querying files on object stores (S3, local filesystem, Iceberg) via Apache DataFusion.

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Tool
     - Description
   * - ``list_tables``
     - Lists registered table names
   * - ``get_schema``
     - Returns column names and types for a table (Arrow schema)
   * - ``query``
     - Executes a SQL query and returns rows as JSON

Each :class:`~airflow.providers.common.sql.config.DataSourceConfig` entry
registers a table backed by Parquet, CSV, Avro, or Iceberg data. Multiple
configs can be registered so that SQL queries can join across tables.

.. code-block:: python

    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.sql.config import DataSourceConfig

    toolset = DataFusionToolset(
        datasource_configs=[
            DataSourceConfig(
                conn_id="aws_default",
                table_name="sales",
                uri="s3://my-bucket/data/sales/",
                format="parquet",
            ),
            DataSourceConfig(
                conn_id="aws_default",
                table_name="returns",
                uri="s3://my-bucket/data/returns/",
                format="csv",
            ),
        ],
        max_rows=100,
    )

The ``DataFusionEngine`` is created lazily on the first tool call. This
toolset requires the ``datafusion`` extra of
``apache-airflow-providers-common-sql``.

Parameters
^^^^^^^^^^

- ``datasource_configs``: One or more
  :class:`~airflow.providers.common.sql.config.DataSourceConfig` entries.
  Requires ``apache-airflow-providers-common-sql[datafusion]``.
- ``allow_writes``: Allow data-modifying SQL (CREATE TABLE, CREATE VIEW,
  INSERT INTO, etc.). Default ``False`` — only SELECT-family statements are
  permitted. DataFusion on object stores is mostly read-only, but it does
  support DDL for in-memory tables; this guard blocks those by default.
- ``max_rows``: Maximum rows returned from the ``query`` tool. Default ``50``.

``LoggingToolset``
------------------

:class:`~airflow.providers.common.ai.toolsets.logging.LoggingToolset` is a
``WrapperToolset`` that intercepts ``call_tool()`` to log each tool invocation
in real time. ``AgentOperator`` applies it automatically (see
``enable_tool_logging``), but you can also use it directly with any pydantic-ai
``Agent``:

.. code-block:: python

    from airflow.providers.common.ai.toolsets.logging import LoggingToolset
    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    sql_toolset = SQLToolset(db_conn_id="my_db")
    logged_toolset = LoggingToolset(wrapped=sql_toolset, logger=my_logger)

Each tool call produces two INFO log lines (name + timing) and optional
DEBUG-level argument logging. Exceptions are logged and re-raised.


``MCPToolset``
--------------

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
^^^^^^^^^^

- ``mcp_conn_id``: Airflow connection ID for the MCP server.
- ``tool_prefix``: Optional prefix prepended to tool names to avoid
  collisions when using multiple MCP servers (e.g. ``"weather"`` produces
  ``"weather_get_forecast"``).

Using Multiple MCP Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^

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

Direct PydanticAI MCP Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For prototyping or when you want full PydanticAI control, you can pass MCP
server instances directly — no Airflow connection needed:

.. code-block:: python

    from pydantic_ai.mcp import MCPServerStreamableHTTP, MCPServerStdio

    AgentOperator(
        task_id="direct_mcp",
        prompt="What tools are available?",
        llm_conn_id="pydanticai_default",
        toolsets=[
            MCPServerStreamableHTTP("http://localhost:3001/mcp"),
            MCPServerStdio("uvx", args=["mcp-run-python"]),
        ],
    )

This works because PydanticAI's MCP server classes implement
``AbstractToolset``. The tradeoff: URLs and credentials are hardcoded in DAG
code instead of being managed through Airflow connections and secret backends.


Security
--------

LLM agents call tools based on natural-language reasoning. This makes them
powerful but introduces risks that don't exist with deterministic operators.

Defense Layers
^^^^^^^^^^^^^^

No single layer is sufficient — they work together.

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Layer
     - What it does
     - What it does NOT do
   * - **Airflow Connections**
     - Credentials are stored in Airflow's secret backend, never in DAG code.
       The LLM agent cannot see API keys or database passwords.
     - Does not prevent the agent from using the connection to access data
       the connection has access to.
   * - **HookToolset: explicit allow-list**
     - Only methods listed in ``allowed_methods`` are exposed as tools.
       Auto-discovery is not supported. Methods are validated at DAG parse
       time.
     - Does not restrict what arguments the agent passes to allowed methods.
   * - **SQLToolset: read-only by default**
     - ``allow_writes=False`` (default) validates every SQL query through
       ``validate_sql()`` and rejects INSERT, UPDATE, DELETE, DROP, etc.
     - Does not prevent the agent from reading sensitive data that the
       database user has SELECT access to.
   * - **DataFusionToolset: read-only by default**
     - ``allow_writes=False`` (default) validates every SQL query through
       ``validate_sql()`` and rejects CREATE TABLE, CREATE VIEW, INSERT
       INTO, and other non-SELECT statements.
     - Does not prevent the agent from reading any registered data source.
   * - **SQLToolset: allowed_tables**
     - Restricts which tables appear in ``list_tables`` and ``get_schema``
       responses, limiting the agent's knowledge of the schema.
     - Does **not** validate table references in SQL queries. The agent can
       still query unlisted tables if it guesses the name. See
       :ref:`allowed-tables-limitation` below.
   * - **SQLToolset: max_rows**
     - Truncates query results to ``max_rows`` (default 50), preventing the
       agent from pulling entire tables into context.
     - Does not limit the number of queries the agent can make.
   * - **pydantic-ai: tool call budget**
     - pydantic-ai's ``max_result_retries`` and ``model_settings`` control
       how many tool-call rounds the agent can make before stopping.
     - Requires explicit configuration — the default allows many rounds.


.. _allowed-tables-limitation:

The ``allowed_tables`` Limitation
"""""""""""""""""""""""""""""""""

``allowed_tables`` is a **metadata filter**, not an access control mechanism.
It hides table names from ``list_tables`` and blocks ``get_schema`` for
unlisted tables, but does not parse SQL queries to validate table references.

An LLM can craft ``SELECT * FROM secrets`` even when
``allowed_tables=["orders"]``. Parsing SQL for table references (including
CTEs, subqueries, aliases, and vendor-specific syntax) is complex and
error-prone; we chose not to provide a false sense of security.

For query-level restrictions, use database permissions:

.. code-block:: sql

    -- Create a read-only role with access to specific tables only
    CREATE ROLE airflow_agent_reader;
    GRANT SELECT ON orders, customers TO airflow_agent_reader;
    -- Use this role's credentials in the Airflow connection

The Airflow connection should use a database user with the minimum privileges
required.


HookToolset Guidelines
""""""""""""""""""""""

- List only the methods the agent needs. Never expose ``run()`` or
  ``get_connection()`` — these give broad access.
- Prefer read-only methods (``list_*``, ``get_*``, ``describe_*``).
- The agent controls arguments. If a method accepts a ``path`` parameter,
  the agent can pass any path the hook has access to.

.. code-block:: python

    # Good: expose only list and read
    HookToolset(
        s3_hook,
        allowed_methods=["list_keys", "read_key"],
        tool_name_prefix="s3_",
    )

    # Bad: exposes delete and write operations
    HookToolset(
        s3_hook,
        allowed_methods=["list_keys", "read_key", "delete_object", "load_string"],
    )


Recommended Configuration
"""""""""""""""""""""""""

**Read-only analytics** (the most common pattern):

.. code-block:: python

    SQLToolset(
        db_conn_id="analytics_readonly",  # Connection with SELECT-only grants
        allowed_tables=["orders", "customers"],  # Hide other tables from agent
        allow_writes=False,  # Default — validates SQL
        max_rows=50,  # Default — truncate large results
    )

**Agents that need to modify data** (use with caution):

.. code-block:: python

    SQLToolset(
        db_conn_id="app_db",
        allowed_tables=["user_preferences"],
        allow_writes=True,  # Disables SQL validation — agent can INSERT/UPDATE
        max_rows=100,
    )


Production Checklist
""""""""""""""""""""

Before deploying an agent task to production:

1. **Connection credentials**: Use Airflow's secret backend. Never hardcode
   API keys in DAG files.
2. **Database permissions**: Create a dedicated database user with minimum
   required grants. Don't reuse the admin connection.
3. **Tool allow-list**: Review ``allowed_methods`` / ``allowed_tables``. The
   agent can call any exposed tool with any arguments.
4. **Read-only default**: Keep ``allow_writes=False`` unless the task
   specifically requires writes.
5. **Row limits**: Set ``max_rows`` appropriate to the use case. Large
   result sets consume LLM context and increase cost.
6. **Model budget**: Configure pydantic-ai's ``model_settings`` (e.g.
   ``max_tokens``) and ``retries`` to bound cost and prevent runaway loops.
7. **System prompt**: Include safety instructions in ``system_prompt`` (e.g.
   "Only query tables related to the question. Never modify data.").
8. **Prompt injection**: Be cautious when the prompt includes untrusted data
   (user input, external API responses, upstream XCom). Consider sanitizing
   inputs before passing them to the agent.
