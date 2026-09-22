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

Securing agent tools
====================

LLM agents call tools based on natural-language reasoning. This makes them
powerful but introduces risks that don't exist with deterministic operators.

What the agent can and cannot reach
-------------------------------------

An agent's reach is exactly the set of tools you register on it, and nothing
more. The model never executes arbitrary code: it can only request one of the
tools you provided, and pydantic-ai rejects any tool name outside that set
before it runs. If no registered tool can read the environment, the
filesystem, or other connections, the model cannot reach them, regardless of
what the prompt instructs it to do.

This is what "untrusted" means in this context. The Dag file itself is
author-written and trusted, exactly like any other Dag. What is untrusted is
the model's *output*: the tool-call requests and text it generates. That output
is confined to your registered tools and bounded by the tool-call budget. An
agent cannot create a new connection, read another connection's credentials, or
run a shell command unless a tool you registered exposes that capability.

The corollary is that every tool you add widens the blast radius, and a custom
toolset is only as safe as you make it. A tool that returns ``os.environ`` or
runs shell commands hands the model whatever that tool can reach. Audit any
custom toolset, and any MCP server you connect through ``MCPToolset``, against
the same standard the bundled toolsets below are built to.

.. _toolset-defense-layers:

Defense layers
--------------

No single layer is sufficient — they work together.

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Layer
     - What it does
     - What it does NOT do
   * - **Airflow Connections**
     - Credentials are stored in Airflow's secret backend, never in Dag code.
       The LLM agent cannot see API keys or database passwords.
     - Does not prevent the agent from using the connection to access data
       the connection has access to.
   * - **HookToolset: explicit allow-list**
     - Only methods listed in ``allowed_methods`` are exposed as tools.
       Auto-discovery is not supported. Methods are validated at Dag parse
       time.
     - Does not restrict what arguments the agent passes to allowed methods.
   * - **SQLToolset: read-only by default**
     - ``allow_writes=False`` (default) validates every SQL query through
       ``validate_sql()``: SELECT-family and read-only metadata
       (``DESCRIBE``/``SHOW``) statements pass; INSERT, UPDATE, DELETE, DROP,
       and writes hidden behind ``EXPLAIN`` are rejected.
     - Does not prevent the agent from reading sensitive data that the
       database user has SELECT access to.
   * - **DataFusionToolset: read-only by default**
     - ``allow_writes=False`` (default) validates every SQL query through
       ``validate_sql()`` and rejects CREATE TABLE, CREATE VIEW, INSERT
       INTO, and other non-SELECT statements.
     - Does not prevent the agent from reading any registered data source.
   * - **SQLToolset: allowed_tables**
     - Restricts the agent to listed tables across ``list_tables``,
       ``get_schema``, ``query``, and ``check_query``. Queries are parsed and
       every referenced table (including via subqueries, CTEs, JOINs, and
       ``DESCRIBE``) is checked against the list before execution.
     - Rejects ``COPY`` and every function sqlglot cannot type (the channel for
       ``pg_read_file`` / ``query_to_xml`` / ``dblink``) unless named in
       ``allowed_functions``. Fail-closed, but only as exact as the SQL parser. Not a
       security boundary -- always pair it with least-privilege database grants. See
       :ref:`allowed-tables-enforcement` below.
   * - **SQLToolset: max_rows / max_result_bytes**
     - Bounds a query result by rows (default 50) and by serialized size
       (default 64 KiB), preventing the agent from pulling entire tables into
       context.
     - Does not limit the number of queries the agent can make, and each result
       stays in message history for the rest of the run. Rows past ``max_rows``
       are not read out of the cursor, but a client-buffering driver has already
       transferred them -- this bounds context, not database or network load.
   * - **MCPToolset: external server**
     - Connects the agent to tools exposed by an MCP server, authenticated
       through an Airflow connection.
     - Does **not** constrain what those tools do. An MCP server can expose
       shell, filesystem, or network access. Run only trusted servers and
       audit the tools they expose.
   * - **SandboxToolset: off-worker execution**
     - Runs the agent's commands and file operations in a disposable microVM,
       never in the worker process. Airflow injects nothing of its own; only
       what ``SandboxSpec`` names goes in, and egress is denied by default. A
       backend that cannot enforce a spec field raises rather than ignoring it.
       Commands are bounded by a timeout ceiling and per-stream output caps.
     - **Does not contain the agent.** The agent loop and every other toolset on
       the same agent still run in the worker with its credentials, so this does
       not stop an agent reaching connections through some other tool. It also
       does not sanitize what the code computes or returns. Custom images can
       carry secrets and a backend you add can expose its own identity. The
       ``sbx`` backend leaks orphaned microVMs if the worker is killed, and its
       CPU allocation defaults to every host CPU; the Modal backend reclaims its
       sandboxes on a server-side timeout, and if you opt into
       ``egress_enforcement="sni"`` its hostname allowlist is enforced at the TLS
       layer and leaves DNS resolution open.
   * - **pydantic-ai: tool call budget**
     - pydantic-ai's ``max_result_retries`` and ``model_settings`` control
       how many tool-call rounds the agent can make before stopping.
     - Requires explicit configuration — the default allows many rounds.

.. _allowed-tables-enforcement:

How ``allowed_tables`` is enforced
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When ``allowed_tables`` is set it governs every tool, not just discovery:

- ``list_tables`` and ``get_schema`` only reveal listed tables.
- ``query`` and ``check_query`` parse the SQL with `sqlglot
  <https://github.com/tobymao/sqlglot>`_ and reject it before execution if it
  references any table that is not on the list. Tables reached indirectly are
  caught too -- through subqueries, CTEs, JOINs, set operations (``UNION`` etc.),
  ``DESCRIBE``, catalog views such as ``information_schema``, and DML. CTE
  references are excluded by lexical scope, so a same-named CTE in another scope
  cannot hide a real table, and the database/catalog is part of the match, so a
  cross-database reference like ``otherdb.public.orders`` is refused.
- Constructs the list cannot describe are rejected outright while it is active:
  table-valued functions (``dblink``), ``TABLE('name')`` row sources, the
  ``TABLE <name>`` shorthand, ``SHOW``, dynamic SQL (``EXEC``), ``COPY``
  (file/program I/O), and **inline comments** -- because parser-vs-engine differences
  hide in comments (MySQL executes ``/*! ... */`` while sqlglot and other engines
  ignore it).
- **Any function sqlglot does not recognize is rejected (fail-closed).** A function
  whose string argument reaches data outside the table graph --
  ``pg_read_file('/etc/passwd')`` (a file), ``query_to_xml('SELECT * FROM other_table', ...)``
  (SQL over another table), a scalar ``dblink`` (a remote database) -- carries no table
  reference for the parser to catch. Rather than maintain a denylist of such functions
  (unbounded, engine-specific, and it would fail *open* on anything missed), the toolset
  rejects every function sqlglot cannot type. Ordinary builtins (``count``, ``lower``,
  ``sum``) are recognized and pass. A legitimate function sqlglot does not type
  (``json_build_object``, ``jsonb_agg``) or a project UDF is rejected until you list it
  in ``allowed_functions``:

  .. code-block:: python

      SQLToolset(
          db_conn_id="analytics_db",
          allowed_tables=["orders"],
          allowed_functions=["json_build_object"],  # opt in per function you trust
      )

So ``SELECT * FROM secrets`` with ``allowed_tables=["orders"]`` is refused, and
the rejection is handed back to the agent so it can re-target an allowed table.

.. warning::

    This is a strong **application-level guardrail, not a security boundary.** The
    fail-closed function check raises the bar, but any query the engine parses
    differently from sqlglot is a residual gap, and ``allowed_functions`` is a trust
    decision you own. **Always** point the connection at a least-privilege database
    role -- that is the boundary that holds even when the parser cannot see through a
    function, and it is what actually keeps an agent (which may be under prompt
    injection) away from data and files you have not granted it:

    .. code-block:: sql

        -- Create a read-only role with access to specific tables only
        CREATE ROLE airflow_agent_reader;
        GRANT SELECT ON orders, customers TO airflow_agent_reader;
        -- Use this role's credentials in the Airflow connection

Defense in depth: the allow-list contains the agent's *intent* (and gives it a
correctable error), while the database role is the boundary that holds even if
the agent reaches data the parser cannot see. The connection should use a
database user with the minimum privileges required.

``HookToolset`` guidelines
^^^^^^^^^^^^^^^^^^^^^^^^^^

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

Recommended configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

**Read-only analytics** (the most common pattern):

.. code-block:: python

    SQLToolset(
        db_conn_id="analytics_readonly",  # Connection with SELECT-only grants
        allowed_tables=["orders", "customers"],  # Hide other tables from agent
        allow_writes=False,  # Default — validates SQL
        max_rows=50,  # Default — cap rows
        max_result_bytes=65536,  # Default — cap bytes; lower it for wide tables
    )

**Agents that need to modify data** (use with caution):

.. code-block:: python

    SQLToolset(
        db_conn_id="app_db",
        allowed_tables=["user_preferences"],
        allow_writes=True,  # Disables SQL validation — agent can INSERT/UPDATE
        max_rows=100,
    )

Production checklist
^^^^^^^^^^^^^^^^^^^^

Before deploying an agent task to production:

1. **Connection credentials**: Use Airflow's secret backend. Never hardcode
   API keys in Dag files.
2. **Database permissions**: Create a dedicated database user with minimum
   required grants. Don't reuse the admin connection.
3. **Tool allow-list**: Review ``allowed_methods`` / ``allowed_tables``. The
   agent can call any exposed tool with any arguments.
4. **Read-only default**: Keep ``allow_writes=False`` unless the task
   specifically requires writes.
5. **Result limits**: Set ``max_rows`` and ``max_result_bytes`` appropriate to
   the use case. ``max_rows`` alone does not bound size -- on wide tables it is
   ``max_result_bytes`` that keeps a result from dominating the context window for
   the rest of the run.
6. **Model budget**: Configure pydantic-ai's ``model_settings`` (e.g.
   ``max_tokens``) and ``retries`` to bound cost and prevent runaway loops.
7. **System prompt**: Include safety instructions in ``system_prompt`` (e.g.
   "Only query tables related to the question. Never modify data.").
8. **Prompt injection**: Be cautious when the prompt includes untrusted data
   (user input, external API responses, upstream XCom). Consider sanitizing
   inputs before passing them to the agent.
