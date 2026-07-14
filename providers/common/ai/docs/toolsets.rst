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
    just the Airflow-native toolsets above. PydanticAI's own ``MCPToolset``
    (built over a FastMCP transport) and third-party toolsets work too. The
    Airflow-native toolsets add connection management, secret backend
    integration, and the connection UI, but you are not locked in.


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

In read-only mode (``allow_writes=False``, the default) the ``query`` tool also
accepts read-only metadata statements -- ``DESCRIBE``/``DESC`` and ``SHOW`` --
in addition to SELECT-family queries. Agents commonly open with ``DESCRIBE`` to
learn a table's columns, so permitting it keeps runs deterministic instead of
hard-failing on schema discovery. The toolset passes the connection's dialect to
the validator, so ``SHOW`` is recognized on databases that support it (Snowflake,
MySQL, etc.); on databases without ``SHOW`` it stays rejected. Data-modifying
statements remain blocked -- including ones hidden behind ``DESCRIBE``/``EXPLAIN``
(e.g. ``EXPLAIN DELETE ...``, ``DESCRIBE DROP TABLE ...``), which the validator
rejects by scanning the parsed statement for write operations. When
``allowed_tables`` is set it scopes these statements too: a ``DESCRIBE`` names a
table, so its target must be on the list, while ``SHOW`` enumerates objects beyond
any single table and is rejected outright (see :ref:`allowed-tables-enforcement`).

Multi-schema warehouses
^^^^^^^^^^^^^^^^^^^^^^^^^

When an agent's tables live in several schemas of one database -- common on
Snowflake -- list them with schema-qualified ``allowed_tables`` entries:

.. code-block:: python

    SQLToolset(
        db_conn_id="snowflake_hq",
        allowed_tables=["MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS", "MODEL_CRM.SF_ASTRO_ORGS"],
    )

``list_tables`` then introspects each referenced schema and returns the matching
tables fully qualified (e.g. ``MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS``), and
``get_schema`` routes each qualified name to its own schema. Without this, a
single ``schema`` only covers one namespace, and leaving ``schema`` unset made
introspection query a literal ``"None"`` schema and fail. Unqualified entries
fall back to ``schema``, and table-name matching is case-insensitive (databases
reflect identifiers in their own case). For tables in a different *database*, use
a separate toolset whose connection points at that database.

Parameters
^^^^^^^^^^

- ``db_conn_id``: Airflow connection ID for the database.
- ``allowed_tables``: Restrict the agent to a fixed set of tables. ``None``
  (default) exposes all tables in ``schema``. Entries may be schema-qualified
  (``"SCHEMA.TABLE"``) to span multiple schemas; see above. Matching is
  case-insensitive. When set, the list is enforced on ``query`` and
  ``check_query`` as well as discovery -- every table a query references must be
  on it. See :ref:`allowed-tables-enforcement` for what this does and does not
  guarantee.
- ``schema``: Default schema/namespace for unqualified table listing and
  introspection. Schema-qualified ``allowed_tables`` entries override it per table.
- ``allow_writes``: Allow data-modifying SQL (INSERT, UPDATE, DELETE, etc.).
  Default ``False`` -- only SELECT-family and read-only metadata
  (``DESCRIBE``/``SHOW``) statements are permitted.
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

Direct PydanticAI MCP Toolsets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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


.. _agent-skills:

``AgentSkillsToolset``
----------------------

:class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset` loads
`Agent Skills <https://agentskills.io>`__ -- ``SKILL.md`` bundles (instructions,
and optionally scripts and resources) that the model discovers and loads *on
demand*. Only a compact catalog of skill names and descriptions sits in the
prompt until the model decides it needs one, so a large skill library costs few
tokens until used (progressive disclosure).

It is backed by the community `pydantic-ai-skills
<https://github.com/DougTrajano/pydantic-ai-skills>`__ package (MIT); native
progressive disclosure is in flight upstream in `pydantic/pydantic-ai#5230
<https://github.com/pydantic/pydantic-ai/pull/5230>`__. Install the optional
extra to use it:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[skills]"

Each source is a local directory or a connection-resolved
:class:`~airflow.providers.common.ai.skills.GitSkills`. Sources are resolved when
the agent enters the toolset, on the worker -- never while the Dag processor
parses the file -- so a Git token is never baked into the serialized Dag, and
cloned repositories are removed when the run ends.

A local directory of ``SKILL.md`` bundles:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_local]
    :end-before: [END howto_operator_agent_skills_local]

A Git repository, with credentials from an Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_git]
    :end-before: [END howto_operator_agent_skills_git]

For a private repository, point ``conn_id`` at a
:doc:`git connection <apache-airflow-providers-git:connections/git>`; credentials
are resolved through the Git provider's ``GitHook`` (an HTTPS token in the
connection password, or an SSH key in the connection's extra). A plain ``http://``
URL with ``conn_id`` is rejected so a credential is never sent in cleartext, and a
``repo_url`` that embeds a username/password is rejected (use ``conn_id``). After
cloning, the credential is stripped from the checkout's ``.git/config``. As with
any ``git clone``, the worker's own git configuration (credential helpers, SSH
agent) may still apply, so run workers without ambient git credentials if you
need strict isolation.

.. warning::

    Skill bundles can contain scripts that the agent may run on the worker via
    the ``run_skill_script`` tool. For a remote source, anyone who can modify the
    repository can introduce code that executes on your worker, outside Dag
    review and versioning. Point ``GitSkills`` at a trusted repository, pin
    ``branch`` to a trusted ref, and treat skill contents as code that runs in
    your environment.

Parameters
^^^^^^^^^^

- ``sources``: List of skill sources -- local directory paths and/or
  :class:`~airflow.providers.common.ai.skills.GitSkills`.
- ``exclude_tools``: Optional set of skill tool names to hide from the agent
  (e.g. ``{"run_skill_script"}`` to disable on-worker script execution).

Using Agent Skills with other frameworks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``AgentSkillsToolset`` is a standard pydantic-ai toolset, so it also works with a
plain ``pydantic_ai.Agent`` you build yourself, not just ``AgentOperator``.

Because Agent Skills is a cross-framework format, the connection handling is also
reusable through :func:`~airflow.providers.common.ai.skills.resolve_skills`, which
resolves sources to local ``SKILL.md`` directories that any loader accepts:

.. code-block:: python

    from airflow.providers.common.ai.skills import GitSkills, resolve_skills

    sources = ["./skills", GitSkills(repo_url="https://github.com/org/skills", conn_id="github_skills")]
    with resolve_skills(sources) as dirs:
        # LangChain DeepAgents
        agent = create_deep_agent(model="openai:gpt-5.4", skills=dirs)
        # ...or Strands
        agent = Agent(plugins=[AgentSkills(skills=dirs)])

``resolve_skills`` needs the Git provider (for ``GitSkills``) but not pydantic-ai,
and removes any cloned directories when the ``with`` block exits.

Working with LangChain
----------------------

Tools bridge in both directions between common.ai's toolsets and LangChain.

**LangChain tools → ``AgentOperator``.** No Airflow code is needed. pydantic-ai
ships `pydantic_ai.ext.langchain.LangChainToolset
<https://ai.pydantic.dev/toolsets/>`__ upstream, which wraps existing LangChain
tools as an ``AbstractToolset``. Drop it straight into ``AgentOperator``:

.. code-block:: python

    from pydantic_ai.ext.langchain import LangChainToolset

    AgentOperator(
        task_id="agent_with_langchain_tools",
        prompt="Research the question and summarise.",
        llm_conn_id="pydanticai_default",
        toolsets=[LangChainToolset([my_langchain_tool])],
    )

**common.ai toolsets → LangChain.** The reverse direction is what
:func:`~airflow.providers.common.ai.toolsets.langchain_bridge.airflow_toolset_to_langchain_tools`
provides. It converts any pydantic-ai toolset -- including ``SQLToolset``,
``HookToolset``, and ``MCPToolset`` -- into a list of LangChain
``StructuredTool`` objects, so a LangChain agent or chain can call Airflow's
curated, connection-managed tools:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_langchain_toolset_bridge.py
    :language: python
    :start-after: [START example_langchain_toolset_bridge]
    :end-before: [END example_langchain_toolset_bridge]

Each generated tool keeps the source tool's name, description, and argument
schema, and routes calls back through the original toolset, so the toolset's own
behaviour (connection resolution, ``SQLToolset``'s SQL validation, and
``allowed_tables`` filtering) still applies. ``get_tools`` runs eagerly at
conversion time to enumerate the tools.

When a toolset raises pydantic-ai's ``ModelRetry`` to ask the model to correct
its input (``SQLToolset`` does this on, for example, an unknown column), the
bridge returns that message as the tool's output so the model sees it and tries
again. ``ModelRetry`` is a feed-the-model-and-retry signal rather than a
failure, so returning it preserves the self-correction the toolset was written
for and works no matter how the agent is configured to handle tool errors
(raising would abort the run under ``create_agent``'s default handling).

The bridge does not hold a toolset session open across calls: ``get_tools`` and
every tool call each run under their own event loop, so for ``MCPToolset`` the
connection is opened and torn down around each call. It reconnects per call,
which is fine for stateless tools but unsuitable for ``stdio`` MCP servers (or
any server that keeps state between calls), since each call starts a fresh
session.

.. note::

    Outside an agent run there is no live ``RunContext``, so the bridge builds a
    minimal one with an inert placeholder model. The bundled toolsets ignore the
    context, so this is transparent for them. A custom toolset that reads live
    run state (``ctx.model``, ``ctx.messages``, ``ctx.usage``) will not behave
    correctly when bridged standalone.

Requires the ``langchain`` extra:
``pip install "apache-airflow-providers-common-ai[langchain]"``


Security
--------

LLM agents call tools based on natural-language reasoning. This makes them
powerful but introduces risks that don't exist with deterministic operators.

What the agent can and cannot reach
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
     - Cannot police data reached through side-effecting scalar functions
       (e.g. ``pg_read_file``), and is only as exact as the SQL parser. Pair it
       with least-privilege database grants. See
       :ref:`allowed-tables-enforcement` below.
   * - **SQLToolset: max_rows**
     - Truncates query results to ``max_rows`` (default 50), preventing the
       agent from pulling entire tables into context.
     - Does not limit the number of queries the agent can make.
   * - **MCPToolset: external server**
     - Connects the agent to tools exposed by an MCP server, authenticated
       through an Airflow connection.
     - Does **not** constrain what those tools do. An MCP server can expose
       shell, filesystem, or network access. Run only trusted servers and
       audit the tools they expose.
   * - **pydantic-ai: tool call budget**
     - pydantic-ai's ``max_result_retries`` and ``model_settings`` control
       how many tool-call rounds the agent can make before stopping.
     - Requires explicit configuration — the default allows many rounds.


.. _allowed-tables-enforcement:

How ``allowed_tables`` Is Enforced
""""""""""""""""""""""""""""""""""

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
  ``TABLE <name>`` shorthand, ``SHOW``, dynamic SQL (``EXEC``), and **inline
  comments** -- the last because parser-vs-engine differences hide in comments
  (MySQL executes ``/*! ... */`` while sqlglot and other engines ignore it).

So ``SELECT * FROM secrets`` with ``allowed_tables=["orders"]`` is refused, and
the rejection is handed back to the agent so it can re-target an allowed table.

This is a strong **application-level guardrail**, but it is not a substitute for
database permissions. It cannot police data reached through a function whose
argument is itself SQL or a path: ``pg_read_file('/etc/passwd')`` reads a file,
and ``query_to_xml('SELECT * FROM other_table', ...)`` or a scalar ``dblink``
reads a table through a string the parser cannot inspect. Any query the engine
parses differently from sqlglot is also a residual gap. For a hard boundary, also
run the connection as a least-privilege role:

.. code-block:: sql

    -- Create a read-only role with access to specific tables only
    CREATE ROLE airflow_agent_reader;
    GRANT SELECT ON orders, customers TO airflow_agent_reader;
    -- Use this role's credentials in the Airflow connection

Defense in depth: the allow-list contains the agent's *intent* (and gives it a
correctable error), while the database role is the boundary that holds even if
the agent reaches data the parser cannot see. The connection should use a
database user with the minimum privileges required.


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
   API keys in Dag files.
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
