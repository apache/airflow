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

Four toolsets are exported directly from the ``airflow.providers.common.ai.toolsets``
package root:

- :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset` — generic
  adapter for any Airflow Hook.
- :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` — curated
  4-tool database toolset.
- :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` — connect to
  `MCP servers <https://modelcontextprotocol.io/>`__ configured via Airflow
  connections.
- :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` — give
  the agent a shell and a filesystem inside an isolated sandbox, off the
  Airflow worker. See :ref:`which boundary that is <sandbox-boundaries>`.

A fourth pair,
:class:`~airflow.providers.common.ai.toolsets.managed_agent.BaseManagedAgentToolset`
and
:class:`~airflow.providers.common.ai.toolsets.managed_agent.FailoverManagedAgentToolset`,
covers **vendor-managed agents** -- agents whose reasoning loop runs on a cloud
provider's infrastructure. The first is a base class that provider packages
subclass; the second composes several interchangeable ones behind a single tool.
See :ref:`managed-agent-toolsets` below.

All of them implement pydantic-ai's
Three more toolsets are documented later on this page. They are not re-exported from the
package root, so import each of them from its own submodule::

    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.ai.toolsets.logging import LoggingToolset
    from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset

All of the toolsets on this page implement pydantic-ai's
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
     - Executes a SQL query and returns bounded, columnar JSON (see
       :ref:`bounded-query-results`)
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
- ``allowed_functions``: Names of functions that sqlglot does not recognize as
  builtins but are safe to run while ``allowed_tables`` is active (e.g.
  ``["json_build_object"]`` or a project UDF). ``None`` (default) rejects every
  unrecognized function. Matching is case-insensitive. Only consulted when
  ``allowed_tables`` is set.
- ``schema``: Default schema/namespace for unqualified table listing and
  introspection. Schema-qualified ``allowed_tables`` entries override it per table.
- ``allow_writes``: Allow data-modifying SQL (INSERT, UPDATE, DELETE, etc.).
  Default ``False`` -- only SELECT-family and read-only metadata
  (``DESCRIBE``/``SHOW``) statements are permitted.
- ``max_rows``: Maximum rows returned from the ``query`` tool. Default ``50``.
  Rows beyond it are not read out of a DBAPI cursor; what the driver has already
  transferred is its own call. See :ref:`bounded-query-results`.
- ``max_result_bytes``: Budget for the serialized ``query`` result. Default 64 KiB.
  See :ref:`bounded-query-results`.

.. _bounded-query-results:

Bounded query results
^^^^^^^^^^^^^^^^^^^^^

A tool result stays in the model's message history for the rest of the run, so its
cost is re-paid on every subsequent model request. The ``query`` tool of both
``SQLToolset`` and ``DataFusionToolset`` bounds that in three ways.

**The result is columnar.** Column names appear once, not once per row:

.. code-block:: json

    {"columns": ["id", "name"], "rows": [[1, "Alice"], [2, "Bob"]], "row_count": 2}

On a table with thousands of columns the repeated names, not the values, are the bulk
of a row-of-dicts payload. Positional rows also keep columns that share a name --
``SELECT o.id, c.id`` -- which a dict per row silently collapsed to one.

**Rows are fetched, not filtered.** ``max_rows`` bounds what leaves the cursor, so a
query matching a whole table costs the worker roughly what one matching ``max_rows``
costs. How much is saved depends on the driver: with a server-side cursor the
remaining rows are never sent, while a client-buffering driver (psycopg2's default
cursor, MySQLdb) has already received them and only the per-row conversion is skipped.
Hooks whose cursor is not DBAPI 2.0 (``ExasolHook`` passes a pyexasol statement) fall
back to a full fetch, and ``DataFusionToolset`` materializes the full result in the
engine before the toolset sees it; in both the payload is bounded but the transfer is
not.

**A byte budget bounds the payload.** ``max_rows`` caps rows, which says nothing about
size -- one row of a 3000-column table is larger than a thousand rows of a narrow one.
``max_result_bytes`` is what actually bounds context. Rows are returned as a contiguous
prefix: the result stops at the first row that does not fit the remaining budget rather
than skipping it and packing later ones, so a single wide row early in the result ends
it. The result says which limit it hit:

.. code-block:: json

    {"columns": ["..."], "rows": ["..."], "row_count": 3,
     "truncated": true, "truncated_by": "max_result_bytes"}

``truncated_by`` is ``max_rows`` or ``max_result_bytes``. When not even one row fits,
or the column names alone exceed the budget, the result carries a ``hint`` telling the
agent to narrow its projection -- the only move that helps. ``total_rows`` is present
when the driver reports a row count for the query; several (SQLite, some warehouse
drivers) do not, and it is then omitted rather than guessed.

The default budget is deliberately generous: the columnar shape alone shrinks a wide
result several-fold, so results that fit before still fit. Lower ``max_result_bytes``
when an agent makes many queries in one run, since every result is re-paid on every
later request.

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
     - Executes a SQL query and returns bounded, columnar JSON (see
       :ref:`bounded-query-results`)

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
- ``max_result_bytes``: Budget for the serialized ``query`` result. Default 64 KiB.
  See :ref:`bounded-query-results`.

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
- ``exclude_resources``: Optional glob patterns to exclude from resource
  discovery, added on top of the built-in defaults (``__pycache__``, ``*.pyc``,
  ``*.pyo``, ``.DS_Store``, ``.git``). A skill exposes every readable text file
  it contains as a resource; these patterns keep matched files out of the
  resource list and the ``read_skill_resource`` tool (e.g.
  ``["*.env", "secrets/*"]``). Each pattern matches the full skill-relative path
  or any single path component. This hides files from resource discovery only --
  it does not stop a skill's ``run_skill_script`` from reading them off disk, so
  pair it with ``exclude_tools={"run_skill_script"}`` when the files are
  genuinely sensitive.

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


``SandboxToolset``
------------------

:class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` gives the
agent a shell and a filesystem inside a disposable sandbox, provisioned by a
:class:`~airflow.providers.common.ai.sandbox.SandboxBackend` and running off the
Airflow worker process. It exposes four tools:

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Tool
     - What it does
   * - ``run_command``
     - Runs a shell command. Pipes, redirection, ``&&`` and globs work. A
       non-zero exit is reported as output, not raised, so the model reads
       ``stderr`` and corrects itself.
   * - ``read_file``
     - Reads a text file, head-first, and reports the next ``offset`` so the
       model can page through a long file.
   * - ``write_file``
     - Writes text to a file, creating parent directories.
   * - ``list_directory``
     - Lists a directory. Directories are shown with a trailing ``/``.

These are the same four names and shapes that pydantic-ai's own sandbox
capabilities use, so a model that has seen one already knows this one, and a
vendor that has written an adapter for one is close to having written this one.

.. _sandbox-boundaries:

Which boundary this is
^^^^^^^^^^^^^^^^^^^^^^

"Sandboxed" means different things at different layers, and picking the wrong
layer is the most common way to end up with less protection than you think.
Four boundaries exist, from smallest to largest:

.. list-table::
   :widths: 22 33 45
   :header-rows: 1

   * - Boundary
     - What moves inside it
     - What that protects you from
   * - **A tool call**
     - What ``run_command`` and the file tools do. **This toolset.**
     - Model-written code damaging the worker host, reading its files, or
       reaching the network from it.
   * - **The glue between tools**
     - Generated orchestration code, via :ref:`code mode <code-mode>` and the
       Monty interpreter.
     - Generated code touching anything other than the tools you registered.
       It still runs in the worker process.
   * - **The agent process**
     - The whole agent loop, its LLM credentials and its message history.
     - The agent's own credentials leaking, and any *other* toolset on the same
       agent. Not available today.
   * - **The whole task**
     - The complete Airflow task, supervisor included, as
       ``KubernetesExecutor`` does.
     - Everything, including Airflow's own worker context and connections.

``SandboxToolset`` is the first row. Being precise about what that means:

- Airflow does not put its context, connections, variables, or worker
  environment into the sandbox. Only what you pass through
  :class:`~airflow.providers.common.ai.sandbox.SandboxSpec` goes in.
- **It does not contain the agent.** The agent loop, the model calls, and every
  other toolset on the same agent still run in the worker process with the
  worker's credentials. An agent that has ``SandboxToolset`` *and* a toolset
  that can reach connections has a contained code tool sitting beside a
  credential path that is not contained. The sandbox does not change what those other
  tools can do.
- Boundary size is not a security level on its own. The image, the credentials
  you inject, the network policy, and the resource limits decide the actual
  isolation. Choose the smallest boundary that fits, then configure the runtime.

If you need the whole task isolated, that is
``KubernetesPodOperator`` or ``KubernetesExecutor`` today, not this.

When to reach for it
^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 45 55
   :header-rows: 1

   * - You want
     - Use
   * - The agent to analyze data, install a package, run a script it wrote, or
       produce a file, without any of that touching the worker
     - ``SandboxToolset``
   * - Fewer model round-trips when chaining tools you already trust
     - :ref:`code mode <code-mode>`. It is not a sandbox for your data; it
       restricts generated glue code to the tools you registered
   * - The agent to query a database with guardrails
     - :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`, which
       enforces table allowlists. A sandbox does not help here
   * - A whole task's worth of untrusted work isolated, with no agent involved
     - ``KubernetesPodOperator``
   * - Airflow's own credentials kept away from the agent
     - Not this. Scope the connections the task can see, and give the agent only
       the toolsets it needs

Both ``code_mode=True`` and ``SandboxToolset`` can be enabled together.
``run_command`` deliberately stays a normal tool in that setup rather than being
folded into ``run_code``, so the model writes Monty code that calls the sandbox,
never a shell script quoted inside a Python string. The three file tools *are*
folded in, where they are more useful as callables.

Lifecycle
^^^^^^^^^

The sandbox is created lazily on the first tool call, shared by every call in
that agent run, and destroyed when the run ends. A run that never calls a tool
never provisions one, and concurrent runs never share a sandbox.

Files written by one call are visible to later calls in the same run. Each
``run_command`` is a fresh shell, so shell variables and background jobs do not
survive between calls; write state to a file if you need it later.

If a command outlives its budget and the backend cannot confirm it stopped, the
backend destroys the sandbox and the tool result says so. The next call gets a
fresh sandbox, and files from earlier calls are gone.

A recoverable failure becomes a bounded retry the model can react to. Only a
terminal failure -- credentials rejected, daemon unreachable, sandbox gone --
fails the task, so Airflow's own retry handles it rather than the model burning
its retry budget.

Controlling what the sandbox gets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`~airflow.providers.common.ai.sandbox.SandboxSpec` says what a sandbox is
provisioned with. The default denies outbound network access and injects no
environment.

.. code-block:: python

    from airflow.providers.common.ai.sandbox import SandboxSpec, SbxSandboxBackend
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(
        SbxSandboxBackend(host_network_policy="allow-all"),
        spec=SandboxSpec(
            env={"HF_TOKEN": "..."},  # only what the generated code legitimately needs
            block_network=False,  # this sandbox may reach the internet
        ),
    )

Anything in ``env`` is readable by model-generated code, so scope it to that one
sandbox's job rather than passing the worker's environment through.

A backend that cannot enforce a field it was given **raises instead of ignoring
it**, so a spec never gives you a false sense of a restriction being in force.
The ``sbx`` backend applies ``allow_egress_to`` as a per-sandbox policy rule,
but only on top of a ``deny-all`` host policy, since a local rule can narrow
egress and never widen it. Ask for an allowlist against an open host policy and
it refuses rather than granting nothing quietly.

Using more than one sandbox
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Tool names must be unique within an agent, so two ``SandboxToolset`` instances
need ``tool_prefix``:

.. code-block:: python

    toolsets = [
        SandboxToolset(
            SbxSandboxBackend(image="python:3.12-slim", host_network_policy="deny-all"),
            tool_prefix="py",
        ),
        SandboxToolset(
            SbxSandboxBackend(image="node:22-slim", host_network_policy="deny-all"),
            tool_prefix="node",
        ),
    ]

That yields ``py_run_command``, ``node_run_command`` and so on. Without a
prefix on at least one of them, the run fails at startup with a duplicate tool
name. Give the model instructions on which one to use for what, or it will guess.

sbx backend (Docker Sandboxes)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`~airflow.providers.common.ai.sandbox.SbxSandboxBackend` runs each sandbox
in a `Docker Sandboxes <https://docs.docker.com/ai/sandboxes/>`__ microVM by
driving the ``sbx`` CLI. Each sandbox is a real microVM with its own kernel, so
agent code is isolated by a hardware boundary rather than a shared kernel.

.. warning::

   **Use this backend for local development, not production.** Docker Sandboxes
   is built for running coding agents against a checkout on your own machine:
   ``sbx create`` takes an agent name (``claude``, ``codex``, ``shell`` and so
   on) and bind-mounts host paths into the sandbox, and its stock network
   profile is described as "typical development traffic ... AI services and
   package registries". Driving it from an Airflow worker is off-label use.

   Concretely, a production worker would need all of: the ``sbx`` binary on the
   host, an authenticated Docker account (``sbx login``), a one-time
   ``sbx policy init``, and on Linux, KVM or nested virtualization. A worker in
   an unprivileged container -- the normal Kubernetes deployment -- generally
   cannot satisfy the last one at all.

   Treat it as the backend you develop and test a sandboxed agent against, then
   run something else in production. A hosted backend plugs in through
   :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`, but none ships
   with the provider yet.

   **Orphans are not reclaimed automatically.** There is no server-side TTL. If
   the worker is killed outright, the microVM and its workspace directory
   survive. Sandboxes are named ``airflow-sandbox-*`` so an operator can find and
   remove them; budget for that sweep.

Installing the CLI is a Deployment Manager prerequisite
(``brew install docker/tap/sbx`` or ``winget install Docker.sbx``); the backend
needs no Python dependency. The template image must provide GNU coreutils
``timeout``, ``base64``, ``stat`` and ``ls``, which any Debian or Ubuntu based
image, including ``python:*-slim``, does.

.. code-block:: python

    from airflow.providers.common.ai.operators.agent import AgentOperator
    from airflow.providers.common.ai.sandbox import SbxSandboxBackend
    from airflow.providers.common.ai.toolsets import SandboxToolset

    AgentOperator(
        task_id="sandboxed_analyst",
        prompt="Estimate pi with a Monte Carlo simulation of one million points.",
        llm_conn_id="pydanticai_default",
        toolsets=[SandboxToolset(SbxSandboxBackend(host_network_policy="deny-all"))],
    )

Constructor parameters:

- ``image``: Container image for the sandbox. Default ``"python:3.12-slim"``.
- ``memory``: Memory limit in binary units. ``sbx`` enforces a 1 GiB minimum.
  Default ``"2g"``.
- ``cpus``: CPUs to allocate. ``None`` (default) uses the ``sbx`` default, which
  is every host CPU.
- ``sbx_path``: Path to the ``sbx`` binary. Default ``"sbx"``.
- ``create_timeout``: Seconds allowed for provisioning; a first-run microVM boot
  plus an image pull can be slow. Default ``600``.
- ``host_network_policy``: What ``sbx policy`` is set to on this host.
  ``"unknown"`` (default) makes ``create`` refuse any spec asking for a network
  guarantee this backend cannot make. Set ``"deny-all"`` after running
  ``sbx policy init deny-all``, or ``"allow-all"`` to state that egress is open.

Bringing your own backend
^^^^^^^^^^^^^^^^^^^^^^^^^

Any vendor that can create a sandbox, run a command in it and destroy it can
plug in. Subclass
:class:`~airflow.providers.common.ai.sandbox.SandboxBackend` in your own package
and pass an instance to ``SandboxToolset``.

**Three methods are required**: ``create``, ``run_command`` and ``destroy``. The
three file operations ship as defaults implemented over ``run_command``, because
reading, writing and listing a file are all expressible as shell commands.
Override them only when the vendor has a native file API, which avoids base64
expansion, the command-line length ceiling, and the guest needing coreutils:

.. code-block:: python

    from airflow.providers.common.ai.sandbox import (
        SandboxBackend,
        SandboxExecResult,
        SandboxSpec,
    )


    class AcmeSandboxBackend(SandboxBackend):
        name = "acme"

        def create(self, *, spec: SandboxSpec | None = None) -> str:
            return acme_sdk.create_sandbox().id

        def run_command(self, sandbox, command, *, timeout, max_output_bytes):
            r = acme_sdk.exec(sandbox, command, timeout=timeout)
            return SandboxExecResult(exit_code=r.exit_code, stdout=r.stdout, stderr=r.stderr)

        def destroy(self, sandbox) -> None:
            acme_sdk.delete_sandbox(sandbox)

        # Optional: inherited from SandboxBackend unless the vendor has
        # something better than shelling out.
        def read_file(self, sandbox, path, *, max_bytes) -> bytes:
            return acme_sdk.download(sandbox, path, limit=max_bytes)

Four rules for an implementation:

- Constructors run at Dag-parse time, so resolve credentials lazily, on first use.
- ``destroy`` must be idempotent; destroying an already-gone sandbox is not an error.
- Raise ``SandboxTerminalError`` when retrying cannot help and ``SandboxError``
  when it might. The first fails the task for Airflow to retry; the second
  becomes a bounded prompt back to the model.
- If you cannot enforce something the ``SandboxSpec`` asks for, **raise**. Never
  provision a weaker sandbox than the Dag author asked for.

Parameters
^^^^^^^^^^

- ``backend``: The backend that provisions and drives the sandbox.
- ``spec``: What to provision it with. Defaults to no environment and no egress.
- ``default_command_timeout``: Seconds for a ``run_command`` the model did not
  put a timeout on. Default ``60``.
- ``max_command_timeout``: Hard ceiling for any single command, including a
  model-supplied ``timeout_seconds``. Default ``300``.
- ``max_output_lines`` / ``max_output_bytes``: Caps per output stream and per
  file read, whichever is hit first. Defaults ``2000`` and 50 KiB. Command
  output keeps the **tail**, where errors and the exit status live; file reads
  keep the head and report a continuation offset.
- ``max_read_bytes``: Largest file ``read_file`` will transfer. Default 5 MiB;
  larger files are refused with a hint to slice them in the shell.
- ``tool_prefix``: Prefix for the four tool names. Needed when one agent has
  more than one ``SandboxToolset``.


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
behavior (connection resolution, ``SQLToolset``'s SQL validation, and
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
       carry secrets, a backend you add can expose its own identity, the ``sbx``
       backend leaks orphaned microVMs
       if the worker is killed, and its CPU allocation defaults to every host CPU.
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

.. _managed-agent-toolsets:

Managed Agent Toolsets
----------------------

Cloud vendors now run agents on your behalf — Snowflake Cortex Agents, Amazon
Bedrock AgentCore runtimes, Azure AI Foundry hosted agents, Vertex AI Agent
Engine. Their reasoning loops execute on the vendor's infrastructure, so they
are not something ``AgentOperator`` runs; they are something an Airflow task
*consults*.

:class:`~airflow.providers.common.ai.toolsets.managed_agent.BaseManagedAgentToolset`
is the contract for exposing one of those as a tool. Each provider package
ships its own subclass, so credentials keep flowing through that provider's
existing hook and no new connection types are needed.

A subclass implements two members:

``agent_ref``
    Normalised identity of the remote agent — ``platform`` and ``name`` — logged
    on every call so a run can be audited for which agents it consulted.

``invoke(prompt)``
    Send the prompt, return the agent's answer. Return the *answer*, not the
    transport envelope.

Tool naming, argument validation, result serialisation, and logging are handled
by the base class, so every provider's implementation presents the same surface
to the calling model.

``tool_name`` is the required identifier — it is what the model emits when it
calls the tool, and the Dag author chooses it. ``description`` is optional and
falls back to the tool name rendered as prose, the same way ``HookToolset``
derives one from a method name when there is no docstring.

.. note::

    Writing a description is still worth the line. It is what tells the model to
    consult the agent rather than answer from its own knowledge, and it is the
    only place to record a scope limit the name cannot carry — "cannot see
    revenue figures". Because the argument schema is always a bare prompt, the
    name and the description are the whole of what the model knows about the
    agent.

Toolset or operator?
""""""""""""""""""""""

Most managed-agent platforms do not offer a plain one-request-one-answer API. Some
require polling a job; others require creating a session and tearing it down around
each exchange. A toolset can do either, but only by blocking inside
``invoke()`` — it cannot defer to the Triggerer, and it has no post-task hook to
clean up with if the worker dies mid-call.

That draws a boundary worth respecting:

.. list-table::
    :header-rows: 1
    :widths: 45 55

    * - Shape
      - Surface to use
    * - A short consultation *inside* an agent's reasoning, where failing the task
        would discard the calling agent's accumulated context
      - A managed agent toolset
    * - Long-running submitted work as a pipeline step in its own right
      - That provider's own operator, with deferral or
        :class:`~airflow.sdk.bases.resumablejobmixin.ResumableJobMixin`


``ResumableJobMixin`` exists for exactly the second case: it persists the external
job ID to the task state store before polling, so a worker crash reconnects to the
running job instead of submitting a duplicate. A toolset cannot offer that, because
the retry boundary is the task, not the tool call — on retry the agent loop restarts
and re-issues the call. Durable execution covers the *completed* call (see
``replayable`` below); it does not cover a call that was still in flight.

Error handling
""""""""""""""

Failures sort into three buckets, and conflating them is the most common way an
implementation goes wrong:

.. list-table::
    :header-rows: 1
    :widths: 22 30 48

    * - Raise
      - When
      - Who recovers
    * - ``ModelRetry``
      - The agent rejected the request in a way rephrasing could fix.
      - The calling model, bounded by its ``usage_limits``.
    * - ``ManagedAgentInvocationError``
      - Terminal: bad credentials, missing agent, revoked quota.
      - Nobody — the task fails fast instead of burning retries.
    * - *let it propagate*
      - Transient: 429, 5xx, connection reset, read timeout.
      - Airflow's task-level retry. A rephrase does nothing for a 503.

Durable execution
"""""""""""""""""

``replayable`` is ``False`` by default. A managed agent may act on systems
Airflow cannot observe, so replaying a cached answer on retry could skip a side
effect. Implementations whose agent is read-only should set it to ``True`` to
avoid paying for the same invocation twice.

Deferral
""""""""

A toolset call runs in the worker and cannot defer to the Triggerer — it blocks
for the duration of the call. See `Toolset or operator?`_ above for when that is
acceptable and when the provider's own deferrable operator is the right surface
instead.

Failover between interchangeable agents
"""""""""""""""""""""""""""""""""""

:class:`~airflow.providers.common.ai.toolsets.managed_agent.FailoverManagedAgentToolset`
composes several managed agents into one tool, trying them in order until one
answers. It is itself a ``BaseManagedAgentToolset``, so the calling model sees a
single tool and has no say in which provider serves the request — the policy
stays deterministic Python rather than a prompt instruction a model may ignore.
Groups nest.

.. code-block:: python

    from airflow.providers.common.ai.toolsets import FailoverManagedAgentToolset

    resilient = FailoverManagedAgentToolset(
        tool_name="ask_claims_agent",
        description="Reviews an insurance claim and returns a coverage determination.",
        members=[bedrock_claims_agent, foundry_claims_agent],  # same image, two clouds
    )

Members must satisfy two preconditions the class cannot check.

**Substitutability.** The same agent deployed twice, not two specialists with
different data. Two containerised agents built from one image qualify; agents
bound to one platform's own objects — a Cortex Agent over Snowflake semantic
models — do not, because there is nothing equivalent to fail over *to*.

**Statelessness per invocation.** Server-side conversation state is the norm
across managed-agent platforms, not the exception — optional on some (Cortex
``thread_id``), mandatory on others where a session is created and torn down
around each exchange. Each member is invoked with a bare prompt and no thread
reference, so a failover silently starts a fresh conversation on the standby:
correct for a one-shot consultation, wrong for a multi-turn one. Treat one-shot
as a restriction a group is deliberately held to, not a safe default.

The three error buckets do real work here:

- ``ManagedAgentInvocationError`` and transient failures move to the next member.
- ``ModelRetry`` is re-raised immediately and never triggers failover. A prompt
  the primary could not parse will not parse on the standby either, so failing
  over would spend the standby's budget reproducing the same error.
- The last member's exception propagates unchanged, so a total outage still fails
  the task rather than returning something misleading.

``failover_on`` defaults to ``Exception`` because ``common.ai`` cannot enumerate
the cloud SDKs' exception trees — ``requests``, ``botocore`` and the Azure SDK
share no common base. Narrow it when the members' exception types are known.

``replayable`` on a group is ``True`` only when every member is, because the
durable cache cannot know which member produced the answer it holds.

.. note::

    For a **standalone** agent call, prefer plain Airflow task-level failover:
    two tasks, the second with ``trigger_rule=TriggerRule.ALL_FAILED``. That keeps
    which provider served the request visible in the grid at no code cost, and
    makes failover rate a task metric. This class is for the case a task boundary
    cannot express — a managed agent consulted as a tool *inside* a longer agent
    run, where failing the task would discard the calling agent's accumulated
    context and re-run every earlier tool call.

Two counters make failover visible, because a failover is a *success-shaped*
event — without them a primary that has been down for a week looks identical to a
healthy one:

.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Metric
      - Tags
    * - ``managed_agent.failover``
      - ``from_platform``, ``to_platform`` — one per failover transition
    * - ``managed_agent.served``
      - ``platform``, ``role`` (``primary`` / ``standby``) — one per answer

The standby-served fraction is a ratio over ``managed_agent.served`` alone, so
"are we quietly running on the standby?" is a dashboard question rather than a log
grep. Both are tagged by platform rather than agent name to keep cardinality
bounded.

One limitation remains: which member served a *particular* answer is in the task
log but not in XCom. ``agent_ref`` on a group describes the group, not the
responder, because the responder is not known until after the call. The counters
cover the operational question; per-answer provenance for an audit trail would
need ``AgentOperator`` to collect per-toolset metadata.
