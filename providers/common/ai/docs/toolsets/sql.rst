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

SQL databases: ``SQLToolset``
=============================

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
-------------------------

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
----------

- ``db_conn_id``: Airflow connection ID for the database.
- ``allowed_tables``: Restrict the agent to a fixed set of tables. Omit the
  argument (the default) to expose all tables in ``schema``. No value means
  allow-all: ``None`` and an empty list both raise ``ValueError``, so an allow-list
  built at runtime that resolves to nothing fails at import instead of silently
  exposing every table. Entries may be schema-qualified
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
---------------------

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

When to choose it
-----------------

**Choose it when** the question is a query and the data is in a DBAPI database.
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` gives the agent
four tools — list tables, get schema, query, check query. Set ``allowed_tables``
and that allow-list is enforced by parsing the SQL rather than by matching
strings; see :ref:`allowed-tables-enforcement` for how the walk handles CTEs,
subqueries and joins.

**What it cannot do**

- ``allowed_tables`` is an application-level guardrail, not a replacement for
  database permissions. Its own docstring says so, and names the residual gap:
  an engine or query the parser reads differently. Point ``db_conn_id`` at a
  least-privilege role whose grants match the allow-list.
- It cannot bound the fetch for every driver. Hooks that hand their handler
  something other than a DBAPI cursor — ``ExasolHook`` and its pyexasol
  statement, for instance — fall back to a full fetch. The payload handed to the
  model is still bounded; the transfer is not. See :ref:`bounded-query-results`.
- Its parser-level closure is opt-in, not the default. The table walk returns
  immediately while ``allowed_tables`` is unset, so out of the box the agent
  reaches every table the connection can see. Only omitting the argument grants
  that: an explicit ``None`` or empty list is rejected at construction.
  ``DESCRIBE`` and ``SHOW`` both pass, on dialects that parse them, while
  ``allowed_tables`` stays unset. Set ``allowed_tables`` and the walk
  turns fail-closed for ``SHOW`` — but not for ``DESCRIBE``: it instead
  becomes an ordinary table reference, allowed only when the table it names
  is on the list. See :ref:`allowed-tables-enforcement` for what else the
  walk rejects once ``allowed_tables`` is active. Statements that modify
  data, ``COPY`` among them, are rejected either way while ``allow_writes``
  is ``False``.
- It does not classify failures. A connection error or a typo in a column
  name reaching ``list_tables``, ``get_schema`` or ``query`` becomes one
  ``ModelRetry``, so the two are treated the same way until the retry budget
  runs out and the task fails for Airflow to retry. Two paths do not raise:
  ``check_query`` catches its own errors and reports them back as a normal
  ``{"valid": false, ...}`` result, and ``get_schema`` returns a normal
  ``{"error": ...}`` result instead of raising when the requested table is
  outside ``allowed_tables`` — other ``get_schema`` failures still raise and
  still become a ``ModelRetry``.

**A real example.** ``example_pydantic_ai_hook.py`` builds an agent around
``SQLToolset`` inside a plain ``@task`` function, with no operator involved:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_hook.py
    :language: python
    :start-after: [START howto_task_with_toolsets]
    :end-before: [END howto_task_with_toolsets]

**Credentials and where it runs.** ``db_conn_id`` is resolved through
``BaseHook.get_connection``, and the connection must supply a ``DbApiHook``.
Queries run from the Airflow worker process against the database. Its tool
calls act as barriers, as they do for the other routes that build their own
tools; see :ref:`toolset-call-barriers`.
