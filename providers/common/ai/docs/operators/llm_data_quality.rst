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

.. _howto/operator:llm_data_quality:

``LLMDataQualityOperator``
==========================

Use :class:`~airflow.providers.common.ai.operators.llm_data_quality.LLMDataQualityOperator`
to run natural-language data-quality checks against a live data source using an LLM agent.

The agent:

1. Calls ``list_checks`` to read the user's quality expectations.
2. Calls ``list_validators`` to discover available validators.
3. Uses schema-discovery tools (``list_tables``, ``get_schema``) to explore the data source.
4. Writes and executes SQL queries, applies validators, and produces a
   :class:`~airflow.providers.common.ai.utils.dataquality.models.DQReport`.
5. Fails the task when any check does not pass.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Architecture — Toolsets
-----------------------

The operator is composed of two toolsets that are passed together in ``toolsets``:

- **Data-source toolset** — gives the agent access to the data (SQL queries, schema discovery).
  Examples: :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` for relational
  databases, :class:`~airflow.providers.common.ai.toolsets.datafusion.DataFusionToolset`
  for object-storage formats (Parquet, CSV, Avro on S3, GCS, etc.).

- **DQ toolset** (:class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset`) —
  adds the ``list_validators`` and ``apply_validator`` tools so the agent can evaluate metrics
  against registered validator thresholds.

When ``toolsets`` is omitted, the operator auto-creates
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` and
:class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset`
from ``db_conn_id`` and ``table_names``.

PostgreSQL / Relational Databases
----------------------------------

Pass ``SQLToolset`` (schema-discovery + SQL execution) alongside ``SQLDQToolset``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_postgres_basic]
    :end-before: [END howto_operator_llm_dq_postgres_basic]

Object Storage (S3 / Parquet)
------------------------------

Use :class:`~airflow.providers.common.ai.toolsets.datafusion.DataFusionToolset` to register
S3 (or other object-store) Parquet/CSV data as a queryable DataFusion table, then pair it with
``SQLDQToolset``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_object_storage]
    :end-before: [END howto_operator_llm_dq_object_storage]

.. note::
    The ``region`` key in the ``aws_default`` connection **Extra** field must match the
    bucket's AWS region to avoid redirect errors, e.g. ``{"region": "eu-central-1"}``.

Explicit Toolsets
-----------------

Passing ``toolsets`` directly gives full control over which tables the agent can access.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_explicit_toolsets]
    :end-before: [END howto_operator_llm_dq_explicit_toolsets]

Checks
------

Each entry in ``checks`` is a
:class:`~airflow.providers.common.ai.utils.dataquality.models.DQCheckInput`
(or a plain ``dict`` with ``name``, ``description``, and optional ``validator`` keys).

.. code-block:: python

    from airflow.providers.common.ai.utils.dataquality import DQCheckInput, null_pct_check

    checks = [
        DQCheckInput(
            name="null_email",
            description="Check the percentage of rows where email is NULL",
            validator=null_pct_check(max_pct=0.05),
        ),
        DQCheckInput(
            name="row_count",
            description="Ensure the table has at least 1000 rows",
        ),
    ]

Check names must be unique within a single operator call.

Validators
----------

Built-in validator factories are exported from
:mod:`~airflow.providers.common.ai.utils.dataquality`:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Factory
     - Description
   * - ``null_pct_check(max_pct=...)``
     - Passes when the fraction of NULL values is at or below ``max_pct``.
   * - ``row_count_check(min_count=...)``
     - Passes when the row count is at least ``min_count``.
   * - ``duplicate_pct_check(max_pct=...)``
     - Passes when the fraction of duplicate values is at or below ``max_pct``.
   * - ``between_check(min_val=..., max_val=...)``
     - Passes when the metric value falls within ``[min_val, max_val]``.
   * - ``exact_check(expected=...)``
     - Passes when the metric value equals ``expected`` exactly.

Checks without a validator are measured and included in the report.
If no validator is applied, the check is treated as passed.

Custom Validators
-----------------

Use :func:`~airflow.providers.common.ai.utils.dataquality.register_validator`
to register validator factories that the LLM can discover via ``list_validators``
and select dynamically:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_email_format_validator]
    :end-before: [END howto_operator_llm_dq_email_format_validator]

Row-Level Checks
----------------

Row-level validators receive a list of values (one per row) instead of a single
aggregate metric.  The agent issues a plain ``SELECT <column> FROM <table>`` query
(no aggregation) and passes the column values to ``apply_validator``.

The validator returns a
:class:`~airflow.providers.common.ai.utils.dataquality.models.RowLevelResult` with
these fields:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Description
   * - ``total``
     - Number of evaluated rows.
   * - ``invalid``
     - Number of rows that failed the validator predicate.
   * - ``invalid_pct``
     - ``invalid / total`` (``0.0`` when ``total`` is zero).
   * - ``sample_violations``
     - Sample of failing values (string form).
   * - ``sample_size``
     - Length of ``sample_violations``.

Register a row-level validator with ``row_level=True``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_email_format_validator]
    :end-before: [END howto_operator_llm_dq_email_format_validator]

Schema Context
--------------

Pass ``schema_context`` to inject a manual schema description into the system prompt.
Useful when the data source cannot be introspected at runtime, or to restrict
the agent to a specific set of tables:

.. code-block:: python

    LLMDataQualityOperator(
        task_id="validate_with_schema",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        schema_context=(
            "Table: orders\n"
            "Columns: id INT, customer_id INT, amount DECIMAL, created_at TIMESTAMP\n\n"
            "Table: customers\n"
            "Columns: id INT, email TEXT, country TEXT"
        ),
        checks=[
            DQCheckInput(
                name="null_amount",
                description="Check the percentage of orders with a NULL amount",
                validator=null_pct_check(max_pct=0.0),
            ),
        ],
    )

Human-in-the-Loop Approval
---------------------------

Set ``require_approval=True`` to gate execution on a human reviewer.
The task runs in two phases:

1. **Phase 1** — the LLM discovers schema, writes SQL queries, and selects validators
   but does *not* execute any SQL or call ``apply_validator``.  The resulting plan
   (SQL statements + validator choices) is surfaced in the Airflow UI.
2. **Phase 2** (after approval) — the approved SQL queries run, validators are applied
   in pure Python, and the final :class:`~...DQReport` is produced.  No LLM calls.

Rejecting the plan raises
:class:`~airflow.providers.standard.exceptions.HITLRejectException`.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_require_approval]
    :end-before: [END howto_operator_llm_dq_require_approval]

See :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` for shared HITL parameters
(``approval_timeout``, ``allow_modifications``).

Durable Execution
-----------------

Set ``durable=True`` to enable cross-run caching of LLM and tool-call results.
When a task is retried or restarted, the operator replays cached model responses and
tool calls instead of re-running them, saving tokens and wall-clock time.

.. code-block:: python

    LLMDataQualityOperator(
        task_id="validate_products",
        llm_conn_id="pydanticai_default",
        toolsets=[
            SQLToolset(db_conn_id="postgres_default", allowed_tables=["products"]),
            SQLDQToolset(),
        ],
        durable=True,
        checks=[...],
    )

TaskFlow Decorator
------------------

Use ``@task.llm_dq`` when checks are produced dynamically by a Python callable.
The function body returns the checks list; the decorator handles the LLM agent run,
SQL execution, and validator application automatically.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_task_decorator]
    :end-before: [END howto_operator_llm_dq_task_decorator]

Output and XCom
---------------

The operator returns a ``dict`` representation of
:class:`~airflow.providers.common.ai.utils.dataquality.models.DQReport` as its XCom value
(``output_mode="execute"``).

Additionally, the full report is pushed to XCom under the key ``dq_report`` on the
task instance:

.. code-block:: python

    report = context["ti"].xcom_pull(task_ids="validate_orders", key="dq_report")

For config-generation backends (``output_mode="generate"``), the operator returns
the generated configuration string instead of a report.

Logging
-------

After each LLM call, the operator logs a summary with model name, token usage,
and request count at INFO level:

.. code-block:: text

    [INFO] Agent run summary — model: gpt-4o, requests: 6, tool_calls: 14,
           input_tokens: 3201, output_tokens: 412, total_tokens: 3613

Parameters Reference
--------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Type
     - Description
   * - ``checks``
     - ``list``
     - List of :class:`~...DQCheckInput` objects or plain dicts.  Names must be unique.
   * - ``toolsets``
     - ``list | None``
     - Pydantic-AI toolsets.  Must include a data-source toolset and a
       :class:`~...BaseDQToolset`.  Auto-created from ``db_conn_id`` when ``None``.
   * - ``db_conn_id``
     - ``str | None``
     - Connection ID for auto-creating ``SQLToolset``.  Ignored when ``toolsets`` is set.
   * - ``table_names``
     - ``list[str] | None``
     - Tables exposed to the auto-created ``SQLToolset``.  Ignored when ``toolsets`` is set.
   * - ``schema_context``
     - ``str | None``
     - Manual schema description injected into the system prompt.
   * - ``durable``
     - ``bool``
     - Enable cross-run LLM and tool-call caching.  Default ``False``.
   * - ``require_approval``
     - ``bool``
     - Enable two-phase HITL approval before SQL execution.  Default ``False``.
   * - ``llm_conn_id``
     - ``str``
     - Pydantic-AI connection ID (inherited from
       :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`).


Each entry in ``checks`` is a
:class:`~airflow.providers.common.ai.utils.dataquality.models.DQCheckInput`.
The operator:

1. Exposes checks and validators as tools to the model.
2. Lets the model discover schema and generate SQL via your data-source toolset.
3. Applies validators and produces a
   :class:`~airflow.providers.common.ai.utils.dataquality.models.DQReport`.
4. Fails the task when any check fails.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

If you pass ``db_conn_id`` (without explicit toolsets), the operator auto-creates:

- :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`
- :class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset`

.. code-block:: python

    from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
    from airflow.providers.common.ai.utils.dataquality import DQCheckInput, null_pct_check, row_count_check

    LLMDataQualityOperator(
        task_id="validate_orders",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        table_names=["orders", "customers"],
        checks=[
            DQCheckInput(
                name="row_count",
                description="The orders table must contain at least 1000 rows.",
                validator=row_count_check(min_count=1000),
            ),
            DQCheckInput(
                name="email_nulls",
                description="No more than 5% of customer emails should be null.",
                validator=null_pct_check(max_pct=0.05),
            ),
        ],
    )

Explicit Toolsets
-----------------

Use explicit toolsets for fine-grained control over accessible tables and validator behavior.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_explicit_toolsets]
    :end-before: [END howto_operator_llm_dq_explicit_toolsets]

Validators
----------

Built-in validator factories are exported from
:mod:`~airflow.providers.common.ai.utils.dataquality`:

- ``null_pct_check(max_pct=...)``
- ``row_count_check(min_count=...)``
- ``duplicate_pct_check(max_pct=...)``
- ``between_check(min_val=..., max_val=...)``
- ``exact_check(expected=...)``

Each validator receives the metric value and returns ``True`` or ``False``.

Checks without a validator are still measured and included in the report.
If no validator is applied, the check is treated as passed.

Custom Validators
-----------------

Use :func:`~airflow.providers.common.ai.utils.dataquality.register_validator`
to register validator factories that can be selected by the LLM.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_email_format_validator]
    :end-before: [END howto_operator_llm_dq_email_format_validator]

Manual Schema Context
---------------------

If runtime introspection is limited, pass schema text explicitly via ``schema_context``.

.. code-block:: python

    LLMDataQualityOperator(
        task_id="validate_with_manual_schema",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        schema_context=(
            "Table: orders(id INT, customer_id INT, amount DECIMAL, created_at TIMESTAMP)\n"
            "Table: customers(id INT, email TEXT, country TEXT)"
        ),
        checks=[
            DQCheckInput(
                name="null_amount",
                description="Check the percentage of orders with a NULL amount",
                validator=null_pct_check(max_pct=0.0),
            ),
        ],
    )

Row-Level Checks
----------------

Row-level checks use a plain ``SELECT <column> FROM <table>`` query (no aggregation)
and validate one value per row.

For row-level validators, SQLDQToolset returns a structured value in the result:
:class:`~airflow.providers.common.ai.utils.dataquality.models.RowLevelResult`.

``RowLevelResult`` fields
~~~~~~~~~~~~~~~~~~~~~~~~~

- ``total``: number of evaluated rows
- ``invalid``: number of rows that failed the validator predicate
- ``invalid_pct``: ``invalid / total`` (or ``0.0`` when total is zero)
- ``sample_violations``: sampled failing values (string form)
- ``sample_size``: ``len(sample_violations)``

Pass/fail uses ``validator._max_invalid_pct`` when present (default ``0.0``).
In practice, this is typically set by validator factory kwargs such as
``max_invalid_pct``.

Human-in-the-Loop Approval
--------------------------

Set ``require_approval=True`` for two-phase execution:

1. Phase 1: the LLM produces a plan (SQL + validator selection) without executing SQL.
2. A reviewer approves or rejects the plan in the UI.
3. Phase 2: approved SQL runs and validators execute in Python.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_require_approval]
    :end-before: [END howto_operator_llm_dq_require_approval]

See :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` for shared HITL parameters
such as ``approval_timeout`` and ``allow_modifications``.

TaskFlow Decorator
------------------

Use ``@task.llm_dq`` when checks are produced dynamically by a Python callable.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_task_decorator]
    :end-before: [END howto_operator_llm_dq_task_decorator]

End-to-End Example
------------------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_postgres_basic]
    :end-before: [END howto_operator_llm_dq_postgres_basic]
