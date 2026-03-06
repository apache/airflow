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
to generate and execute data-quality checks from natural language using an LLM.

Each entry in ``prompts`` describes **one** data-quality expectation.
The LLM groups related checks into optimised SQL queries, executes them against the
target database, and validates each metric against the corresponding entry in
``validators``.  The task fails if any check does not pass, gating downstream tasks
on data quality.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydantic_ai>`

Basic Usage
-----------

Provide a ``prompts`` dict and a target ``db_conn_id``. The operator introspects the
schema automatically when ``table_names`` is provided:

.. code-block:: python

    from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
    from airflow.providers.common.ai.utils.dq_validation import null_pct_check, row_count_check

    LLMDataQualityOperator(
        task_id="validate_orders",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        table_names=["orders", "customers"],
        prompts={
            "row_count": "The orders table must contain at least 1000 rows.",
            "email_nulls": "No more than 5% of customer email addresses should be null.",
        },
        validators={
            "row_count": row_count_check(min_count=1000),
            "email_nulls": null_pct_check(max_pct=0.05),
        },
    )

Validators
----------

The ``validators`` dict maps each check name to a callable that receives the raw
metric value returned by the generated SQL and returns ``True`` (pass) or
``False`` (fail).

Built-in Factories
~~~~~~~~~~~~~~~~~~

:mod:`~airflow.providers.common.ai.utils.dq_validation` ships ready-made factories
for the most common thresholds:

.. list-table::
   :header-rows: 1
   :widths: 30 50 20

   * - Factory
     - Passes when …
     - Example
   * - ``null_pct_check(max_pct=…)``
     - metric ≤ ``max_pct``
     - ``null_pct_check(max_pct=0.05)``
   * - ``row_count_check(min_count=…)``
     - metric ≥ ``min_count``
     - ``row_count_check(min_count=1000)``
   * - ``duplicate_pct_check(max_pct=…)``
     - metric ≤ ``max_pct``
     - ``duplicate_pct_check(max_pct=0.0)``
   * - ``between_check(min_val=…, max_val=…)``
     - ``min_val`` ≤ metric ≤ ``max_val``
     - ``between_check(min_val=0.0, max_val=1.0)``
   * - ``exact_check(expected=…)``
     - metric == ``expected``
     - ``exact_check(expected=0)``

You can also use plain lambdas for one-off conditions::

    validators = {
        "stale_rows": lambda v: int(v) < 1000,
        "revenue_range": lambda v: 0.0 <= float(v) <= 1_000_000.0,
    }

Checks without a validator are always marked as **passed** — metrics are still
collected and included in the report, but no threshold is enforced.

With Schema Introspection
-------------------------

Pass ``db_conn_id`` and ``table_names`` so the operator can introspect the live
database schema. The LLM receives real column names and types, producing more
accurate SQL:

.. code-block:: python

    from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
    from airflow.providers.common.ai.utils.dq_validation import (
        duplicate_pct_check,
        exact_check,
        null_pct_check,
        row_count_check,
    )

    LLMDataQualityOperator(
        task_id="validate_customers",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        table_names=["customers"],
        prompt_version="v1",
        prompts={
            "null_emails": "Check the percentage of rows where email is NULL",
            "duplicate_ids": "Calculate the percentage of duplicate customer IDs",
            "negative_balance": "Count rows where account_balance is negative",
            "min_customers": "Count the total number of customer rows",
        },
        validators={
            "null_emails": null_pct_check(max_pct=0.01),
            "duplicate_ids": duplicate_pct_check(max_pct=0.0),
            "negative_balance": exact_check(expected=0),
            "min_customers": row_count_check(min_count=10_000),
        },
    )

With Manual Schema Context
~~~~~~~~~~~~~~~~~~~~~~~~~~

When you cannot or do not want to connect to the database at plan-generation
time, pass a manual schema description via ``schema_context``:

.. code-block:: python

    LLMDataQualityOperator(
        task_id="validate_with_manual_schema",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        schema_context=(
            "Table: orders(id INT, customer_id INT, amount DECIMAL, created_at TIMESTAMP)\n"
            "Table: customers(id INT, email TEXT, country TEXT)"
        ),
        prompts={
            "null_amount": "Check the percentage of orders with a NULL amount",
        },
        validators={
            "null_amount": null_pct_check(max_pct=0.0),
        },
    )

With Object Storage
-------------------

Use ``datasource_config`` to validate data stored in object storage
(e.g., Amazon S3, Azure Blob Storage) via
`DataFusion <https://datafusion.apache.org/>`_.
The operator uses :class:`~airflow.providers.common.sql.config.DataSourceConfig`
to register the object storage source as a table so the LLM can incorporate it
in the schema context.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_data_quality.py
    :language: python
    :start-after: [START howto_operator_llm_dq_s3_parquet]
    :end-before: [END howto_operator_llm_dq_s3_parquet]

Plan Caching
------------

Generated SQL plans are cached in Airflow
:class:`~airflow.models.variable.Variable` to avoid repeat LLM calls on every
DAG run.

Cache key
~~~~~~~~~

The cache key is derived from a SHA-256 digest of the sorted ``prompts`` dict
combined with ``prompt_version``.  The key format is::

    dq_plan_{version_tag}_{sha256[:16]}

Because it is order-independent, reordering keys in ``prompts`` does **not**
invalidate the cache.

Invalidating the Cache
~~~~~~~~~~~~~~~~~~~~~~

Bump ``prompt_version`` whenever the intent of a prompt changes, even if the
text is the same:

.. code-block:: python

    LLMDataQualityOperator(
        task_id="validate_orders",
        prompt_version="v2",  # was "v1" — forces a new LLM call
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        prompts={"row_count": "The orders table must contain at least 5000 rows."},
        validators={"row_count": row_count_check(min_count=5000)},
    )

Dry Run Mode
------------

Set ``dry_run=True`` to generate and cache the SQL plan without executing it.
The operator returns the serialised plan dict, which you can inspect or route
to a human-approval step with
:class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`:

.. code-block:: python

    from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    preview = LLMDataQualityOperator(
        task_id="preview_dq_plan",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        table_names=["sales"],
        prompts={"row_count": "The sales table must have at least 1000 rows."},
        dry_run=True,
    )

    approve = ApprovalOperator(task_id="approve_plan")

    execute = LLMDataQualityOperator(
        task_id="run_dq_checks",
        llm_conn_id="pydanticai_default",
        db_conn_id="postgres_default",
        table_names=["sales"],
        prompts={"row_count": "The sales table must have at least 1000 rows."},
        validators={"row_count": row_count_check(min_count=1000)},
    )

    preview >> approve >> execute

Because the ``dry_run`` run already cached the plan under the same key, the
execution run skips the LLM entirely and reads from the
:class:`~airflow.models.variable.Variable` cache.

TaskFlow Decorator
------------------

The ``@task.llm_dq`` decorator lets you write a function that returns the
``prompts`` dict. The decorator handles LLM plan generation, plan caching, SQL
execution, and metric validation:

.. code-block:: python

    from airflow.providers.common.ai.utils.dq_validation import (
        null_pct_check,
        row_count_check,
    )
    from airflow.sdk import dag, task


    @dag
    def validate_pipeline():
        @task.llm_dq(
            llm_conn_id="pydanticai_default",
            db_conn_id="postgres_default",
            table_names=["orders", "customers"],
            validators={
                "row_count": row_count_check(min_count=1000),
                "email_nulls": null_pct_check(max_pct=0.05),
            },
        )
        def dq_checks(ds=None):
            return {
                "row_count": f"The orders table must have at least 1000 rows as of {ds}.",
                "email_nulls": "No more than 5% of customer emails should be null.",
            }

        dq_checks()


    validate_pipeline()

The function body can use Airflow context variables, XCom pulls, or any
runtime information to build the prompts dict dynamically.

.. note::
    Validator key validation is deferred to execution time when using the
    ``@task.llm_dq`` decorator, because ``prompts`` is not known until the
    callable runs. Any ``validators`` key that does not appear in the dict
    returned by the callable raises ``ValueError`` at task execution time.

Logging
-------

After executing each SQL group, the operator logs:

- The schema context used (at INFO level).
- Whether the plan was loaded from cache or freshly generated (at INFO level).
- Per-group SQL queries and check names in ``dry_run`` mode (at INFO level).
- The number of checks that passed after execution (at INFO level).
- A failure summary raised as :class:`~airflow.exceptions.AirflowException`
  when any check fails, listing each failing check name and reason.

See :ref:`AgentOperator — Logging <howto/operator:agent>` for details on the
underlying LLM call log format (model name, token usage, request count).
