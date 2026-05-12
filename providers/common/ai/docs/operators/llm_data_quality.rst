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
to run natural-language data quality checks with an LLM agent.

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
