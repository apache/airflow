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

.. _howto/operator:llm_sql_query:

Natural language to SQL: ``LLMSQLQueryOperator`` and ``@task.llm_sql``
======================================================================

Use :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator` to generate
SQL queries from natural language using an LLM.

The operator generates SQL but does not execute it. The generated query is returned
as XCom and can be passed to ``SQLExecuteQueryOperator`` or used in downstream tasks.

Install the ``sql`` extra, which adds ``apache-airflow-providers-common-sql`` and
`sqlglot <https://github.com/tobymao/sqlglot>`__ (used to validate the generated SQL)::

    pip install "apache-airflow-providers-common-ai[sql]"

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

Provide a natural language ``prompt`` and the operator generates a SQL query:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_operator_llm_sql_basic]
    :end-before: [END howto_operator_llm_sql_basic]

With Schema Introspection
-------------------------

Use ``db_conn_id`` and ``table_names`` to automatically include database schema
in the LLM's context. This produces more accurate queries because the LLM knows
the actual column names and types:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_operator_llm_sql_schema]
    :end-before: [END howto_operator_llm_sql_schema]

With Object Storage
-------------------

Use ``datasource_config`` to generate queries for data stored in object storage
(e.g., S3, GCS, Azure Blob Storage, local filesystem) via `DataFusion <https://datafusion.apache.org/>`_.
The operator uses :class:`~airflow.providers.common.sql.config.DataSourceConfig`
to register the object storage source as a table so the LLM can include it in
the schema context.

.. note::

    Object-storage schema introspection requires the ``datafusion`` extra of
    ``apache-airflow-providers-common-sql``. Install it with
    ``pip install "apache-airflow-providers-common-sql[datafusion]"``. ``common.sql`` pins
    ``datafusion`` below 52 because the DataFusion 52 crate is not supported with
    ``iceberg-core``.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_operator_llm_sql_with_object_storage]
    :end-before: [END howto_operator_llm_sql_with_object_storage]

Once the SQL is generated, you can execute it against object storage data using
:class:`~airflow.providers.common.sql.operators.analytics.AnalyticsOperator`.
Chain the two operators so that the generated query flows into the analytics
execution step:

.. code-block:: python

    from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
    from airflow.providers.common.sql.config import DataSourceConfig
    from airflow.providers.common.sql.operators.analytics import AnalyticsOperator

    datasource_config = DataSourceConfig(
        conn_id="aws_default",
        table_name="sales_data",
        uri="s3://my-bucket/data/sales/",
        format="parquet",
    )

    generate_sql = LLMSQLQueryOperator(
        task_id="generate_sql",
        prompt="Find the top 5 products by total sales amount",
        llm_conn_id="pydanticai_default",
        datasource_config=datasource_config,
    )

    run_query = AnalyticsOperator(
        task_id="run_query",
        datasource_configs=[datasource_config],
        queries=["{{ ti.xcom_pull(task_ids='generate_sql') }}"],
    )

    generate_sql >> run_query

TaskFlow Decorator
------------------

The ``@task.llm_sql`` decorator lets you write a function that returns the
prompt. The decorator handles LLM connection, schema introspection, SQL generation,
and safety validation:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_decorator_llm_sql]
    :end-before: [END howto_decorator_llm_sql]

The callable may also return a non-empty ``Sequence[UserContent]`` for
multimodal inputs -- see
:ref:`@task.agent multimodal prompts <howto/operator:agent-multimodal>`.

Dynamic Task Mapping
--------------------

Generate SQL for multiple prompts in parallel using ``expand()``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_operator_llm_sql_expand]
    :end-before: [END howto_operator_llm_sql_expand]

Usage Limits
------------

``usage_limits`` caps the request count, token usage, tool calls, or cost of the
SQL generation run, and fails the task when a budget is exceeded. It is inherited
from ``LLMOperator`` -- see :ref:`Usage Limits <howto/operator:llm_usage_limits>`.

Human-in-the-Loop Approval
--------------------------

Set ``require_approval=True`` to pause the task after SQL generation and wait
for a human reviewer to approve the query before it is returned.
When ``allow_modifications=True``, the reviewer can also edit the SQL; the
modified query is re-validated against the same safety rules automatically.
``approval_timeout`` and ``on_approval_timeout`` behave as on
:ref:`LLMOperator <howto/operator:llm>`:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_operator_llm_sql_approval]
    :end-before: [END howto_operator_llm_sql_approval]

``approval_timeout``, ``approval_notifiers``, ``approval_assigned_users``, and
the rest of the approval behaviour are inherited from
:ref:`LLMOperator <howto/operator:llm>`.
``decision_policy`` is not supported here: the operator runs its own ``execute`` without the
confidence gate and rejects a policy with a bar at construction.

SQL Safety Validation
---------------------

By default, the operator validates generated SQL using an allowlist approach:

- Only ``SELECT``, ``UNION``, ``INTERSECT``, and ``EXCEPT`` statements are allowed.
- Multi-statement SQL (semicolon-separated) is rejected.
- Disallowed statements (``INSERT``, ``UPDATE``, ``DELETE``, ``DROP``, etc.) raise
  :class:`~airflow.providers.common.ai.utils.sql_validation.SQLSafetyError`.

You can disable validation with ``validate_sql=False`` or customize the allowed
statement types with ``allowed_sql_types``.

Restricting the tables a query may reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``table_names`` only selects the schemas shown to the LLM; it does not stop the
generated SQL from reading other tables. Set ``allowed_tables`` to reject any query
that references a table outside the list:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_operator_llm_sql_allowed_tables]
    :end-before: [END howto_operator_llm_sql_allowed_tables]

Every table the query reaches is checked, including tables in subqueries and joins,
while a CTE defined in the query is not mistaken for a table. Entries are ``table`` or
``schema.table`` and match case-insensitively. The operator has no default schema, so
an unqualified entry only matches unqualified references: ``orders`` does not match
``sales.orders``. Catalog-qualified references never match.

SQL that cannot be checked against the list is refused as well. In practice that
mostly means:

- SQL comments, such as a trailing ``-- per customer``;
- quoted identifiers, such as ``FROM "orders"``, because a quoted name is
  case-sensitive on the database while the list is matched case-insensitively;
- functions sqlglot does not recognize, such as Postgres' ``age`` or
  ``json_build_object``. List trusted ones in ``allowed_functions``;
- table-valued functions such as ``dblink``.

When ``allowed_tables`` is set, the prompt lists the allowed tables and asks for
unquoted table names and no comments. If the model's answer is still refused, the
refusal is sent back to the model, which can rewrite the query as many times as
``output_retries`` allows; if it is refused every time, the task fails with
:class:`~airflow.providers.common.ai.utils.sql_validation.SQLSafetyError`. SQL edited
by a reviewer during Human-in-the-Loop approval is checked too, and a refusal raises
the same error.
``allowed_tables`` requires ``validate_sql=True``. It is not a template field, so the
list is fixed when the Dag is parsed.

The check uses the same matching as ``SQLToolset(allowed_tables=...)``, except that
``SQLToolset`` resolves unqualified entries through its ``schema`` argument. Like
``SQLToolset``'s allow-list, it fails closed but is only as exact as the SQL parser,
so it is not a security boundary: pair it with least-privilege grants for the
database user that runs the generated query.

Logging
-------

After each LLM call, the operator logs a summary with model name, token usage,
and request count at INFO level. At DEBUG level, the generated SQL is also
logged (truncated to 500 characters). See :ref:`AgentOperator logging <howto/operator:agent>`
for details on the log format.

Like :doc:`llm`, the operator also pushes ``usage``, ``run_id`` and the model name to
XCom, on a failed run too. See :doc:`../observability`.
