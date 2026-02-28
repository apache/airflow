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

``LLMSQLQueryOperator``
========================

Use :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator` to generate
SQL queries from natural language using an LLM.

The operator generates SQL but does not execute it. The generated query is returned
as XCom and can be passed to ``SQLExecuteQueryOperator`` or used in downstream tasks.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydantic_ai>`

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
(e.g., S3, local filesystem) via `DataFusion <https://datafusion.apache.org/>`_.
The operator uses :class:`~airflow.providers.common.sql.config.DataSourceConfig`
to register the object storage source as a table so the LLM can include it in
the schema context.

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
        llm_conn_id="pydantic_ai_default",
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

Dynamic Task Mapping
--------------------

Generate SQL for multiple prompts in parallel using ``expand()``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_sql.py
    :language: python
    :start-after: [START howto_operator_llm_sql_expand]
    :end-before: [END howto_operator_llm_sql_expand]

SQL Safety Validation
---------------------

By default, the operator validates generated SQL using an allowlist approach:

- Only ``SELECT``, ``UNION``, ``INTERSECT``, and ``EXCEPT`` statements are allowed.
- Multi-statement SQL (semicolon-separated) is rejected.
- Disallowed statements (``INSERT``, ``UPDATE``, ``DELETE``, ``DROP``, etc.) raise
  :class:`~airflow.providers.common.ai.utils.sql_validation.SQLSafetyError`.

You can disable validation with ``validate_sql=False`` or customize the allowed
statement types with ``allowed_sql_types``.
